#include <IOKit/IOKitLib.h>
#include <IOKit/IOCFPlugIn.h>
#include <IOKit/scsi/SCSITaskLib.h>
#include <CoreFoundation/CoreFoundation.h>
#include <DiskArbitration/DiskArbitration.h>
#include <dispatch/dispatch.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <spawn.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <signal.h>

extern char **environ;

// ── Types ──────────────────────────────────────────────────────────────────

typedef struct {
    IOCFPlugInInterface      **plugin;
    MMCDeviceInterface       **mmc;
    SCSITaskDeviceInterface  **scsi;
    int                        exclusive;
    // DiskArbitration claim held for the whole session so diskarbitrationd
    // cannot remount the disc out from under an in-progress rip (the mount
    // approval callback dissents while claimed).
    DASessionRef               da_session;
    DADiskRef                  da_disk;
    dispatch_queue_t           da_queue;
    int                        da_claimed;
} ShimHandle;

typedef struct {
    char bsd_name[32];
    char vendor[32];
    char model[48];
    char firmware[16];
} ShimDriveInfo;

// ── Global handle (single-drive, same as before) ──────────────────────────

static ShimHandle g_handle = {NULL, NULL, NULL, 0};

// ── Registry helpers ──────────────────────────────────────────────────────

// Convert a registry property to a C string.
//
// The value is taken as CFTypeRef, not CFStringRef, and its type is checked
// before use. IORegistryEntryCreateCFProperty / CFDictionaryGetValue return
// whatever the driver published: the IOKit registry contract (Apple, "Accessing
// Hardware From Applications" — Device Access and the I/O Kit) fixes the
// property KEYS, not the CoreFoundation type behind them, and a third-party
// optical driver publishing a CFNumber or CFData for "BSD Name" or "Product
// Revision Level" is legal. CFStringGetCString on a non-CFString aborts the
// process (CFRuntime type assertion) — from inside the public
// scsi::list_drives(), which is documented never to fail. Wrong type → treated
// as absent.
static int cfstring_to_cstr(CFTypeRef cf, char *buf, size_t buflen) {
    if (!cf) return 0;
    if (CFGetTypeID(cf) != CFStringGetTypeID()) return 0;
    if (!CFStringGetCString((CFStringRef)cf, buf, buflen, kCFStringEncodingUTF8)) return 0;
    return 1;
}

static int registry_entry_bsd_name(io_registry_entry_t entry, char *buf, size_t buflen) {
    CFTypeRef cf = IORegistryEntryCreateCFProperty(entry, CFSTR("BSD Name"),
        kCFAllocatorDefault, 0);
    if (!cf) return 0;
    int ok = cfstring_to_cstr(cf, buf, buflen);
    CFRelease(cf);
    return ok;
}

static io_registry_entry_t find_iomedia_child(io_registry_entry_t parent) {
    io_iterator_t iter;
    kern_return_t kr = IORegistryEntryGetChildIterator(parent, kIOServicePlane, &iter);
    if (kr != KERN_SUCCESS) return 0;

    io_registry_entry_t child;
    while ((child = IOIteratorNext(iter)) != 0) {
        char cls[128];
        kr = IOObjectGetClass(child, cls);
        if (kr == KERN_SUCCESS) {
            if (strcmp(cls, "IOMedia") == 0 || strcmp(cls, "IOBDMedia") == 0) {
                IOObjectRelease(iter);
                return child;
            }
        }
        IOObjectRelease(child);
    }
    IOObjectRelease(iter);
    return 0;
}

static io_registry_entry_t find_child_of_class(io_registry_entry_t parent, const char *target_class) {
    io_iterator_t iter;
    kern_return_t kr = IORegistryEntryGetChildIterator(parent, kIOServicePlane, &iter);
    if (kr != KERN_SUCCESS) return 0;

    io_registry_entry_t child;
    while ((child = IOIteratorNext(iter)) != 0) {
        char cls[128];
        kr = IOObjectGetClass(child, cls);
        if (kr == KERN_SUCCESS && strcmp(cls, target_class) == 0) {
            IOObjectRelease(iter);
            return child;
        }
        IOObjectRelease(child);
    }
    IOObjectRelease(iter);
    return 0;
}

static io_registry_entry_t find_parent_of_class(io_registry_entry_t entry, const char *target_class) {
    io_registry_entry_t parent;
    kern_return_t kr = IORegistryEntryGetParentEntry(entry, kIOServicePlane, &parent);
    if (kr != KERN_SUCCESS) return 0;

    char cls[128];
    kr = IOObjectGetClass(parent, cls);
    if (kr == KERN_SUCCESS && strcmp(cls, target_class) == 0) {
        return parent;
    }
    IOObjectRelease(parent);
    return 0;
}

// Given an IOBDServices, find the BSD name of its IOMedia child.
// Chain: IOBDServices -> IOBDBlockStorageDriver -> IOMedia (has "BSD Name")
static int bdsvc_to_bsd_name(io_registry_entry_t bdsvc, char *buf, size_t buflen) {
    io_registry_entry_t driver = find_child_of_class(bdsvc, "IOBDBlockStorageDriver");
    if (!driver) return 0;

    io_registry_entry_t media = find_iomedia_child(driver);
    IOObjectRelease(driver);
    if (!media) return 0;

    int ok = registry_entry_bsd_name(media, buf, buflen);
    IOObjectRelease(media);
    return ok;
}

// Given an IOBDServices, extract Device Characteristics strings.
static void bdsvc_device_info(io_registry_entry_t bdsvc, ShimDriveInfo *info) {
    // "Device Characteristics" is declared a dictionary, but the value is
    // driver-published and the registry contract does not enforce the type.
    // CFDictionaryGetValue on a non-dictionary aborts the process, so the type
    // is checked before it is used as one. Each member string is type-checked
    // in turn by cfstring_to_cstr.
    CFTypeRef dc = IORegistryEntryCreateCFProperty(bdsvc,
        CFSTR("Device Characteristics"), kCFAllocatorDefault, 0);
    if (!dc) return;
    if (CFGetTypeID(dc) != CFDictionaryGetTypeID()) {
        CFRelease(dc);
        return;
    }
    CFDictionaryRef dict = (CFDictionaryRef)dc;

    CFTypeRef val;

    val = CFDictionaryGetValue(dict, CFSTR("Vendor Name"));
    if (val) cfstring_to_cstr(val, info->vendor, sizeof(info->vendor));

    val = CFDictionaryGetValue(dict, CFSTR("Product Name"));
    if (val) cfstring_to_cstr(val, info->model, sizeof(info->model));

    val = CFDictionaryGetValue(dict, CFSTR("Product Revision Level"));
    if (val) cfstring_to_cstr(val, info->firmware, sizeof(info->firmware));

    CFRelease(dc);
}

// Find the IOBDServices that owns the given BSD name.
// Returns a retained io_service_t (caller must release), or 0.
static io_service_t find_bdsvc_by_bsd_name(mach_port_t mp, const char *bsd_name) {
    CFMutableDictionaryRef matching = IOServiceMatching("IOBDServices");
    if (!matching) return 0;

    io_iterator_t iter;
    kern_return_t kr = IOServiceGetMatchingServices(mp, matching, &iter);
    if (kr != KERN_SUCCESS) return 0;

    io_service_t result = 0;
    io_service_t svc;
    while ((svc = IOIteratorNext(iter)) != 0) {
        char name[64];
        if (bdsvc_to_bsd_name(svc, name, sizeof(name))) {
            if (strcmp(name, bsd_name) == 0) {
                result = svc;
                break;
            }
        }
        IOObjectRelease(svc);
    }

    if (!result) {
        IOIteratorReset(iter);
        while ((svc = IOIteratorNext(iter)) != 0) {
            IOObjectRelease(svc);
        }
    }

    IOObjectRelease(iter);
    return result;
}

// Find the IOBDServices that owns the given BSD name by walking from
// IOMedia upward. Used as fallback when bdsvc_to_bsd_name fails
// (e.g. disc under exclusive access, no IOMedia child).
// Chain: IOMedia -> IOBDBlockStorageDriver -> IOBDServices
static io_service_t find_bdsvc_from_iomedia(mach_port_t mp, const char *bsd_name) {
    CFMutableDictionaryRef matching = IOServiceMatching("IOMedia");
    if (!matching) return 0;

    io_iterator_t iter;
    kern_return_t kr = IOServiceGetMatchingServices(mp, matching, &iter);
    if (kr != KERN_SUCCESS) return 0;

    io_service_t result = 0;
    io_service_t media;
    while ((media = IOIteratorNext(iter)) != 0) {
        char name[64];
        if (registry_entry_bsd_name(media, name, sizeof(name))
            && strcmp(name, bsd_name) == 0)
        {
            io_registry_entry_t driver = find_parent_of_class(media, "IOBDBlockStorageDriver");
            if (driver) {
                io_registry_entry_t bdsvc = find_parent_of_class(driver, "IOBDServices");
                IOObjectRelease(driver);
                if (bdsvc) {
                    result = bdsvc;
                    IOObjectRelease(media);
                    break;
                }
            }
        }
        IOObjectRelease(media);
    }

    IOObjectRelease(iter);
    return result;
}

// ── DiskArbitration claim ──────────────────────────────────────────────────
//
// ObtainExclusiveAccess reserves the SCSI passthrough but says nothing to
// diskarbitrationd, which stays free to remount the disc mid-rip (Spotlight
// re-probe, etc.), yanking the media out from under an in-progress read. We
// hold a DADiskClaim + a mount-approval dissenter for the session so the OS
// cannot remount our disc until shim_close().

static char g_da_bsd[32];

// Dissent a remount only for OUR disk; every other disk is approved so we
// never block the rest of the system's volumes.
static DADissenterRef da_mount_approval(DADiskRef disk, void *ctx) {
    (void)ctx;
    const char *n = DADiskGetBSDName(disk);
    if (!n || strcmp(n, g_da_bsd) != 0) return NULL;
    return DADissenterCreate(kCFAllocatorDefault, kDAReturnBusy,
        CFSTR("freemkv is reading this disc"));
}

// Refuse an involuntary claim release; we give it back only in shim_close().
static DADissenterRef da_claim_release(DADiskRef disk, void *ctx) {
    (void)disk; (void)ctx;
    return DADissenterCreate(kCFAllocatorDefault, kDAReturnBusy,
        CFSTR("freemkv still holds this disc"));
}

typedef struct { dispatch_semaphore_t sem; int ok; } DAClaimResult;

static void da_claim_done(DADiskRef disk, DADissenterRef dissenter, void *ctx) {
    (void)disk;
    DAClaimResult *r = (DAClaimResult *)ctx;
    r->ok = (dissenter == NULL);
    dispatch_semaphore_signal(r->sem);
}

// Best-effort: claim the disk and register the mount-approval dissenter.
// Returns 1 if claimed. The caller proceeds either way — ObtainExclusiveAccess
// remains the hard gate; the claim is what keeps DA from remounting after it.
static int da_hold(const char *bsd_name) {
    strncpy(g_da_bsd, bsd_name, sizeof(g_da_bsd) - 1);
    g_da_bsd[sizeof(g_da_bsd) - 1] = 0;

    g_handle.da_queue = dispatch_queue_create("io.freemkv.da", DISPATCH_QUEUE_SERIAL);
    if (!g_handle.da_queue) return 0;
    g_handle.da_session = DASessionCreate(kCFAllocatorDefault);
    if (!g_handle.da_session) return 0;
    DASessionSetDispatchQueue(g_handle.da_session, g_handle.da_queue);
    g_handle.da_disk =
        DADiskCreateFromBSDName(kCFAllocatorDefault, g_handle.da_session, bsd_name);
    if (!g_handle.da_disk) return 0;

    DARegisterDiskMountApprovalCallback(g_handle.da_session, NULL, da_mount_approval, NULL);

    DAClaimResult r = {dispatch_semaphore_create(0), 0};
    DADiskClaim(g_handle.da_disk, kDADiskClaimOptionDefault,
        da_claim_release, NULL, da_claim_done, &r);
    // Bounded wait for the async claim callback (5 s), so a wedged DA can't
    // hang open() forever.
    if (dispatch_semaphore_wait(r.sem,
            dispatch_time(DISPATCH_TIME_NOW, 5LL * NSEC_PER_SEC)) != 0) {
        g_handle.da_claimed = 0;
    } else {
        g_handle.da_claimed = r.ok;
    }
    return g_handle.da_claimed;
}

static void da_release(void) {
    if (g_handle.da_session) {
        DAUnregisterCallback(g_handle.da_session, (void *)da_mount_approval, NULL);
    }
    if (g_handle.da_disk) {
        if (g_handle.da_claimed) DADiskUnclaim(g_handle.da_disk);
        CFRelease(g_handle.da_disk);
        g_handle.da_disk = NULL;
    }
    if (g_handle.da_session) {
        DASessionSetDispatchQueue(g_handle.da_session, NULL);
        CFRelease(g_handle.da_session);
        g_handle.da_session = NULL;
    }
    if (g_handle.da_queue) {
        dispatch_release(g_handle.da_queue);
        g_handle.da_queue = NULL;
    }
    g_handle.da_claimed = 0;
}

// ── Public API ────────────────────────────────────────────────────────────

int shim_open_exclusive(const char *bsd_name) {
    kern_return_t kr;
    HRESULT hr;
    SInt32 score = 0;

    if (g_handle.exclusive && g_handle.scsi) {
        return 0;
    }

    // Unmount via diskutil, invoked directly with posix_spawn (no shell) so
    // the BSD device name can never be interpreted as shell syntax. A shell
    // wrapper here (system()/sh -c) was a command-injection vector for an
    // attacker-controlled device argument. Passing bsd_name as a discrete
    // argv element also sidesteps the old buffer-truncation concern entirely.
    // stdout/stderr go to /dev/null to keep diskutil chatter out of the
    // caller's streams.
    {
        posix_spawn_file_actions_t fa;
        posix_spawn_file_actions_init(&fa);
        posix_spawn_file_actions_addopen(&fa, STDOUT_FILENO, "/dev/null", O_WRONLY, 0);
        posix_spawn_file_actions_addopen(&fa, STDERR_FILENO, "/dev/null", O_WRONLY, 0);
        char *const argv[] = {
            "diskutil", "unmountDisk", "force", (char *)bsd_name, NULL
        };
        pid_t pid;
        if (posix_spawn(&pid, "/usr/sbin/diskutil", &fa, NULL, argv, environ) == 0) {
            // BOUNDED wait. A plain blocking waitpid() here hung the public
            // scsi::open() forever whenever the unmount wedged — diskutil
            // blocks indefinitely on a volume whose filesystem is stuck (a
            // hung network mount, a fs process not answering the unmount
            // notification), and there is no signal, timeout or cancellation
            // reaching this frame. Poll with WNOHANG to a deadline, then
            // SIGKILL and reap so no zombie is left behind.
            //
            // Continuing after a killed unmount is deliberate:
            // ObtainExclusiveAccess below is the real gate, and it reports the
            // still-mounted disc through the shim's -5 sentinel (mapped to
            // Error::DeviceLocked) — a typed error the caller can act on,
            // instead of a process that never returns.
            const int poll_us = 50000;      // 50 ms
            const int max_polls = 400;      // 400 x 50 ms = 20 s
            int status;
            int reaped = 0;
            for (int i = 0; i <= max_polls; i++) {
                pid_t r = waitpid(pid, &status, WNOHANG);
                if (r == pid) { reaped = 1; break; }
                // r < 0 means the child is already gone (ECHILD) — nothing to
                // wait for, and looping would spin to the deadline.
                if (r < 0) { reaped = 1; break; }
                if (i == max_polls) break;
                usleep(poll_us);
            }
            if (!reaped) {
                kill(pid, SIGKILL);
                // SIGKILL is uncatchable, so this reap converges; still poll
                // rather than block, so the shim has no unbounded wait at all.
                for (int i = 0; i < 100; i++) {
                    if (waitpid(pid, &status, WNOHANG) != 0) break;
                    usleep(10000); // 10 ms x 100 = 1 s
                }
            }
        }
        posix_spawn_file_actions_destroy(&fa);
    }
    usleep(500000);

    mach_port_t mp;
    // Check the return before using the port. On failure IOMainPort leaves `mp`
    // untouched, so every IOKit call below would run against an uninitialised
    // mach port. shim_list_drives does check it; this path did not.
    if (IOMainPort(0, &mp) != kIOReturnSuccess) return -1;

    io_service_t svc = find_bdsvc_by_bsd_name(mp, bsd_name);
    if (!svc) {
        svc = find_bdsvc_from_iomedia(mp, bsd_name);
    }
    if (!svc) {
        // IOServiceMatching returns NULL on allocation failure. Both other call
        // sites in this file check it; this one did not, and
        // IOServiceGetMatchingService with a NULL matching dictionary is
        // undefined (it consumes the reference it is given).
        CFMutableDictionaryRef matching = IOServiceMatching("IOBDServices");
        if (matching) {
            svc = IOServiceGetMatchingService(mp, matching);
        }
    }
    if (!svc) return -1;

    kr = IOCreatePlugInInterfaceForService(svc,
        kIOMMCDeviceUserClientTypeID, kIOCFPlugInInterfaceID,
        &g_handle.plugin, &score);
    IOObjectRelease(svc);

    if (kr != KERN_SUCCESS || !g_handle.plugin) return -2;

    hr = (*g_handle.plugin)->QueryInterface(g_handle.plugin,
        CFUUIDGetUUIDBytes(kIOMMCDeviceInterfaceID), (LPVOID *)&g_handle.mmc);
    if (hr != S_OK || !g_handle.mmc) {
        IODestroyPlugInInterface(g_handle.plugin);
        g_handle.plugin = NULL;
        return -3;
    }

    g_handle.scsi = (*g_handle.mmc)->GetSCSITaskDeviceInterface(g_handle.mmc);
    if (!g_handle.scsi) {
        (*g_handle.mmc)->Release(g_handle.mmc);
        IODestroyPlugInInterface(g_handle.plugin);
        g_handle.mmc = NULL;
        g_handle.plugin = NULL;
        return -4;
    }

    for (int retry = 0; retry < 10; retry++) {
        kr = (*g_handle.scsi)->ObtainExclusiveAccess(g_handle.scsi);
        if (kr == kIOReturnSuccess) break;
        usleep(500000);
    }
    if (kr != kIOReturnSuccess) {
        (*g_handle.scsi)->Release(g_handle.scsi);
        (*g_handle.mmc)->Release(g_handle.mmc);
        IODestroyPlugInInterface(g_handle.plugin);
        g_handle.scsi = NULL;
        g_handle.mmc = NULL;
        g_handle.plugin = NULL;
        return -5;
    }

    g_handle.exclusive = 1;

    // Claim the disk now that we hold the drive, so DA can't remount it during
    // the read. Best-effort: a failed claim does not fail the open (we already
    // have exclusive SCSI access and the disc is unmounted).
    da_hold(bsd_name);

    return 0;
}

void shim_close(void) {
    da_release();
    if (g_handle.exclusive && g_handle.scsi) {
        (*g_handle.scsi)->ReleaseExclusiveAccess(g_handle.scsi);
    }
    if (g_handle.scsi) {
        (*g_handle.scsi)->Release(g_handle.scsi);
        g_handle.scsi = NULL;
    }
    if (g_handle.mmc) {
        (*g_handle.mmc)->Release(g_handle.mmc);
        g_handle.mmc = NULL;
    }
    if (g_handle.plugin) {
        IODestroyPlugInInterface(g_handle.plugin);
        g_handle.plugin = NULL;
    }
    g_handle.exclusive = 0;
}

int shim_execute(const unsigned char *cdb, unsigned char cdb_len,
                 void *buf, unsigned int buf_len, int data_in,
                 unsigned char *sense_out, unsigned int sense_len,
                 unsigned char *task_status_out, unsigned long long *transfer_count) {
    if (!g_handle.scsi) return -1;

    SCSITaskInterface **task = (*g_handle.scsi)->CreateSCSITask(g_handle.scsi);
    if (!task) return -2;

    SCSICommandDescriptorBlock cdb_buf;
    memset(&cdb_buf, 0, sizeof(cdb_buf));
    memcpy(&cdb_buf, cdb, cdb_len);

    (*task)->SetCommandDescriptorBlock(task, cdb_buf, cdb_len);

    if (buf_len > 0 && buf) {
        SCSITaskSGElement sg;
        sg.address = (UInt64)(uintptr_t)buf;
        sg.length = buf_len;
        (*task)->SetScatterGatherEntries(task, &sg, 1, buf_len,
            data_in ? kSCSIDataTransfer_FromTargetToInitiator
                    : kSCSIDataTransfer_FromInitiatorToTarget);
    } else {
        (*task)->SetScatterGatherEntries(task, NULL, 0, 0,
            kSCSIDataTransfer_NoDataTransfer);
    }

    (*task)->SetTimeoutDuration(task, 30000);

    SCSI_Sense_Data sense;
    memset(&sense, 0, sizeof(sense));
    SCSITaskStatus status = 0xFF;
    UInt64 count = 0;

    IOReturn kr = (*task)->ExecuteTaskSync(task, &sense, &status, &count);

    if (sense_out && sense_len > 0) {
        size_t copy = sense_len < sizeof(sense) ? sense_len : sizeof(sense);
        memcpy(sense_out, &sense, copy);
    }
    if (task_status_out) *task_status_out = (unsigned char)status;
    if (transfer_count) *transfer_count = count;

    (*task)->Release(task);

    return (int)kr;
}

// ── Registry-based media-presence probe ───────────────────────────────────
//
// "Is a disc inserted?" answered from the IOKit registry alone: no exclusive
// access, no unmount, no SCSI command, no state change of any kind.
//
// Apple's IOStorageFamily publishes an IOMedia object for a removable device
// only while media is present, and tears it down on eject — that is the
// documented media lifecycle (Apple, "Mass Storage Device Driver Programming
// Guide": Media Objects / media arrival and removal). So the presence of an
// IOMedia whose "BSD Name" is the requested device IS the presence of a disc.
//
// Returns 1 (media present), 0 (no media), or -1 (IOKit unavailable).
int shim_media_present(const char *bsd_name) {
    mach_port_t mp;
    if (IOMainPort(0, &mp) != kIOReturnSuccess) return -1;

    CFMutableDictionaryRef matching = IOServiceMatching("IOMedia");
    if (!matching) return -1;

    io_iterator_t iter;
    // Consumes `matching` whether it succeeds or fails.
    if (IOServiceGetMatchingServices(mp, matching, &iter) != KERN_SUCCESS) return -1;

    int found = 0;
    io_service_t media;
    while ((media = IOIteratorNext(iter)) != 0) {
        char name[64];
        if (registry_entry_bsd_name(media, name, sizeof(name))
            && strcmp(name, bsd_name) == 0)
        {
            found = 1;
        }
        IOObjectRelease(media);
        if (found) break;
    }

    // Drain the rest so no entry is leaked when we broke early.
    while ((media = IOIteratorNext(iter)) != 0) {
        IOObjectRelease(media);
    }
    IOObjectRelease(iter);
    return found;
}

// ── Registry-based drive enumeration ──────────────────────────────────────
//
// Walks IOBDServices entries in the IOKit registry. No exclusive access,
// no SCSI commands, no unmounts. Returns up to max_entries drives.

int shim_list_drives(ShimDriveInfo *out, int max_entries) {
    mach_port_t mp;
    IOReturn ret = IOMainPort(0, &mp);
    if (ret != kIOReturnSuccess) return 0;

    CFMutableDictionaryRef matching = IOServiceMatching("IOBDServices");
    if (!matching) return 0;

    io_iterator_t iter;
    kern_return_t kr = IOServiceGetMatchingServices(mp, matching, &iter);
    if (kr != KERN_SUCCESS) return 0;

    int count = 0;
    io_service_t svc;
    while ((svc = IOIteratorNext(iter)) != 0 && count < max_entries) {
        ShimDriveInfo *info = &out[count];
        memset(info, 0, sizeof(*info));

        bdsvc_device_info(svc, info);
        bdsvc_to_bsd_name(svc, info->bsd_name, sizeof(info->bsd_name));

        if (info->bsd_name[0]) {
            count++;
        }

        IOObjectRelease(svc);
    }

    IOObjectRelease(iter);
    return count;
}
