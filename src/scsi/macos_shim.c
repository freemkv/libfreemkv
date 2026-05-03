#include <IOKit/IOKitLib.h>
#include <IOKit/IOCFPlugIn.h>
#include <IOKit/scsi/SCSITaskLib.h>
#include <CoreFoundation/CoreFoundation.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// ── Types ──────────────────────────────────────────────────────────────────

typedef struct {
    IOCFPlugInInterface      **plugin;
    MMCDeviceInterface       **mmc;
    SCSITaskDeviceInterface  **scsi;
    int                        exclusive;
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

static int cfstring_to_cstr(CFStringRef cf, char *buf, size_t buflen) {
    if (!cf) return 0;
    if (!CFStringGetCString(cf, buf, buflen, kCFStringEncodingUTF8)) return 0;
    return 1;
}

static int registry_entry_bsd_name(io_registry_entry_t entry, char *buf, size_t buflen) {
    CFStringRef cf = IORegistryEntryCreateCFProperty(entry, CFSTR("BSD Name"),
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
    CFDictionaryRef dc = IORegistryEntryCreateCFProperty(bdsvc,
        CFSTR("Device Characteristics"), kCFAllocatorDefault, 0);
    if (!dc) return;

    CFStringRef val;

    val = CFDictionaryGetValue(dc, CFSTR("Vendor Name"));
    if (val) cfstring_to_cstr(val, info->vendor, sizeof(info->vendor));

    val = CFDictionaryGetValue(dc, CFSTR("Product Name"));
    if (val) cfstring_to_cstr(val, info->model, sizeof(info->model));

    val = CFDictionaryGetValue(dc, CFSTR("Product Revision Level"));
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

// ── Public API ────────────────────────────────────────────────────────────

int shim_open_exclusive(const char *bsd_name) {
    kern_return_t kr;
    HRESULT hr;
    SInt32 score = 0;

    if (g_handle.exclusive && g_handle.scsi) {
        return 0;
    }

    // Use a shell wrapper so the device path is not subject to buffer limits.
    // snprintf into 128 bytes could truncate long BSD names (e.g. disk12s3s1),
    // producing a broken command. sh -c with $1 passes the arg via argv.
    const char *shell_fmt = "sh -c 'diskutil unmountDisk force \"$1\" >/dev/null 2>&1' _ %s";
    char cmd[512];
    int written = snprintf(cmd, sizeof(cmd), shell_fmt, bsd_name);
    if (written < 0 || (size_t)written >= sizeof(cmd)) {
        return -1;
    }
    system(cmd);
    usleep(500000);

    mach_port_t mp;
    IOMainPort(0, &mp);

    io_service_t svc = find_bdsvc_by_bsd_name(mp, bsd_name);
    if (!svc) {
        svc = find_bdsvc_from_iomedia(mp, bsd_name);
    }
    if (!svc) {
        CFMutableDictionaryRef matching = IOServiceMatching("IOBDServices");
        svc = IOServiceGetMatchingService(mp, matching);
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
    return 0;
}

void shim_close(void) {
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
