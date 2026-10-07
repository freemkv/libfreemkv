#include <IOKit/IOKitLib.h>
#include <IOKit/IOCFPlugIn.h>
#include <IOKit/storage/IOMedia.h>
#include <IOKit/scsi/SCSITaskLib.h>
#include <CoreFoundation/CoreFoundation.h>
#include <DiskArbitration/DiskArbitration.h>
#include <dispatch/dispatch.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <spawn.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <errno.h>
#include <stdarg.h>
#include <mach/mach_error.h>
#include <libproc.h>
#include <sys/xattr.h>

extern void freemkv_macos_diagnostic(const char *message);

static void diagnostic(const char *format, ...) {
    char message[2048];
    va_list args;
    va_start(args, format);
    vsnprintf(message, sizeof(message), format, args);
    va_end(args);
    freemkv_macos_diagnostic(message);
}

static void diagnostic_launch(void) {
    char path[PROC_PIDPATHINFO_MAXSIZE] = {0};
    int length = proc_pidpath(getpid(), path, sizeof(path));
    diagnostic("executable=%s path_result=%d", path, length);
    if (length > 0) {
        const char *attributes[] = { "com.apple.quarantine", "com.apple.provenance" };
        for (unsigned i = 0; i < sizeof(attributes) / sizeof(attributes[0]); i++) {
            errno = 0;
            ssize_t size = getxattr(path, attributes[i], NULL, 0, 0, 0);
            int error = errno;
            diagnostic("executable attribute=%s size=%lld errno=%d state=%s", attributes[i],
                (long long)size, error, size >= 0 ? "present" : error == ENOATTR ? "absent" : "unavailable");
        }
    }
    memset(path, 0, sizeof(path));
    length = proc_pidpath(getppid(), path, sizeof(path));
    diagnostic("parent_pid=%d executable=%s path_result=%d", getppid(), path, length);
}

static void diagnostic_result(const char *stage, IOReturn result) {
    diagnostic("stage=%s result=0x%08x description=%s", stage,
        (unsigned)result, mach_error_string(result));
}

static void diagnostic_service(io_service_t service) {
    io_name_t name = {0}, cls = {0};
    io_string_t path = {0};
    uint64_t registry_id = 0;
    IORegistryEntryGetName(service, name);
    IOObjectGetClass(service, cls);
    IORegistryEntryGetPath(service, kIOServicePlane, path);
    IORegistryEntryGetRegistryEntryID(service, &registry_id);
    diagnostic("selected registry_id=%" PRIu64 " name=%s class=%s path=%s",
        registry_id, name, cls, path);
    CFTypeRef types = IORegistryEntryCreateCFProperty(service,
        CFSTR("IOCFPlugInTypes"), kCFAllocatorDefault, 0);
    CFStringRef description = types ? CFCopyDescription(types) : NULL;
    char text[1024] = {0};
    if (description) CFStringGetCString(description, text, sizeof(text), kCFStringEncodingUTF8);
    diagnostic("IOCFPlugInTypes=%s", types ? text : "missing");
    if (description) CFRelease(description);
    if (types) CFRelease(types);
}

extern char **environ;

// ── Cancellation (stop design §2.9 M2) ─────────────────────────────────────

// shim_open_exclusive's result when the caller's cancel byte is set; Rust maps it to Halted.
#define SHIM_CANCELLED (-6)
// The longest a shim wait sleeps between cancel-byte checks (§2.9: "sliced at ≤ 20 ms").
#define SHIM_WAIT_SLICE_MS 20
// diskutil unmount budget; a wedged unmount is killed so open() still returns (§2.9).
#define SHIM_DISKUTIL_BUDGET_MS 20000
// Wait between ObtainExclusiveAccess retries (the unmount not yet released).
#define SHIM_SETTLE_MS 500
// DiskArbitration claim wait, so a wedged DA can't hang open().
#define SHIM_DA_CLAIM_MS 5000

void shim_close(void);

// The cancel byte is the Rust Halt's AtomicBool; the Acquire load pairs with its Release (SS-23).
static int shim_cancelled(const volatile uint8_t *cancel) {
    return cancel && __atomic_load_n(cancel, __ATOMIC_ACQUIRE) != 0;
}

static uint64_t shim_now_ns(void) {
    return clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
}

// The next wait until `end_ns`: at most one slice, in nanoseconds.
static uint64_t shim_slice_ns(uint64_t now_ns, uint64_t end_ns) {
    uint64_t left = end_ns - now_ns;
    uint64_t slice = (uint64_t)SHIM_WAIT_SLICE_MS * NSEC_PER_MSEC;
    return left < slice ? left : slice;
}

// Sleep `ms`. 0 once slept out, SHIM_CANCELLED on cancel.
static int sliced_sleep(uint32_t ms, const volatile uint8_t *cancel) {
    uint64_t end = shim_now_ns() + (uint64_t)ms * NSEC_PER_MSEC;
    for (;;) {
        if (shim_cancelled(cancel)) return SHIM_CANCELLED;
        uint64_t now = shim_now_ns();
        if (now >= end) return 0;
        usleep((useconds_t)((shim_slice_ns(now, end) + NSEC_PER_USEC - 1) / NSEC_PER_USEC));
    }
}

// Wait up to `ms` for `sem`. 0 signalled, 1 timed out, SHIM_CANCELLED on cancel.
static int sliced_sem_wait(dispatch_semaphore_t sem, uint32_t ms, const volatile uint8_t *cancel) {
    uint64_t end = shim_now_ns() + (uint64_t)ms * NSEC_PER_MSEC;
    for (;;) {
        if (shim_cancelled(cancel)) return SHIM_CANCELLED;
        uint64_t now = shim_now_ns();
        if (now >= end) return 1;
        int64_t slice = (int64_t)shim_slice_ns(now, end);
        // Apple dispatch/semaphore.h: "Returns zero on success, or non-zero if the timeout occurred."
        if (dispatch_semaphore_wait(sem, dispatch_time(DISPATCH_TIME_NOW, slice)) == 0) {
            return 0;
        }
    }
}

// Run `path argv` (no shell; stdout/stderr to /dev/null) and wait for it, killing it on
// budget or cancel. 0 exited, 1 budget spent, SHIM_CANCELLED, -1 spawn failed.
static int run_and_reap(const char *path, char *const argv[], uint32_t budget_ms,
                        const volatile uint8_t *cancel, pid_t *pid_out) {
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, STDOUT_FILENO, "/dev/null", O_WRONLY, 0);
    posix_spawn_file_actions_addopen(&fa, STDERR_FILENO, "/dev/null", O_WRONLY, 0);
    pid_t pid;
    int spawned = posix_spawn(&pid, path, &fa, NULL, argv, environ);
    posix_spawn_file_actions_destroy(&fa);
    if (spawned != 0) return -1;
    if (pid_out) *pid_out = pid;
    uint64_t end = shim_now_ns() + (uint64_t)budget_ms * NSEC_PER_MSEC;
    int status;
    int outcome;
    for (;;) {
        // wait(2): "The WNOHANG option is used to indicate that the call should not block if
        // there are no processes that wish to report status." r < 0 (ECHILD): already gone.
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r < 0 && errno == EINTR) continue;
        if (r == pid || r < 0) return 0;
        if (shim_cancelled(cancel)) { outcome = SHIM_CANCELLED; break; }
        uint64_t now = shim_now_ns();
        if (now >= end) { outcome = 1; break; }
        usleep((useconds_t)((shim_slice_ns(now, end) + NSEC_PER_USEC - 1) / NSEC_PER_USEC));
    }
    // signal(3): "Except for the SIGKILL and SIGSTOP signals, the signal() function allows for
    // a signal to be caught" — so the reap converges; still bounded (1 s) and sliced.
    kill(pid, SIGKILL);
    for (int i = 0; i < 1000 / SHIM_WAIT_SLICE_MS; i++) {
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid || (r < 0 && errno != EINTR)) break;
        usleep(SHIM_WAIT_SLICE_MS * 1000);
    }
    return outcome;
}

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
    // Claim whose callback had not arrived by the 5 s timeout; resolved in da_release.
    struct DAClaimResult      *da_pending;
} ShimHandle;

typedef struct {
    char device_selector[32];
    char vendor[32];
    char model[48];
    char firmware[16];
    char bsd_name[32];
} ShimDriveInfo;

// ── Global handle (single-drive, same as before) ──────────────────────────

static ShimHandle g_handle = {NULL, NULL, NULL, 0};

// Serializes read-modify-write of the process-global g_handle fields and the
// g_da_bsd buffer so concurrent/re-entrant open+close can't interleave a check
// with a mutate. NOT held across the bounded 5 s async-claim wait (would serialize).
static pthread_mutex_t g_handle_lock = PTHREAD_MUTEX_INITIALIZER;

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

// Optical services outlive their IOMedia nodes, which disappear for an empty
// tray and during exclusive access. Use the service registry ID as the stable
// selector throughout insertion, ripping and ejection (until unplug/reboot).
static int registry_id_selector(io_registry_entry_t entry, char *buf, size_t buflen) {
    uint64_t registry_id = 0;
    if (IORegistryEntryGetRegistryEntryID(entry, &registry_id) != KERN_SUCCESS) return 0;
    int n = snprintf(buf, buflen, "ioreg:%" PRIu64, registry_id);
    return n > 0 && (size_t)n < buflen;
}

// BD, DVD and CD-only drives publish distinct service and driver classes
// (IOBDServices / IODVDServices / IOCompactDiscServices and the matching
// IO*BlockStorageDriver). All three are optical drives; IOServiceMatching
// matches a class and its subclasses only, so each is queried in turn.
static const char *const k_optical_services[] = {
    "IOBDServices", "IODVDServices", "IOCompactDiscServices", NULL
};
static const char *const k_optical_drivers[] = {
    "IOBDBlockStorageDriver", "IODVDBlockStorageDriver", "IOCDBlockStorageDriver", NULL
};

static int conforms_to_any(io_object_t obj, const char *const *classes) {
    for (; *classes; classes++) {
        if (IOObjectConformsTo(obj, *classes)) return 1;
    }
    return 0;
}

// The default IOKit port is MACH_PORT_NULL (kIOMainPortDefault), on every macOS: no
// IOMainPort/IOMasterPort call, so no availability check the Rust link cannot resolve.
static kern_return_t shim_main_port(mach_port_t *mp) {
    *mp = MACH_PORT_NULL;
    return kIOReturnSuccess;
}

// Visitor result: 0 = continue (svc released), 1 = stop and return svc
// (retained), 2 = stop (svc released).
typedef int (*optical_visit_fn)(io_service_t svc, void *ctx);

static io_service_t for_each_optical_service(mach_port_t mp, optical_visit_fn visit, void *ctx) {
    for (int i = 0; k_optical_services[i]; i++) {
        CFMutableDictionaryRef matching = IOServiceMatching(k_optical_services[i]);
        if (!matching) continue;
        io_iterator_t iter;
        if (IOServiceGetMatchingServices(mp, matching, &iter) != KERN_SUCCESS) continue;
        io_service_t svc;
        while ((svc = IOIteratorNext(iter)) != 0) {
            // A service matched by an earlier class was already visited.
            int seen = 0;
            for (int j = 0; j < i && !seen; j++) {
                seen = IOObjectConformsTo(svc, k_optical_services[j]);
            }
            int act = seen ? 0 : visit(svc, ctx);
            if (act == 1) {
                io_service_t kept = svc, rest;
                while ((rest = IOIteratorNext(iter)) != 0) IOObjectRelease(rest);
                IOObjectRelease(iter);
                return kept;
            }
            IOObjectRelease(svc);
            if (act == 2) {
                while ((svc = IOIteratorNext(iter)) != 0) IOObjectRelease(svc);
                IOObjectRelease(iter);
                return 0;
            }
        }
        IOObjectRelease(iter);
    }
    return 0;
}

static int parse_registry_id_selector(const char *selector, uint64_t *registry_id) {
    static const char prefix[] = "ioreg:";
    if (strncmp(selector, prefix, sizeof(prefix) - 1) != 0) return 0;
    const char *digits = selector + sizeof(prefix) - 1;
    if (!*digits) return 0;
    for (const char *p = digits; *p; p++) {
        if (*p < '0' || *p > '9') return 0;
    }
    errno = 0;
    char *end = NULL;
    unsigned long long parsed = strtoull(digits, &end, 10);
    if (errno == ERANGE || !end || *end || end == digits) return 0;
    *registry_id = (uint64_t)parsed;
    return 1;
}

static int visit_registry_id(io_service_t svc, void *ctx) {
    uint64_t candidate = 0;
    return IORegistryEntryGetRegistryEntryID(svc, &candidate) == KERN_SUCCESS
        && candidate == *(const uint64_t *)ctx;
}

static io_service_t find_bdsvc_by_registry_id(mach_port_t mp, uint64_t registry_id) {
    return for_each_optical_service(mp, visit_registry_id, &registry_id);
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
            // DVD and CD discs publish IODVDMedia / IOCDMedia, subclasses of IOMedia.
            if (IOObjectConformsTo(child, kIOMediaClass)) {
                IOObjectRelease(iter);
                return child;
            }
        }
        IOObjectRelease(child);
    }
    IOObjectRelease(iter);
    return 0;
}

static io_registry_entry_t find_child_of_class(io_registry_entry_t parent, const char *const *classes) {
    io_iterator_t iter;
    kern_return_t kr = IORegistryEntryGetChildIterator(parent, kIOServicePlane, &iter);
    if (kr != KERN_SUCCESS) return 0;

    io_registry_entry_t child;
    while ((child = IOIteratorNext(iter)) != 0) {
        if (conforms_to_any(child, classes)) {
            IOObjectRelease(iter);
            return child;
        }
        IOObjectRelease(child);
    }
    IOObjectRelease(iter);
    return 0;
}

static io_registry_entry_t find_parent_of_class(io_registry_entry_t entry, const char *const *classes) {
    io_registry_entry_t parent;
    kern_return_t kr = IORegistryEntryGetParentEntry(entry, kIOServicePlane, &parent);
    if (kr != KERN_SUCCESS) return 0;

    if (conforms_to_any(parent, classes)) {
        return parent;
    }
    IOObjectRelease(parent);
    return 0;
}

// Given an optical service (BD/DVD/CD), find the BSD name of its IOMedia child.
// Chain: service -> IO*BlockStorageDriver -> IOMedia (has "BSD Name")
static int bdsvc_to_bsd_name(io_registry_entry_t bdsvc, char *buf, size_t buflen) {
    io_registry_entry_t driver = find_child_of_class(bdsvc, k_optical_drivers);
    if (!driver) return 0;

    io_registry_entry_t media = find_iomedia_child(driver);
    IOObjectRelease(driver);
    if (!media) return 0;

    int ok = registry_entry_bsd_name(media, buf, buflen);
    IOObjectRelease(media);
    return ok;
}

// Given an optical service, extract Device Characteristics strings.
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

static int visit_bsd_name(io_service_t svc, void *ctx) {
    char name[64];
    return bdsvc_to_bsd_name(svc, name, sizeof(name))
        && strcmp(name, (const char *)ctx) == 0;
}

// Find the optical service that owns the given BSD name.
// Returns a retained io_service_t (caller must release), or 0.
static io_service_t find_bdsvc_by_bsd_name(mach_port_t mp, const char *bsd_name) {
    return for_each_optical_service(mp, visit_bsd_name, (void *)bsd_name);
}

// Find the optical service that owns the given BSD name by walking from
// IOMedia upward. Used as fallback when bdsvc_to_bsd_name fails
// (e.g. disc under exclusive access, no IOMedia child).
// Chain: IOMedia -> IO*BlockStorageDriver -> IO*Services
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
            io_registry_entry_t driver = find_parent_of_class(media, k_optical_drivers);
            if (driver) {
                io_registry_entry_t bdsvc = find_parent_of_class(driver, k_optical_services);
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

// ObtainExclusiveAccess gates SCSI but not diskarbitrationd, which can remount
// the disc mid-rip. We hold a DADiskClaim + mount-approval dissenter for the
// session so nothing remounts our disc until shim_close().

static char g_da_bsd[32];

// Dissent a remount only for OUR disk; every other disk is approved so we
// never block the rest of the system's volumes.
static DADissenterRef da_mount_approval(DADiskRef disk, void *ctx) {
    (void)ctx;
    const char *n = DADiskGetBSDName(disk);
    pthread_mutex_lock(&g_handle_lock);
    int ours = (n && strcmp(n, g_da_bsd) == 0);
    pthread_mutex_unlock(&g_handle_lock);
    if (!ours) return NULL;
    return DADissenterCreate(kCFAllocatorDefault, kDAReturnBusy,
        CFSTR("freemkv is reading this disc"));
}

// Refuse an involuntary claim release; we give it back only in shim_close().
static DADissenterRef da_claim_release(DADiskRef disk, void *ctx) {
    (void)disk; (void)ctx;
    return DADissenterCreate(kCFAllocatorDefault, kDAReturnBusy,
        CFSTR("freemkv still holds this disc"));
}

// Heap-allocated and reference-counted (rc=2: one ref for da_hold, one for the
// async callback) so neither the struct nor its semaphore is freed while the
// other side may still touch it. This matters on the 5 s timeout path, where
// da_hold returns but da_claim_done can still fire later — a stack DAClaimResult
// would be a use-after-free, and never releasing the semaphore would leak it.
typedef struct DAClaimResult {
    dispatch_semaphore_t sem; int ok; volatile int done; volatile int rc;
} DAClaimResult;

static void da_claim_result_release(DAClaimResult *r) {
    if (__sync_sub_and_fetch(&r->rc, 1) == 0) {
        dispatch_release(r->sem);
        free(r);
    }
}

static void da_claim_done(DADiskRef disk, DADissenterRef dissenter, void *ctx) {
    (void)disk;
    DAClaimResult *r = (DAClaimResult *)ctx;
    r->ok = (dissenter == NULL);
    __atomic_store_n(&r->done, 1, __ATOMIC_RELEASE);
    dispatch_semaphore_signal(r->sem);
    da_claim_result_release(r);
}

// Best-effort: claim the disk and register the mount-approval dissenter.
// Returns 1 if claimed. The caller proceeds either way — ObtainExclusiveAccess
// remains the hard gate; the claim is what keeps DA from remounting after it.
static int da_hold(const char *bsd_name, const volatile uint8_t *cancel) {
    // Hold the lock only for the g_da_bsd / g_handle field mutations below; it
    // is dropped before the bounded 5 s wait so legit claim waits aren't
    // serialized behind it (and re-taken just to publish g_handle.da_claimed).
    pthread_mutex_lock(&g_handle_lock);
    strncpy(g_da_bsd, bsd_name, sizeof(g_da_bsd) - 1);
    g_da_bsd[sizeof(g_da_bsd) - 1] = 0;

    g_handle.da_queue = dispatch_queue_create("io.freemkv.da", DISPATCH_QUEUE_SERIAL);
    if (!g_handle.da_queue) { pthread_mutex_unlock(&g_handle_lock); return 0; }
    g_handle.da_session = DASessionCreate(kCFAllocatorDefault);
    if (!g_handle.da_session) { pthread_mutex_unlock(&g_handle_lock); return 0; }
    DASessionSetDispatchQueue(g_handle.da_session, g_handle.da_queue);
    g_handle.da_disk =
        DADiskCreateFromBSDName(kCFAllocatorDefault, g_handle.da_session, bsd_name);
    if (!g_handle.da_disk) { pthread_mutex_unlock(&g_handle_lock); return 0; }

    DARegisterDiskMountApprovalCallback(g_handle.da_session, NULL, da_mount_approval, NULL);

    DAClaimResult *r = malloc(sizeof(DAClaimResult));
    if (!r) { pthread_mutex_unlock(&g_handle_lock); return 0; }
    r->sem = dispatch_semaphore_create(0);
    r->ok = 0;
    r->done = 0;
    r->rc = 2; // one ref held here, one for the async da_claim_done callback
    DADiskClaim(g_handle.da_disk, kDADiskClaimOptionDefault,
        da_claim_release, NULL, da_claim_done, r);
    pthread_mutex_unlock(&g_handle_lock);

    // Bounded wait (5 s) so a wedged DA can't hang open(). On timeout the
    // callback may still fire: keep our ref in da_pending for da_release,
    // which reaps it (and unclaims a late success) once callbacks are stopped.
    int claimed = 0;
    int waited = sliced_sem_wait(r->sem, SHIM_DA_CLAIM_MS, cancel);
    int timed_out = waited != 0;
    if (!timed_out) {
        claimed = r->ok;
        da_claim_result_release(r);
    }

    pthread_mutex_lock(&g_handle_lock);
    g_handle.da_claimed = claimed;
    if (timed_out) {
        if (g_handle.da_pending) da_claim_result_release(g_handle.da_pending);
        g_handle.da_pending = r;
    }
    pthread_mutex_unlock(&g_handle_lock);
    return waited == SHIM_CANCELLED ? SHIM_CANCELLED : claimed;
}

static void da_noop(void *ctx) { (void)ctx; }

static void da_release(void) {
    // Detach the DA state under the lock, tear it down outside it: draining the
    // queue runs da_mount_approval, which takes g_handle_lock itself.
    pthread_mutex_lock(&g_handle_lock);
    DASessionRef session = g_handle.da_session;
    DADiskRef disk = g_handle.da_disk;
    dispatch_queue_t queue = g_handle.da_queue;
    int claimed = g_handle.da_claimed;
    DAClaimResult *pending = g_handle.da_pending;
    g_handle.da_session = NULL;
    g_handle.da_disk = NULL;
    g_handle.da_queue = NULL;
    g_handle.da_claimed = 0;
    g_handle.da_pending = NULL;
    pthread_mutex_unlock(&g_handle_lock);

    if (session) DAUnregisterCallback(session, (void *)da_mount_approval, NULL);
    if (disk && claimed) DADiskUnclaim(disk);
    if (session) DASessionSetDispatchQueue(session, NULL);
    // No callback is scheduled past this point; flush any already in flight.
    if (queue) dispatch_sync_f(queue, NULL, da_noop);
    if (pending) {
        if (__atomic_load_n(&pending->done, __ATOMIC_ACQUIRE)) {
            // Late success: the claim is held even though da_hold gave up.
            if (disk && pending->ok) DADiskUnclaim(disk);
        } else {
            // Never delivered now: drop the callback's reference too.
            da_claim_result_release(pending);
        }
        da_claim_result_release(pending);
    }
    if (disk) CFRelease(disk);
    if (session) CFRelease(session);
    if (queue) dispatch_release(queue);
}

// ── Public API ────────────────────────────────────────────────────────────

// The IOReturn/HRESULT behind the last negative shim_open_exclusive result (0 if none).
static volatile int32_t g_last_open_kr;
// Selftest only: an unresolved BSD selector resolves to a stand-in service.
static volatile int g_selftest_fake_optical;

int32_t shim_last_open_kr(void) { return g_last_open_kr; }

int shim_open_exclusive(const char *selector, const volatile uint8_t *cancel) {
    kern_return_t kr;
    HRESULT hr;
    SInt32 score = 0;

    // Serialize the whole g_handle read-modify-write below: the entry guard is a
    // check-then-act TOCTOU, and a concurrent shim_close must not tear down scsi/
    // mmc/plugin mid-setup. Released before the self-locking da_hold (5 s wait).
    pthread_mutex_lock(&g_handle_lock);
    g_last_open_kr = 0;
    diagnostic("open selector=%s pid=%d euid=%d", selector, getpid(), geteuid());
    diagnostic_launch();

    // A Stop before the open does nothing at all: no unmount is started.
    if (shim_cancelled(cancel)) {
        pthread_mutex_unlock(&g_handle_lock);
        return SHIM_CANCELLED;
    }

    if (g_handle.exclusive && g_handle.scsi) {
        pthread_mutex_unlock(&g_handle_lock);
        return 0;
    }

    // The selector is user-supplied: resolve it to an optical drive BEFORE
    // unmounting, so a wrong /dev/diskN never touches another disk.
    if (strlen(selector) >= 32) {
        pthread_mutex_unlock(&g_handle_lock);
        return -1;
    }
    uint64_t registry_id = 0;
    int is_registry_selector = parse_registry_id_selector(selector, &registry_id);
    char bsd_name[32] = {0};
    io_service_t svc = 0;
    mach_port_t mp = MACH_PORT_NULL;
    // On failure IOMainPort leaves `mp` untouched; never use it unchecked.
    if (shim_main_port(&mp) != kIOReturnSuccess) {
        pthread_mutex_unlock(&g_handle_lock);
        return -1;
    }
    if (is_registry_selector) {
        svc = find_bdsvc_by_registry_id(mp, registry_id);
        if (svc) {
            // With an empty tray there is no BSD disk to unmount or claim.
            bdsvc_to_bsd_name(svc, bsd_name, sizeof(bsd_name));
        }
    } else {
        svc = find_bdsvc_by_bsd_name(mp, selector);
        if (!svc) svc = find_bdsvc_from_iomedia(mp, selector);
        if (!svc && g_selftest_fake_optical) svc = IORegistryGetRootEntry(mp);
        if (svc) strlcpy(bsd_name, selector, sizeof(bsd_name));
    }
    if (!svc) {
        diagnostic("stage=resolve_service result=not_found");
        pthread_mutex_unlock(&g_handle_lock);
        return -1;
    }

    diagnostic_service(svc);
    diagnostic("resolved BSD name=%s; empty means no media node to unmount/claim", bsd_name);

    // Unmount via diskutil, invoked directly with posix_spawn (no shell) so the
    // BSD device name is a discrete argv element and never shell syntax. Only a
    // name confirmed to belong to an optical drive above reaches this point. A
    // killed unmount is fine: ObtainExclusiveAccess below is the real gate (-5).
    // No settle sleep after it: a drive not yet released is waited for by the
    // ObtainExclusiveAccess retry loop, so a prompt unmount costs no fixed delay.
    if (bsd_name[0]) {
        char *const argv[] = {
            "diskutil", "unmountDisk", "force", bsd_name, NULL
        };
        // On cancel run_and_reap has killed and reaped diskutil (§2.9 M2); a cancel that
        // lands as the unmount ends still stops the open before the drive is claimed.
        if (run_and_reap("/usr/sbin/diskutil", argv, SHIM_DISKUTIL_BUDGET_MS, cancel, NULL)
                == SHIM_CANCELLED
            || shim_cancelled(cancel)) {
            IOObjectRelease(svc);
            pthread_mutex_unlock(&g_handle_lock);
            return SHIM_CANCELLED;
        }
    }

    diagnostic("stage=IOCreatePlugInInterfaceForService requested=kIOMMCDeviceUserClientTypeID interface=kIOCFPlugInInterfaceID");
    kr = IOCreatePlugInInterfaceForService(svc,
        kIOMMCDeviceUserClientTypeID, kIOCFPlugInInterfaceID,
        &g_handle.plugin, &score);
    diagnostic_result("IOCreatePlugInInterfaceForService", kr);
    diagnostic("plugin_present=%d score=%d", g_handle.plugin != NULL, (int)score);
    IOObjectRelease(svc);

    if (kr != KERN_SUCCESS || !g_handle.plugin) {
        g_last_open_kr = (int32_t)kr;
        pthread_mutex_unlock(&g_handle_lock);
        return -2;
    }

    hr = (*g_handle.plugin)->QueryInterface(g_handle.plugin,
        CFUUIDGetUUIDBytes(kIOMMCDeviceInterfaceID), (LPVOID *)&g_handle.mmc);
    diagnostic("stage=QueryInterface requested=kIOMMCDeviceInterfaceID HRESULT=0x%08x interface_present=%d", (unsigned)hr, g_handle.mmc != NULL);
    if (hr != S_OK || !g_handle.mmc) {
        g_last_open_kr = (int32_t)hr;
        diagnostic_result("IODestroyPlugInInterface", IODestroyPlugInInterface(g_handle.plugin));
        g_handle.plugin = NULL;
        pthread_mutex_unlock(&g_handle_lock);
        return -3;
    }

    g_handle.scsi = (*g_handle.mmc)->GetSCSITaskDeviceInterface(g_handle.mmc);
    diagnostic("stage=GetSCSITaskDeviceInterface interface_present=%d", g_handle.scsi != NULL);
    if (!g_handle.scsi) {
        g_last_open_kr = (int32_t)kIOReturnNoDevice;
        (*g_handle.mmc)->Release(g_handle.mmc);
        diagnostic_result("IODestroyPlugInInterface", IODestroyPlugInInterface(g_handle.plugin));
        g_handle.mmc = NULL;
        g_handle.plugin = NULL;
        pthread_mutex_unlock(&g_handle_lock);
        return -4;
    }

    int cancelled = 0;
    for (int retry = 0; retry < 10; retry++) {
        diagnostic("stage=ObtainExclusiveAccess attempt=%d", retry + 1);
        kr = (*g_handle.scsi)->ObtainExclusiveAccess(g_handle.scsi);
        diagnostic_result("ObtainExclusiveAccess", kr);
        if (kr == kIOReturnSuccess) break;
        if (sliced_sleep(SHIM_SETTLE_MS, cancel) == SHIM_CANCELLED) { cancelled = 1; break; }
    }
    if (kr != kIOReturnSuccess) {
        g_last_open_kr = (int32_t)kr;
        (*g_handle.scsi)->Release(g_handle.scsi);
        (*g_handle.mmc)->Release(g_handle.mmc);
        diagnostic_result("IODestroyPlugInInterface", IODestroyPlugInInterface(g_handle.plugin));
        g_handle.scsi = NULL;
        g_handle.mmc = NULL;
        g_handle.plugin = NULL;
        pthread_mutex_unlock(&g_handle_lock);
        return cancelled ? SHIM_CANCELLED : -5;
    }

    g_handle.exclusive = 1;
    pthread_mutex_unlock(&g_handle_lock);

    // Claim the disk now that we hold the drive, so DA can't remount it during
    // the read. Best-effort: a failed claim does not fail the open (we already
    // have exclusive SCSI access and the disc is unmounted).
    //
    // TOCTOU note: the lock is released above, so a concurrent shim_close could
    // tear g_handle down in the gap before da_hold runs. That is SAFE, not a
    // use-after-free: da_hold re-takes g_handle_lock and builds its
    // DiskArbitration state (da_queue / da_session / da_disk) from scratch,
    // entirely under the lock and keyed only off its bsd_name argument — it never
    // dereferences a g_handle field it read before acquiring the lock, so a
    // teardown in the gap just leaves fresh state to overwrite. If any of those
    // allocations fails it returns early (0 → no claim). The lock is deliberately
    // NOT held across da_hold's ~5 s self-locking claim wait, which would
    // serialize every open behind it.
    // A cancel during the claim wait undoes the whole open; da_release reaps the pending claim.
    if (bsd_name[0] && da_hold(bsd_name, cancel) == SHIM_CANCELLED) {
        shim_close();
        return SHIM_CANCELLED;
    }

    return 0;
}

void shim_close(void) {
    da_release(); // takes g_handle_lock on its own; keep it out of the region below
    pthread_mutex_lock(&g_handle_lock);
    if (g_handle.exclusive && g_handle.scsi) {
        diagnostic_result("ReleaseExclusiveAccess",
            (*g_handle.scsi)->ReleaseExclusiveAccess(g_handle.scsi));
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
        diagnostic_result("IODestroyPlugInInterface", IODestroyPlugInInterface(g_handle.plugin));
        g_handle.plugin = NULL;
    }
    g_handle.exclusive = 0;
    pthread_mutex_unlock(&g_handle_lock);
}

int shim_execute(const unsigned char *cdb, unsigned char cdb_len,
                 void *buf, unsigned int buf_len, int data_in,
                 unsigned char *sense_out, unsigned int sense_len,
                 unsigned char *task_status_out, unsigned long long *transfer_count,
                 unsigned int timeout_ms) {
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

    // §2.9 M1: the caller's timeout_ms, not a fixed 30 s. SCSITaskLib.h SetTimeoutDuration:
    // "The timeout duration is counted in milliseconds."
    (*task)->SetTimeoutDuration(task, timeout_ms);

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
int shim_media_present(const char *selector) {
    mach_port_t mp;
    if (shim_main_port(&mp) != kIOReturnSuccess) return -1;

    uint64_t registry_id = 0;
    if (parse_registry_id_selector(selector, &registry_id)) {
        io_service_t svc = find_bdsvc_by_registry_id(mp, registry_id);
        if (!svc) return 0;
        char bsd_name[32] = {0};
        int has_media = bdsvc_to_bsd_name(svc, bsd_name, sizeof(bsd_name));
        IOObjectRelease(svc);
        return has_media && bsd_name[0] ? 1 : 0;
    }

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
            && strcmp(name, selector) == 0)
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
// Walks optical (BD/DVD/CD) service entries in the IOKit registry. No exclusive
// access, no SCSI commands, no unmounts. Returns up to max_entries drives.

typedef struct { ShimDriveInfo *out; int count; int max; } ListCtx;

static int visit_list(io_service_t svc, void *ctx) {
    ListCtx *lc = (ListCtx *)ctx;
    ShimDriveInfo *info = &lc->out[lc->count];
    memset(info, 0, sizeof(*info));

    bdsvc_device_info(svc, info);
    // IOMedia (and its diskN name) disappears during exclusive access. The
    // optical service survives: always use its ID so a busy drive cannot be
    // enumerated as a second, apparently empty drive after opening it.
    registry_id_selector(svc, info->device_selector, sizeof(info->device_selector));
    // Only present while a disc is mounted; the drive's user-visible name.
    bdsvc_to_bsd_name(svc, info->bsd_name, sizeof(info->bsd_name));
    if (info->device_selector[0]) lc->count++;
    return lc->count >= lc->max ? 2 : 0;
}

int shim_list_drives(ShimDriveInfo *out, int max_entries) {
    mach_port_t mp;
    if (max_entries <= 0 || shim_main_port(&mp) != kIOReturnSuccess) return 0;

    ListCtx lc = { out, 0, max_entries };
    for_each_optical_service(mp, visit_list, &lc);
    return lc.count;
}

// ── shim_selftest hooks (stop design §5.1 LM1) ────────────────────────────
//
// Entry points for macos.rs's `shim_selftest_*` tests. They drive the same static
// helpers shim_open_exclusive uses, so no drive is needed (qa release-tests, D6).

__attribute__((visibility("hidden"))) int shim_selftest_wait_slice_ms(void) { return SHIM_WAIT_SLICE_MS; }

__attribute__((visibility("hidden"))) int shim_selftest_sleep(unsigned int ms, const volatile uint8_t *cancel) {
    return sliced_sleep(ms, cancel);
}

// A fresh semaphore, signalled up front when `signalled` is set, waited on like the DA claim.
__attribute__((visibility("hidden"))) int shim_selftest_sem_wait(unsigned int ms, int signalled, const volatile uint8_t *cancel) {
    dispatch_semaphore_t sem = dispatch_semaphore_create(0);
    if (!sem) return -1;
    if (signalled) dispatch_semaphore_signal(sem);
    int rc = sliced_sem_wait(sem, ms, cancel);
    dispatch_release(sem);
    return rc;
}

__attribute__((visibility("hidden"))) int shim_selftest_run_and_reap(const char *path, const char *arg, unsigned int budget_ms,
                               const volatile uint8_t *cancel, int *pid_out) {
    char *const argv[] = { (char *)path, (char *)arg, NULL };
    pid_t pid = 0;
    int rc = run_and_reap(path, argv, budget_ms, cancel, &pid);
    if (pid_out) *pid_out = (int)pid;
    return rc;
}

// 1 once `pid` is reaped. wait(2) ECHILD: "The process specified by pid does not exist or
// is not a child of the calling process". A zombie is reaped here and reported as 0.
__attribute__((visibility("hidden"))) int shim_selftest_reaped(int pid) {
    int status;
    errno = 0;
    return waitpid((pid_t)pid, &status, WNOHANG) < 0 && errno == ECHILD;
}

// A fake SCSITaskDeviceInterface that records the timeout shim_execute sets on its task.
static volatile UInt32 g_selftest_timeout_ms;

static ULONG selftest_release(void *self) { (void)self; return 0; }
static IOReturn selftest_release_exclusive(void *self) { (void)self; return kIOReturnSuccess; }
static IOReturn selftest_set_cdb(void *task, UInt8 *cdb, UInt8 size) {
    (void)task; (void)cdb; (void)size; return kIOReturnSuccess;
}
static IOReturn selftest_set_sg(void *task, SCSITaskSGElement *sg, UInt8 entries,
                                UInt64 count, UInt8 direction) {
    (void)task; (void)sg; (void)entries; (void)count; (void)direction; return kIOReturnSuccess;
}
static IOReturn selftest_set_timeout(void *task, UInt32 ms) {
    (void)task; g_selftest_timeout_ms = ms; return kIOReturnSuccess;
}
// What the fake's ExecuteTaskSync reports; defaults to success (GOOD, 0 bytes, no sense).
static volatile IOReturn g_selftest_kr;
static volatile SCSITaskStatus g_selftest_status;
static volatile UInt64 g_selftest_count;
static UInt8 g_selftest_sense[32];

static IOReturn selftest_execute(void *task, SCSI_Sense_Data *sense, SCSITaskStatus *status,
                                 UInt64 *count) {
    (void)task;
    *status = g_selftest_status;
    *count = g_selftest_count;
    memcpy(sense, g_selftest_sense, sizeof(*sense) < sizeof(g_selftest_sense) ? sizeof(*sense) : sizeof(g_selftest_sense));
    return g_selftest_kr;
}

static SCSITaskInterface g_selftest_task_vt = {
    .Release = selftest_release,
    .SetCommandDescriptorBlock = selftest_set_cdb,
    .SetScatterGatherEntries = selftest_set_sg,
    .SetTimeoutDuration = selftest_set_timeout,
    .ExecuteTaskSync = selftest_execute,
};
static SCSITaskInterface *g_selftest_task = &g_selftest_task_vt;

static SCSITaskInterface **selftest_create_task(void *self) { (void)self; return &g_selftest_task; }

static SCSITaskDeviceInterface g_selftest_device_vt = {
    .Release = selftest_release,
    .ReleaseExclusiveAccess = selftest_release_exclusive,
    .CreateSCSITask = selftest_create_task,
};
static SCSITaskDeviceInterface *g_selftest_device = &g_selftest_device_vt;

// Install the fake as the open handle; shim_close tears it down. -1 if a handle is open.
__attribute__((visibility("hidden"))) int shim_selftest_install_fake_device(void) {
    pthread_mutex_lock(&g_handle_lock);
    if (g_handle.scsi) { pthread_mutex_unlock(&g_handle_lock); return -1; }
    g_handle.scsi = &g_selftest_device;
    g_handle.exclusive = 1;
    g_selftest_timeout_ms = 0;
    g_selftest_kr = kIOReturnSuccess;
    g_selftest_status = kSCSITaskStatus_GOOD;
    g_selftest_count = 0;
    memset(g_selftest_sense, 0, sizeof(g_selftest_sense));
    pthread_mutex_unlock(&g_handle_lock);
    return 0;
}

// Sets what the fake device's next tasks report: IOReturn, SCSI status, bytes moved, and
// (32 bytes, or NULL for none) the sense data.
__attribute__((visibility("hidden"))) void shim_selftest_set_execute(int kr, unsigned char status,
                                                                     unsigned long long count,
                                                                     const unsigned char *sense) {
    g_selftest_kr = (IOReturn)kr;
    g_selftest_status = (SCSITaskStatus)status;
    g_selftest_count = count;
    memset(g_selftest_sense, 0, sizeof(g_selftest_sense));
    if (sense) memcpy(g_selftest_sense, sense, sizeof(g_selftest_sense));
}

// Makes shim_open_exclusive treat any BSD selector as an optical drive (its
// plugin creation then fails), so a cancel in the unmount/settle wait is testable.
__attribute__((visibility("hidden"))) void shim_selftest_fake_optical(int on) { g_selftest_fake_optical = on; }

__attribute__((visibility("hidden"))) unsigned int shim_selftest_last_timeout_ms(void) { return g_selftest_timeout_ms; }
