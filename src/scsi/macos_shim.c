#include <IOKit/IOKitLib.h>
#include <IOKit/IOCFPlugIn.h>
#include <IOKit/scsi/SCSITaskLib.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    IOCFPlugInInterface      **plugin;
    MMCDeviceInterface       **mmc;
    SCSITaskDeviceInterface  **scsi;
    int                        exclusive;
} ShimHandle;

static ShimHandle g_handle = {NULL, NULL, NULL, 0};

int shim_open_exclusive(const char *bsd_name) {
    kern_return_t kr;
    HRESULT hr;
    SInt32 score = 0;

    if (g_handle.exclusive && g_handle.scsi) {
        return 0;
    }

    char cmd[128];
    snprintf(cmd, sizeof(cmd), "diskutil unmountDisk force %s 2>/dev/null", bsd_name);
    system(cmd);
    usleep(500000);

    mach_port_t mp;
    IOMainPort(0, &mp);

    CFMutableDictionaryRef matching = IOServiceMatching("IOBDServices");
    io_service_t svc = IOServiceGetMatchingService(mp, matching);
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

    kr = (*g_handle.scsi)->ObtainExclusiveAccess(g_handle.scsi);
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
