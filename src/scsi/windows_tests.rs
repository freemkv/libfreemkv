use super::*;

// A drive root like `D:\` must open the volume, not the root directory.
#[test]
fn normalize_device_path_maps_drive_forms_to_the_volume_path() {
    assert_eq!(normalize_device_path("D:"), r"\\.\D:");
    assert_eq!(normalize_device_path(r"D:\"), r"\\.\D:");
    assert_eq!(normalize_device_path(r"\\.\D:"), r"\\.\D:");
    assert_eq!(normalize_device_path(r"\\.\CdRom0"), r"\\.\CdRom0");
    assert_eq!(normalize_device_path("CdRom0"), r"\\.\CdRom0");
}

// Regression guard: `StorageAdapterDescriptor` must match `STORAGE_ADAPTER_DESCRIPTOR`
// (winioctl.h) field-for-field.
#[test]
fn storage_adapter_descriptor_matches_sdk_layout() {
    use std::mem::{offset_of, size_of};
    assert_eq!(offset_of!(StorageAdapterDescriptor, Version), 0);
    assert_eq!(offset_of!(StorageAdapterDescriptor, Size), 4);
    assert_eq!(
        offset_of!(StorageAdapterDescriptor, MaximumTransferLength),
        8
    );
    assert_eq!(
        offset_of!(StorageAdapterDescriptor, MaximumPhysicalPages),
        12
    );
    assert_eq!(offset_of!(StorageAdapterDescriptor, AlignmentMask), 16);
    assert_eq!(offset_of!(StorageAdapterDescriptor, AdapterUsesPio), 20);
    assert_eq!(offset_of!(StorageAdapterDescriptor, AdapterScansDown), 21);
    assert_eq!(offset_of!(StorageAdapterDescriptor, CommandQueueing), 22);
    assert_eq!(
        offset_of!(StorageAdapterDescriptor, AcceleratedTransfer),
        23
    );
    // The fields that were misplaced by the old `u8` BusType.
    assert_eq!(offset_of!(StorageAdapterDescriptor, BusType), 24);
    assert_eq!(offset_of!(StorageAdapterDescriptor, BusMajorVersion), 28);
    assert_eq!(offset_of!(StorageAdapterDescriptor, BusMinorVersion), 30);
    assert_eq!(size_of::<StorageAdapterDescriptor>(), 32);
}

// Regression guard for `ScsiPassThroughDirect`, cross-checked against the SDK's
// `SCSI_PASS_THROUGH_DIRECT` (natural alignment, no pack). Do NOT add `packed(4)`
#[test]
#[cfg(target_pointer_width = "64")]
fn scsi_pass_through_direct_matches_sdk_layout_win64() {
    use std::mem::{offset_of, size_of};
    assert_eq!(offset_of!(ScsiPassThroughDirect, Length), 0);
    assert_eq!(offset_of!(ScsiPassThroughDirect, DataTransferLength), 12);
    assert_eq!(offset_of!(ScsiPassThroughDirect, TimeOutValue), 16);
    assert_eq!(offset_of!(ScsiPassThroughDirect, DataBuffer), 24);
    assert_eq!(offset_of!(ScsiPassThroughDirect, SenseInfoOffset), 32);
    assert_eq!(offset_of!(ScsiPassThroughDirect, Cdb), 36);
    assert_eq!(size_of::<ScsiPassThroughDirect>(), 56);
    // `sense` immediately follows the 56-byte spt; SenseInfoOffset points
    // here via offset_of! in execute(), so this must stay consistent.
    assert_eq!(offset_of!(SptwbDirect, spt), 0);
    assert_eq!(offset_of!(SptwbDirect, sense), 56);
    assert_eq!(size_of::<SptwbDirect>(), 56 + K_SENSE_SIZE);
}

// Cross-checked against `STORAGE_PROPERTY_QUERY` (winioctl.h): both
// enums are `int`-sized, so PropertyId@0, QueryType@4,
// AdditionalParameters@8, size 12.
#[test]
fn storage_property_query_matches_sdk_layout() {
    use std::mem::{offset_of, size_of};
    assert_eq!(offset_of!(StoragePropertyQuery, PropertyId), 0);
    assert_eq!(offset_of!(StoragePropertyQuery, QueryType), 4);
    assert_eq!(offset_of!(StoragePropertyQuery, AdditionalParameters), 8);
    assert_eq!(size_of::<StoragePropertyQuery>(), 12);
}

// Cross-checked against the Windows SDK headers: each IOCTL is asserted against an
// independent re-derivation of `CTL_CODE`, not just its literal.
#[test]
fn windows_ffi_constants_match_sdk() {
    // CTL_CODE re-derivation. FILE_DEVICE_CONTROLLER=0x4,
    // FILE_DEVICE_MASS_STORAGE=0x2D, METHOD_BUFFERED=0,
    // FILE_ANY_ACCESS=0, FILE_READ_ACCESS=1, FILE_WRITE_ACCESS=2.
    const fn ctl_code(dev: u32, func: u32, method: u32, access: u32) -> u32 {
        (dev << 16) | (access << 14) | (func << 2) | method
    }
    // ntddscsi.h: CTL_CODE(IOCTL_SCSI_BASE, 0x0405, METHOD_BUFFERED, READ|WRITE)
    assert_eq!(
        IOCTL_SCSI_PASS_THROUGH_DIRECT,
        ctl_code(0x4, 0x405, 0, 1 | 2)
    );
    // winioctl.h: CTL_CODE(IOCTL_STORAGE_BASE, 0x0500, METHOD_BUFFERED, ANY)
    assert_eq!(IOCTL_STORAGE_QUERY_PROPERTY, ctl_code(0x2D, 0x500, 0, 0));

    // winioctl.h STORAGE_PROPERTY_ID / STORAGE_QUERY_TYPE enums.
    assert_eq!(STORAGE_ADAPTER_PROPERTY, 1); // StorageAdapterProperty (Device=0, Adapter=1)
    assert_eq!(PROPERTY_STANDARD_QUERY, 0); // PropertyStandardQuery

    // ntddscsi.h SCSI_IOCTL_DATA_*.
    assert_eq!(SCSI_IOCTL_DATA_OUT, 0);
    assert_eq!(SCSI_IOCTL_DATA_IN, 1);
    assert_eq!(SCSI_IOCTL_DATA_UNSPECIFIED, 2);

    // winnt.h GENERIC_* / FILE_SHARE_* / FILE_ATTRIBUTE_*; fileapi.h OPEN_EXISTING.
    assert_eq!(GENERIC_READ, 0x8000_0000);
    assert_eq!(GENERIC_WRITE, 0x4000_0000);
    assert_eq!(FILE_SHARE_READ, 0x1);
    assert_eq!(FILE_SHARE_WRITE, 0x2);
    assert_eq!(OPEN_EXISTING, 3);
    assert_eq!(FILE_ATTRIBUTE_NORMAL, 0x80);
    assert_eq!(INVALID_HANDLE_VALUE, -1);
}
