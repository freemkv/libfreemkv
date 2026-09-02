# Linux `sg*` device enumeration

`enumerate_sg_names` (in `src/drive/linux.rs`) lists `/dev/sgN` SCSI-generic
device names to probe for optical drives.

Linux assigns `/dev/sgN` sequentially across *all* SCSI-generic devices
(disks, tape, HBAs, optical), not just optical drives. That means a fixed
`sg0..15` range can miss an optical drive on a host with many other SCSI
targets ahead of it in enumeration order.

To avoid that, the function prefers the exact present-device list from
`/sys/class/scsi_generic/`, which enumerates only the `sg*` nodes that
actually exist. It falls back to a bounded `sg0..15` probe only when sysfs
itself is unreadable, which happens in some minimal containers that lack a
mounted `/sys`.
