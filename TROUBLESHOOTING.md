# Troubleshooting Guide

Common problems and solutions for optical drive ripping with freemkv.

---

## 1. USB-SATA Bridge Issues

This is the single most common source of problems when ripping discs over USB.

### Symptoms

- The drive disappears mid-rip. The ripping tool reports the device is gone, and `ls /dev/sg*` no longer shows it.
- The device re-enumerates under a different name: `sg4` becomes `sg5`, then `sg7`, then `sg11` after each USB port reset.
- `dmesg` shows USB port resets: `usb X-Y: reset high-speed USB device`, `xhci_hcd 0000:00:14.0: Cannot enable. Maybe the USB cable is bad?`, or `usb-storage: device reset failed`.
- The SCSI layer reports `host_status=7` (Linux USB transport error) in sense data.
- The drive works fine for reading data discs or burning, but crashes when hitting damaged sectors during a rip.
- After the crash, the drive is completely invisible until physically unplugged and reconnected.

### Root Cause

USB-SATA bridges translate between the USB Mass Storage protocol (BOT or UAS) and the drive's native SATA interface. When the optical drive encounters an unreadable sector, it returns a SCSI CHECK CONDITION with sense key 0x03 (MEDIUM ERROR). Some bridge chipsets -- particularly the Initio INIC-36xx family -- have buggy firmware that mishandles this error response.

Specific failure modes:

- **Incorrect residue reporting.** The bridge claims a different number of bytes transferred than what actually occurred. The Linux USB storage driver sees this discrepancy as a protocol violation and resets the port to recover. The `US_FL_IGNORE_RESIDUE` quirk exists specifically for this class of bug (see `drivers/usb/storage/transport.c` in the Linux kernel).
- **Bridge firmware crash.** On some Initio bridges, a malformed SCSI error response from the drive causes the bridge MCU to hang entirely. The USB controller sees the device stop responding and initiates a port reset. The bridge recovers (it re-enumerates), but the rip is dead -- all state is lost.
- **Sense data corruption.** The bridge forwards garbled or truncated sense data to the host, which the SCSI midlayer cannot parse, leading to a transport reset.

This is a hardware + firmware problem, not a software bug. The same drive connected via direct SATA does not exhibit these symptoms.

### Known Affected Bridges

| Chipset | USB IDs | Notes |
|---------|---------|-------|
| Initio INIC-3609 | `13fd:3609` | Very common in cheap SATA-to-USB enclosures. Highly problematic. |
| Initio INIC-3619 | `13fd:3940` | Same firmware family as INIC-3609. |
| Initio INIC-3069 | `13fd:0840` | Older variant, same residue bug. |
| ASMedia ASM1051 | `174c:5106` | Early ASM SATA bridge. Residue issues on error paths. |
| JMicron JMB36x | `152d:0561` | Some firmware versions. Not all JMicroon chips are affected. |

If your drive came in a pre-built external enclosure (Vantec, Sabrent, OWC, etc.), it almost certainly uses one of these bridge chips internally.

### The Fix: USB Storage Quirk

Apply the `US_FL_IGNORE_RESIDUE` kernel quirk for your bridge. This tells the Linux USB storage driver to ignore the residue field in SCSI response frames, preventing the port reset on mismatched byte counts.

**Step 1: Identify your bridge's vendor:product ID.**

```bash
lsusb
```

Look for your drive's entry. Example output:

```
Bus 002 Device 005: ID 13fd:0840 Initio Corporation INIC-3609
```

Here the vendor ID is `13fd` and the product ID is `0840`.

**Step 2: Apply the quirk at runtime.**

```bash
echo "13fd:0840:i" > /sys/module/usb_storage/parameters/quirks
```

Replace `13fd:0840` with your device's actual IDs. The `:i` flag means `US_FL_IGNORE_RESIDUE`.

You can combine multiple flags. Common additions:

- `:i` -- ignore residue (`US_FL_IGNORE_RESIDUE`)
- `:u` -- force BOT mode instead of UAS, for bridges with UAS bugs

**Step 3: Reconnect the drive.** Unplug and replug the USB cable, or bind/unbind the device. The quirk is applied per-module-load, so existing sessions may need the drive reconnected.

### Making It Persistent

Add the quirk to your kernel boot parameters so it survives reboots.

Edit `/etc/default/grub` (GRUB) and add to `GRUB_CMDLINE_LINUX_DEFAULT`:

```
GRUB_CMDLINE_LINUX_DEFAULT="quiet usb_storage.quirks=13fd:0840:i"
```

Then rebuild the GRUB config:

```bash
sudo update-grub
```

For systemd-boot, add to your loader entry or `/etc/kernel/cmdline`:

```
usb_storage.quirks=13fd:0840:i
```

Multiple devices can be separated by commas:

```
usb_storage.quirks=13fd:0840:i,174c:5106:u
```

### Recommended Bridges

If you are buying a USB-SATA adapter or enclosure for optical drive use:

| Bridge | USB IDs | Notes |
|--------|---------|-------|
| ASMedia ASM1153 | `174c:1153` | Reliable. Widely available in SATA-USB 3.0 cables. |
| JMicron JMS578 | `152d:0578` | Good firmware. Supports UASP. |
| Icy Box IB-AC640-C3 | N/A | Uses a known-good bridge internally. Plug-and-play. |

Avoid any enclosure or adapter listing an Initio chipset.

### Best Solution: Direct SATA

Connect your optical drive directly to a motherboard SATA port. This eliminates the USB-SATA bridge entirely and is the most reliable configuration:

- No USB protocol overhead or translation errors.
- No bridge firmware bugs.
- No port resets or re-enumeration.
- Full SATA error recovery handled natively by the kernel's libata driver.
- Sustained read speeds are limited only by the drive, not the USB bus.

If your machine has a free SATA port, use it.

---

## 2. Damaged Disc Handling

### Symptoms

- SCSI MEDIUM ERROR (sense key 0x03) at specific LBAs. `dmesg` shows `sr X:0:0:0: [srY] Unrecoverable read error` or similar.
- Read speed drops to near zero when approaching a damaged area.
- The drive makes audible retrying noises (laser repositioning, spindle speed changes).
- On USB-connected drives: the bridge crashes (see section 1 above) when the drive returns the error.

### How freemkv Handles This

freemkv uses a three-layer recovery model. See [`docs/rip-recovery.md`](docs/rip-recovery.md) for full details.

- **Pass 1 (Disc::copy):** Fast sweep with 64 KB reads. On failure, zero-fills the block and skips forward. Writes a ddrescue-format mapfile for later retry.
- **Pass 2+ (Disc::patch):** Targeted re-reads of bad ranges with a long 30-second timeout per CDB. The drive firmware performs its own ECC and laser power retries within that window.
- **In-stream (DiscStream):** Adaptive batch halving -- reduces request size on failure to isolate bad sectors within a larger block.

This means a disc with some bad sectors will still produce a usable ISO. The damaged areas are zero-filled in pass 1 and retried in subsequent passes. Structure-protected sectors (deliberate unreadable regions from copy protection) will never yield, which is expected.

### The Drive Taint Issue (LG BU40N)

Some drives, notably the LG BU40N, exhibit a "taint" behavior after encountering MEDIUM ERRORs:

1. The drive hits a damaged sector and returns a MEDIUM ERROR.
2. From that point forward, **all subsequent reads fail** -- even reads to sectors that were previously successful.
3. The only recovery is to physically unplug and reconnect the drive (or power-cycle it).

This is not a freemkv bug. It is a drive firmware behavior triggered by the interaction between the drive's internal error recovery and the USB-SATA bridge's handling of the error response. The drive firmware enters a degraded state that it does not recover from without a power cycle.

Workarounds:

- **Use a direct SATA connection.** This eliminates the bridge interaction that triggers the taint.
- **Use a different bridge.** The ASM1153 and JMS578 are less likely to trigger this behavior.
- **Accept the partial ISO.** freemkv's skip-forward recovery will zero-fill the unreadable blocks and continue. The resulting ISO may be playable with minor glitches in the affected areas.
- **Physical replug between retry passes.** If running multi-pass patch, replug the drive between passes to clear the taint state.

freemkv deliberately does not attempt inline SCSI resets or eject cycles to recover from this state, because those operations were found to make the problem worse on affected hardware (see the design rationale in [`docs/rip-recovery.md`](docs/rip-recovery.md)).

---

## 3. Drive Not Detected

### Check Hardware Visibility

```bash
lsusb
```

Verify the drive appears in the USB device list. If it does not show up, the drive is not visible to the host at all -- check cables, power, and USB port.

```bash
ls /dev/sg*
```

On Linux, optical drives appear as `/dev/sg*` devices (the SCSI Generic interface). freemkv uses `/dev/sg*`, not `/dev/sr*`. If `lsusb` shows the device but no `/dev/sg*` entry exists, the `sg` kernel module may not be loaded:

```bash
sudo modprobe sg
```

### Check Kernel Messages

```bash
dmesg | grep -i usb | tail -30
dmesg | grep -i sg | tail -10
```

Look for:
- USB enumeration errors or failed port resets.
- `sg_add` messages confirming the sg device was registered.
- Permission denied or access errors.

### Permission Issues

On most Linux distributions, `/dev/sg*` devices are owned by `root:disk` or `root:cdrom` with restricted permissions. Running freemkv as an unprivileged user will fail with permission errors.

Options:

- Add your user to the appropriate group:

  ```bash
  sudo usermod -aG disk $USER
  ```

  Then log out and back in for the change to take effect. On some distributions the group is `cdrom` or `optical` instead of `disk`.

- Run with elevated privileges:

  ```bash
  sudo freemkv ...
  ```

- Install a udev rule for persistent per-device permissions. Create `/etc/udev/rules.d/99-sg-optical.rules`:

  ```
  SUBSYSTEM=="scsi_generic", ATTRS{type}=="5", MODE="0666"
  ```

  Then reload udev rules:

  ```bash
  sudo udevadm control --reload-rules && sudo udevadm trigger
  ```

### Spin-Up Delay

Optical drives take 30-60 seconds to spin up and become ready after hot-plug or disc insertion. During this window, SCSI commands may return NOT READY or timeout.

freemkv's `Drive::wait_ready()` handles this automatically by polling with TEST UNIT READY until the drive responds. If you are writing your own code using the library, always call `wait_ready()` before `init()`:

```rust
let mut drive = Drive::open(Path::new("/dev/sg4"))?;
drive.wait_ready()?;  // blocks until disc is ready, up to 30s
drive.init()?;
```

If the drive was just plugged in, wait a full minute before concluding it is not detected.

---

## 4. How to Identify Your USB-SATA Bridge

If you are experiencing the issues described in section 1, you need to know which bridge chipset your adapter or enclosure uses.

### Step 1: Find the Device

```bash
lsusb
```

Look for entries matching your drive or enclosure. Bridges may appear under their own manufacturer name or as a generic SATA device. Common examples:

```
Bus 002 Device 005: ID 13fd:0840 Initio Corporation
Bus 002 Device 006: ID 174c:1153 ASMedia Technology Inc. ASM1153
Bus 002 Device 007: ID 152d:0578 JMicron Technology Corp. JMS578
```

### Step 2: Match the IDs

| Vendor | Product ID | Chipset | Status |
|--------|-----------|---------|--------|
| `13fd` | `3609` | Initio INIC-3609 | Affected. Apply quirk. |
| `13fd` | `3940` | Initio INIC-3619 | Affected. Apply quirk. |
| `13fd` | `0840` | Initio INIC-3069 | Affected. Apply quirk. |
| `174c` | `5106` | ASMedia ASM1051 | Affected (early firmware). Apply quirk. |
| `174c` | `1153` | ASMedia ASM1153 | Good. No quirk needed. |
| `152d` | `0561` | JMicron JMB36x | Affected (some firmware). Apply quirk if issues occur. |
| `152d` | `0578` | JMicron JMS578 | Good. No quirk needed. |

### Step 3: Check dmesg for the Bridge Name

```bash
dmesg | grep -i "usb-storage\|uas\|initio\|asmedia\|jmicron"
```

This often reveals the bridge chipset even when `lsusb` shows a generic name.

### Step 4: If the Enclosure Is Sealed

Many external drive enclosures (Vantec NexStar, Sabrent, OWC, etc.) do not advertise the bridge chipset on the packaging. In this case:

1. Check `lsusb` while the enclosure is connected.
2. Search the vendor:product ID online -- there are community-maintained lists of which chipsets popular enclosures use.
3. If you cannot determine the chipset and are experiencing bridge crashes, assume it is an Initio and apply the quirk with its IDs.
4. The definitive test: connect the bare drive to a motherboard SATA port. If the problems disappear, the bridge was the cause.

---

## 5. General Debugging Checklist

When something goes wrong during a rip, gather this information before filing an issue:

1. **freemkv version:** `freemkv --version` or the crate version in `Cargo.toml`.
2. **Drive model:** from the drive label, or from `freemkv info`.
3. **Connection type:** USB (with bridge chipset if known) or direct SATA.
4. **Operating system and kernel:** `uname -a`.
5. **Kernel messages during the failure:** `dmesg | tail -50` immediately after the crash.
6. **SCSI device:** which `/dev/sg*` the drive was on, and whether it changed after the failure.
7. **The disc:** title, format (BD/DVD/UHD), condition.

Include all of the above in bug reports. SCSI transport errors that resolve with the `US_FL_IGNORE_RESIDUE` quirk or by switching to direct SATA are bridge firmware bugs, not freemkv bugs.
