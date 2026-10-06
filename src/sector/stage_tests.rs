use super::*;
use crate::error::{E_CSS_KEY_MISSING, E_NO_DISC_KEY, error_code};
use std::io::Cursor;

// A DVD-Video pack: 14-byte pack header (stuffing 0), a PES of `id` at 0x0E whose
// MPEG-2 flags byte (0x14) carries `scramble`, and a varied body.
fn dvd_pack(id: u8, scramble: u8) -> Vec<u8> {
    let mut p: Vec<u8> = (0..SECTOR_BYTES).map(|i| (i * 37 + 11) as u8).collect();
    p[..4].copy_from_slice(&PACK_START);
    p[4] = 0x44;
    p[0x0D] = 0xF8;
    p[0x0E..0x11].copy_from_slice(&[0, 0, 1]);
    p[0x11] = id;
    p[0x12..0x14].copy_from_slice(&0x07ECu16.to_be_bytes());
    p[0x14] = 0x80 | (scramble << 4);
    p
}

fn ts_unit(cpi: bool) -> Vec<u8> {
    let mut u: Vec<u8> = (0..ALIGNED_UNIT_LEN)
        .map(|i| (i * 7 + 3) as u8 | 1)
        .collect();
    for p in u.chunks_mut(BD_SOURCE_PACKET_BYTES) {
        p[0] = if cpi { p[0] | 0xC0 } else { p[0] & 0x3F };
        p[4] = 0x47;
    }
    u
}

fn noise(n: usize) -> Vec<u8> {
    (0..n).map(|i| ((i * 2_654_435_761) >> 13) as u8).collect()
}

#[test]
fn containers_and_arbitrary_bytes_are_opaque() {
    let mut mkv = vec![0x1A, 0x45, 0xDF, 0xA3, 0x47];
    mkv.extend(noise(FILE_HEAD));
    let mut mp4 = vec![0, 0, 0, 0x20];
    mp4.extend(b"ftypisom");
    mp4.extend(noise(FILE_HEAD));
    let mut fmkv = b"FMKV\0\x01\0\0".to_vec();
    fmkv.extend(noise(FILE_HEAD));
    let mut ifo = b"DVDVIDEO-VTS".to_vec();
    ifo.extend(noise(FILE_HEAD));
    // A 188-byte TS (sync at byte 0, not 4) carries no AACS unit.
    let mut ts188 = Vec::new();
    for _ in 0..200 {
        ts188.push(0x47);
        ts188.extend(noise(187));
    }
    for (name, head) in [
        ("mkv", mkv),
        ("mp4", mp4),
        ("fmkv", fmkv),
        ("ifo", ifo),
        ("udf zeros", vec![0u8; FILE_HEAD]),
        ("noise", noise(FILE_HEAD)),
        ("ts188", ts188),
        ("empty", Vec::new()),
    ] {
        assert_eq!(classify(&head), Kind::Opaque, "{name}");
    }
}

#[test]
fn pack_streams_and_source_packets_are_recognised() {
    let mut ps = dvd_pack(0xE0, 0);
    ps.extend(noise(SECTOR_BYTES));
    assert_eq!(classify(&ps), Kind::Ps { mpeg2: true });
    let mut later = noise(SECTOR_BYTES);
    later.extend(dvd_pack(0xE0, 1));
    assert_eq!(
        classify(&later),
        Kind::Opaque,
        "a pack past the head of another stream"
    );
    let mut mpeg1 = dvd_pack(0xE0, 0);
    mpeg1[4] = 0x21;
    mpeg1[12..15].copy_from_slice(&[0, 0, 1]);
    assert_eq!(classify(&mpeg1), Kind::Ps { mpeg2: false });
    // Review: an MP4 whose first box is 0x1BA bytes opens `00 00 01 BA 'f'`.
    let mut mp4 = vec![0, 0, 1, 0xBA];
    mp4.extend(b"ftypisom");
    mp4.extend(noise(FILE_HEAD));
    assert_eq!(classify(&mp4), Kind::Opaque);
    let clear: Vec<u8> = (0..4).flat_map(|_| ts_unit(false)).collect();
    assert_eq!(classify(&clear), Kind::BdTs);
    let mut enc = ts_unit(true);
    enc[BD_SOURCE_PACKET_BYTES + 4] = 0x00; // an encrypted rest loses its syncs
    for p in enc[BD_SOURCE_PACKET_BYTES..].chunks_mut(BD_SOURCE_PACKET_BYTES) {
        p[4] = 0x5C;
    }
    assert_eq!(classify(&enc), Kind::BdTs);
    let mut noisy = noise(ALIGNED_UNIT_LEN);
    noisy[0] |= 0xC0;
    noisy[4] = 0x47;
    noisy.extend(noise(ALIGNED_UNIT_LEN));
    assert_eq!(
        classify(&noisy),
        Kind::Opaque,
        "a second unit with no seed sync"
    );
}

// A stream shorter than one unit is BD-TS only if every source packet is synced at byte 4.
#[test]
fn a_short_stream_is_bd_ts_only_when_every_packet_syncs() {
    let unit = ts_unit(false);
    let short = unit[..3 * BD_SOURCE_PACKET_BYTES].to_vec();
    assert_eq!(classify(&short), Kind::BdTs);
    let mut one_bad = short.clone();
    one_bad[BD_SOURCE_PACKET_BYTES + 4] = 0x00;
    assert_eq!(classify(&one_bad), Kind::Opaque, "one packet off sync");
    let mut sync_at_0 = short;
    for p in sync_at_0.chunks_mut(BD_SOURCE_PACKET_BYTES) {
        p[0] = 0x47;
        p[4] = 0x00;
    }
    assert_eq!(classify(&sync_at_0), Kind::Opaque, "sync belongs at byte 4");
}

// KS-5: a decrypter that leaves CPI set on units it has already decrypted still streams:
// clear TS is never a missing-key refusal, wherever its CPI flag says otherwise.
#[test]
fn clear_ts_with_a_cpi_flag_is_not_refused() {
    let bytes: Vec<u8> = (0..4).flat_map(|_| ts_unit(true)).collect();
    let mut stage = Stage::lazy(Cursor::new(bytes.clone()), false);
    let mut out = Vec::new();
    stage.read_to_end(&mut out).expect("clear TS is not E7022");
    assert_eq!(out, bytes);
    assert_eq!(stage.kind(), Some(Kind::BdTs));
}

// An Opaque stream is handed through byte-identical, even if a later sector looks like a
// scrambled pack (a VOB muxed into an MKV attachment stays untouched).
#[test]
fn an_opaque_stream_passes_untouched() {
    let mut bytes = b"FMKV\0\x01\0\0".to_vec();
    bytes.resize(SECTOR_BYTES, 0);
    bytes.extend(dvd_pack(0xE0, 1));
    bytes.extend(ts_unit(true));
    let mut stage = Stage::lazy(Cursor::new(bytes.clone()), false);
    let mut out = Vec::new();
    stage.read_to_end(&mut out).unwrap();
    assert_eq!(out, bytes);
    assert_eq!(stage.kind(), Some(Kind::Opaque));
}

#[test]
fn a_byte_stream_refuses_encrypted_content_unless_raw() {
    let mut ps = dvd_pack(0xE0, 0);
    ps.extend(dvd_pack(0xE0, 1));
    let mut ts = ts_unit(false);
    let mut enc = ts_unit(true);
    for p in enc[BD_SOURCE_PACKET_BYTES..].chunks_mut(BD_SOURCE_PACKET_BYTES) {
        p[4] = 0x5C;
    }
    ts.extend(enc);
    for (bytes, code) in [(ps, E_CSS_KEY_MISSING), (ts, E_NO_DISC_KEY)] {
        let mut out = Vec::new();
        let e = Stage::lazy(Cursor::new(bytes.clone()), false)
            .read_to_end(&mut out)
            .unwrap_err();
        assert_eq!(error_code(&e), Some(code));
        let mut raw = Vec::new();
        Stage::lazy(Cursor::new(bytes.clone()), true)
            .read_to_end(&mut raw)
            .unwrap();
        assert_eq!(raw, bytes, "raw passes ciphertext");
    }
}

// A block straddling reads is judged once complete; a seek re-syncs to the next boundary.
#[test]
fn watched_blocks_are_judged_across_reads_and_seeks() {
    let mut ps = dvd_pack(0xE0, 0);
    ps.extend(dvd_pack(0xE0, 0));
    ps.extend(dvd_pack(0xE0, 2));
    let mut stage = Stage::eager(Cursor::new(ps.clone()), false).unwrap();
    let mut small = [0u8; 700];
    let mut e = None;
    for _ in 0..10 {
        if let Err(x) = stage.read(&mut small) {
            e = Some(x);
            break;
        }
    }
    assert_eq!(error_code(&e.unwrap()), Some(E_CSS_KEY_MISSING));
    let mut stage = Stage::eager(Cursor::new(ps), false).unwrap();
    stage.seek(SeekFrom::Start(100)).unwrap();
    let mut two = vec![0u8; 2 * SECTOR_BYTES - 100];
    stage.read_exact(&mut two).unwrap();
    let mut rest = Vec::new();
    assert!(
        stage.read_to_end(&mut rest).is_err(),
        "the scrambled pack after the seek"
    );
}

// X-3: ONE pack test for disc and file: every DVD-Video stream id and scramble value; an
// IFO/UDF sector (the 38→10-titles guard) or a pack whose 0x14 only looks flagged (stuffed,
// map-first, MPEG-1 PES) is never scrambled, so the disc path leaves it untouched too.
#[test]
fn one_pack_test_serves_disc_and_file() {
    for id in [0xBDu8, 0xBE, 0xBF, 0xC0, 0xC7, 0xE0] {
        for scramble in 0..4u8 {
            let want = (scramble != 0 && !matches!(id, 0xBE | 0xBF)).then_some(0x14);
            let got = scrambled_at(&dvd_pack(id, scramble));
            assert_eq!(got, want, "id {id:#x} scramble {scramble}");
        }
    }
    let mut ifo = b"DVDVIDEO-VTS".to_vec();
    ifo.resize(SECTOR_BYTES, 0x30);
    let mut udf = vec![0u8; SECTOR_BYTES];
    udf[..6].copy_from_slice(&[0x02, 0x00, 0x02, 0x00, 0x30, 0x30]);
    let mut stuffed = dvd_pack(0xE0, 0);
    stuffed[0x0D] = 0xF8 | 1;
    stuffed[0x14] = 0x30;
    let mut map_first = dvd_pack(0xBC, 0);
    map_first[0x14] = 0xB0;
    let mut mpeg1_pes = dvd_pack(0xE0, 0);
    mpeg1_pes[0x14] = 0xFF;
    for s in [
        ifo,
        udf,
        vec![0x30; SECTOR_BYTES],
        stuffed,
        map_first,
        mpeg1_pes,
    ] {
        assert_eq!(scrambled_at(&s), None);
        let mut region = s.clone();
        crate::css::descramble_region(&mut region, &mut [0x42; 5]).unwrap();
        assert_eq!(region, s, "the disc path's descramble leaves it untouched");
    }
}

// A VOB with a blank or damaged first sector is still a PS to a file stage, and one
// damaged unit at a clip's start does not hide its source packets.
#[test]
fn damage_at_the_head_does_not_hide_the_stream() {
    let mut vob = vec![0u8; SECTOR_BYTES];
    vob.extend(dvd_pack(0xE0, 1));
    assert_eq!(
        classify(&vob),
        Kind::Opaque,
        "a byte stream needs a pack at 0"
    );
    assert_eq!(classify_sectors(&vob), Kind::Ps { mpeg2: true });
    let mut clip = vec![0u8; ALIGNED_UNIT_LEN];
    clip.extend((0..3).flat_map(|_| ts_unit(true)));
    assert_eq!(classify(&clip), Kind::BdTs, "a zeroed first unit");
    let mut garbled = noise(ALIGNED_UNIT_LEN);
    garbled.extend((0..3).flat_map(|_| ts_unit(false)));
    assert_eq!(classify(&garbled), Kind::BdTs, "a garbled first unit");
}

// An FMKV stream is decided from five bytes: its header is parsed with no more data.
#[test]
fn a_stream_header_is_not_held_for_a_whole_unit() {
    struct Trickle(Vec<u8>, usize);
    impl Read for Trickle {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            assert!(self.1 < self.0.len(), "the stage read past the sent header");
            let n = buf.len().min(self.0.len() - self.1);
            buf[..n].copy_from_slice(&self.0[self.1..self.1 + n]);
            self.1 += n;
            Ok(n)
        }
    }
    let header = b"FMKV\0\x01\0\0header".to_vec();
    let mut stage = Stage::lazy(Trickle(header.clone(), 0), false);
    let mut got = vec![0u8; header.len()];
    stage.read_exact(&mut got).unwrap();
    assert_eq!(got, header);
    assert_eq!(stage.kind(), Some(Kind::Opaque));
}

// Review: head bytes read before a transient error are kept for the retry.
#[test]
fn a_head_read_error_keeps_what_was_read() {
    struct Flaky(Vec<u8>, usize, bool);
    impl Read for Flaky {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.1 == 5 && !self.2 {
                self.2 = true;
                return Err(io::ErrorKind::ConnectionReset.into());
            }
            let n = buf.len().min(self.0.len() - self.1);
            buf[..n].copy_from_slice(&self.0[self.1..self.1 + n]);
            self.1 += n;
            Ok(n)
        }
    }
    let unit = ts_unit(false);
    let mut stage = Stage::lazy(Flaky(unit.clone(), 0, false), false);
    let mut out = Vec::new();
    assert!(stage.read_to_end(&mut out).is_err());
    stage.read_to_end(&mut out).unwrap();
    assert_eq!(out, unit);
}

// Review: a refused read leaves none of its raw bytes for a retry to serve.
#[test]
fn a_refused_read_is_refused_again() {
    struct Refuses;
    impl SectorSource for Refuses {
        fn capacity_sectors(&self) -> u32 {
            4
        }
        fn read_sectors(
            &mut self,
            _: u32,
            _: u16,
            buf: &mut [u8],
            _: bool,
        ) -> crate::error::Result<usize> {
            buf.fill(0xEE);
            Err(Error::CssKeyMissing)
        }
    }
    let mut bytes = SectorBytes::new(Refuses, 4 * SECTOR_BYTES as u64);
    let mut out = [0u8; 100];
    assert!(bytes.read(&mut out).is_err());
    assert!(
        bytes.read(&mut out).is_err(),
        "never the refused read's bytes"
    );
}

// N9: a source that ends early is an error, never a silent end of stream.
#[test]
fn a_source_ending_early_is_unexpected_eof() {
    struct Empty;
    impl SectorSource for Empty {
        fn capacity_sectors(&self) -> u32 {
            4
        }
        fn read_sectors(
            &mut self,
            _: u32,
            _: u16,
            _: &mut [u8],
            _: bool,
        ) -> crate::error::Result<usize> {
            Ok(0)
        }
    }
    let e = SectorBytes::new(Empty, 4 * SECTOR_BYTES as u64)
        .read(&mut [0u8; 100])
        .unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn sector_bytes_clip_at_the_real_length() {
    let data: Vec<u8> = noise(3 * SECTOR_BYTES + 700);
    let mut padded = data.clone();
    padded.resize(4 * SECTOR_BYTES, 0);
    let src = crate::test_util::MemSource::new(padded);
    let mut out = Vec::new();
    SectorBytes::new(src, data.len() as u64)
        .read_to_end(&mut out)
        .unwrap();
    assert_eq!(out, data);
}
