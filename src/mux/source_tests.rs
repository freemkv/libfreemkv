use super::*;

fn disc(volume_id: &str, meta_title: Option<&str>) -> Disc {
    let mut title = crate::disc::DiscTitle::empty();
    title.playlist = "VTS_11_26.VOB".into();
    Disc {
        volume_id: volume_id.into(),
        meta_title: meta_title.map(str::to_string),
        format: crate::disc::DiscFormat::Dvd,
        capacity_sectors: 1,
        capacity_bytes: 2048,
        layers: 1,
        titles: vec![title],
        region: crate::disc::DiscRegion::Free,
        aacs: None,
        css: None,
        encrypted: false,
        aacs_error: None,
        css_error: None,
        content_format: crate::disc::ContentFormat::MpegPs,
    }
}

fn image(d: Disc, folder: bool) -> Source<'static> {
    Source {
        origin: Origin::Image {
            path: PathBuf::from("x"),
            folder,
            reader: Box::new(crate::test_util::MemSource::new(vec![0u8; 2048])),
            disc: d,
        },
    }
}

// An image and its extracted folder name a title alike: the folder's volume id is its
// directory name, so with no meta title both keep the title's playlist name, and the
// engine's pre-scanned image title agrees.
#[test]
fn an_image_and_its_folder_name_a_title_alike() {
    let iso = image(disc("GREENLAND", None), false);
    let dir = image(disc("fmt_dir", None), true);
    assert_eq!(iso.title_name(), None);
    assert_eq!(dir.title_name(), None);
    let scanned = ScannedTitle::of(&disc("GREENLAND", None), 0).unwrap();
    assert_eq!(scanned.disc_name, None);
    assert_eq!(scanned.title.playlist, "VTS_11_26.VOB");

    let iso = image(disc("GREENLAND", Some("Greenland")), false);
    let dir = image(disc("fmt_dir", Some("Greenland")), true);
    assert_eq!(iso.title_name().as_deref(), Some("Greenland"));
    assert_eq!(dir.title_name().as_deref(), Some("Greenland"));
    let scanned = ScannedTitle::of(&disc("GREENLAND", Some("Greenland")), 0).unwrap();
    assert_eq!(scanned.disc_name.as_deref(), Some("Greenland"));
}
