//! Bounded bootstrap-selected inputs. The same loader serves UDF and extracted
//! metadata fixtures; finding unrelated archives never establishes provenance.
use super::{Reject, Result, bootstrap, runtime};
use std::io::{Cursor, Read};

const METADATA_LIMIT: usize = 65536;
const JAR_LIMIT: usize = 32 * 1024 * 1024;
const TOTAL_LIMIT: usize = 80 * 1024 * 1024;

fn budgeted_read(
    read: &mut impl FnMut(&str, usize) -> Result<Vec<u8>>,
    path: &str,
    limit: usize,
    total: &mut usize,
) -> Result<Vec<u8>> {
    let remaining = TOTAL_LIMIT.checked_sub(*total).ok_or(Reject::Budget)?;
    if remaining == 0 {
        return Err(Reject::Budget);
    }
    // The EOF sentinel also consumes the aggregate read budget. A full
    // aggregate-boundary read cannot prove EOF, so reject it conservatively.
    let request = limit.checked_add(1).ok_or(Reject::Budget)?.min(remaining);
    let bytes = read(path, request)?;
    *total = total.checked_add(bytes.len()).ok_or(Reject::Budget)?;
    if bytes.len() >= request {
        return Err(Reject::Budget);
    }
    Ok(bytes)
}

pub(super) struct Assets {
    pub first_play: u16,
    pub movie_objects: Vec<u8>,
    #[cfg(test)]
    pub top_menu: String,
    #[cfg(test)]
    pub applications: bootstrap::Applications,
    #[cfg(test)]
    pub runtime_code: runtime::ClassSet,
    #[cfg(test)]
    pub dispatcher_code: runtime::ClassSet,
    pub code_version: runtime::CodeVersion,
    pub playlist_metadata: Vec<u8>,
    runtime_jar: Vec<u8>,
}

impl Assets {
    pub fn authored_file(&self, name: &str) -> Result<Vec<u8>> {
        if name.is_empty()
            || !name.is_ascii()
            || name.contains('\\')
            || name
                .split('/')
                .any(|p| p.is_empty() || matches!(p, "." | ".."))
        {
            return Err(Reject::Invalid);
        }
        let mut archive =
            zip::ZipArchive::new(Cursor::new(&self.runtime_jar)).map_err(|_| Reject::Invalid)?;
        let file = archive.by_name(name).map_err(|_| Reject::MissingAsset)?;
        let limit = super::binary::MAX_BYTES;
        if file.size() > limit as u64 {
            return Err(Reject::Budget);
        }
        let mut bytes = Vec::new();
        file.take((limit + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| Reject::Invalid)?;
        if bytes.len() > limit {
            return Err(Reject::Budget);
        }
        Ok(bytes)
    }
}

/// `read` must honor the byte limit without scanning file contents beyond it.
pub(super) fn load(mut read: impl FnMut(&str, usize) -> Result<Vec<u8>>) -> Result<Assets> {
    let mut total = 0usize;
    let mut bounded = |path: &str, limit: usize| -> Result<Vec<u8>> {
        budgeted_read(&mut read, path, limit, &mut total)
    };
    let index = bootstrap::index(&bounded("/BDMV/index.bdmv", METADATA_LIMIT)?)?;
    if !index.bdjos.contains(&index.top_menu) {
        return Err(Reject::Invalid);
    }
    let movie_objects = bounded("/BDMV/MovieObject.bdmv", super::binary::MAX_BYTES)?;
    if index.bdjos.len() > 16 {
        return Err(Reject::Budget);
    }
    let mut applications = None;
    for id in &index.bdjos {
        let bytes = bounded(&format!("/BDMV/BDJO/{id}.bdjo"), METADATA_LIMIT)?;
        let parsed = bootstrap::applications(&bytes)?;
        if applications.as_ref().is_some_and(|old| *old != parsed) {
            return Err(Reject::Unsupported);
        }
        applications = Some(parsed);
    }
    let applications = applications.ok_or(Reject::MissingAsset)?;
    let config = bounded("/BDMV/JAR/onQClient.cfg", 16384)?;
    let metadata_path = bootstrap::configuration(&config, &applications.runtime)?;
    let playlist_metadata = bounded(&metadata_path, super::binary::MAX_BYTES)?;
    let runtime = bounded(
        &format!("/BDMV/JAR/{}.jar", applications.runtime),
        JAR_LIMIT,
    )?;
    let extension = bounded(
        &format!("/BDMV/JAR/{}.jar", applications.extension),
        JAR_LIMIT,
    )?;
    let dispatcher = bounded(
        &format!("/BDMV/JAR/{}.jar", applications.dispatcher),
        JAR_LIMIT,
    )?;
    let runtime_code = runtime::ClassSet::read(&[&runtime, &extension])?;
    let dispatcher_code = runtime::ClassSet::read(&[&dispatcher])?;
    let code_version = runtime::version(&runtime_code, &dispatcher_code)?;
    Ok(Assets {
        first_play: index.first_play,
        movie_objects,
        #[cfg(test)]
        top_menu: index.top_menu,
        #[cfg(test)]
        applications,
        #[cfg(test)]
        runtime_code,
        #[cfg(test)]
        dispatcher_code,
        code_version,
        playlist_metadata,
        runtime_jar: runtime,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_file_sentinel_and_short_reads_share_the_actual_budget() {
        let mut total = TOTAL_LIMIT - 10;
        let mut observed = 0;
        for size in [3, 2] {
            let bytes = budgeted_read(
                &mut |_, request| {
                    assert_eq!(request, 4);
                    observed += size;
                    Ok(vec![0; size])
                },
                "small",
                3,
                &mut total,
            )
            .unwrap();
            assert_eq!(bytes.len(), size);
        }
        assert_eq!(total, TOTAL_LIMIT - 10 + observed);
        assert_eq!(
            budgeted_read(
                &mut |_, request| {
                    assert_eq!(request, 4);
                    observed += request;
                    Ok(vec![0; request])
                },
                "oversized",
                3,
                &mut total,
            ),
            Err(Reject::Budget)
        );
        assert_eq!(total, TOTAL_LIMIT - 10 + observed);
        assert!(total <= TOTAL_LIMIT);
    }

    #[test]
    fn cumulative_budget_clamps_next_read_before_allocation() {
        let mut total = TOTAL_LIMIT - 5;
        let mut read = |_: &str, limit| {
            assert_eq!(limit, 5);
            Ok(vec![0; limit])
        };
        assert!(matches!(
            budgeted_read(&mut read, "test", JAR_LIMIT, &mut total),
            Err(Reject::Budget)
        ));
        assert_eq!(total, TOTAL_LIMIT);
        total = TOTAL_LIMIT - 5;
        budgeted_read(
            &mut |_, limit| {
                assert_eq!(limit, 5);
                Ok(vec![0; 4])
            },
            "test",
            JAR_LIMIT,
            &mut total,
        )
        .unwrap();
        assert_eq!(total, TOTAL_LIMIT - 1);
        assert_eq!(
            budgeted_read(&mut |_, limit| Ok(vec![0; limit]), "last", 1, &mut total),
            Err(Reject::Budget)
        );
        assert_eq!(total, TOTAL_LIMIT);
        assert!(
            budgeted_read(
                &mut |_, _| panic!("exhausted budget must not read"),
                "test",
                1,
                &mut total
            )
            .is_err()
        );
    }

    #[test]
    fn absent_or_oversized_index_never_searches_other_inputs() {
        let mut calls = 0;
        assert!(
            load(|path, _| {
                calls += 1;
                assert_eq!(path, "/BDMV/index.bdmv");
                Err(Reject::MissingAsset)
            })
            .is_err()
        );
        assert_eq!(calls, 1);
        assert!(load(|_, limit| Ok(vec![0; limit])).is_err());
    }

    #[test]
    #[ignore = "bounded cached metadata only; no ISO scan or Java execution"]
    fn actual_bootstrap_inputs_use_the_same_bounded_loader() {
        let jar = std::path::PathBuf::from(std::env::var("ONQ_TEST_JAR").unwrap());
        let directory = jar.parent().unwrap();
        let cached = |path: &str, limit: usize| -> Result<Vec<u8>> {
            let leaf = path.rsplit('/').next().ok_or(Reject::Invalid)?;
            let file =
                std::fs::File::open(directory.join(leaf)).map_err(|_| Reject::MissingAsset)?;
            let mut bytes = Vec::new();
            file.take(limit as u64)
                .read_to_end(&mut bytes)
                .map_err(|_| Reject::Invalid)?;
            Ok(bytes)
        };
        let assets = load(cached).unwrap();
        assert_eq!(assets.top_menu, "00000");
        assert_eq!(assets.first_play, 0);
        assert!(assets.movie_objects.starts_with(b"MOBJ"));
        assert_eq!(assets.applications.runtime, "00002");
        assert_eq!(assets.code_version, runtime::CodeVersion::OnqUhdV1);
        assert!(!assets.playlist_metadata.is_empty());
        assert_ne!(
            assets.runtime_code.fingerprint(),
            assets.dispatcher_code.fingerprint()
        );
        assert!(super::super::qco::parse(&assets.authored_file("FS.QCO").unwrap()).is_ok());
        for bad in ["../FS.QCO", "/FS.QCO", "FS.QCO/../FS.QCO", "absent.qcs"] {
            assert!(assets.authored_file(bad).is_err());
        }
        for missing in [
            "MovieObject.bdmv",
            "00000.bdjo",
            "00002.jar",
            "44444.jar",
            "onQClient.cfg",
            "playlists.xml",
        ] {
            assert!(
                load(|path, limit| if path.ends_with(missing) {
                    Err(Reject::MissingAsset)
                } else {
                    cached(path, limit)
                })
                .is_err()
            );
        }
        assert!(
            load(|path, limit| {
                let mut bytes = cached(path, limit)?;
                if path.ends_with("00001.bdjo") {
                    bytes[0] ^= 1;
                }
                Ok(bytes)
            })
            .is_err()
        );
    }
}
