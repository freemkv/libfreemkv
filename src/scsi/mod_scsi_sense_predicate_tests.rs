//! Classification of [`ScsiSense`] predicate methods against SPC-4
//! §4.5.6 Table 28 sense keys. These drive `freemkv_engine::recovery::copy` hysteresis
//! and `freemkv_engine::recovery::patch` routing; a misclassification here silently
//! changes which sectors get retried vs. marked unreadable.
use super::*;

fn s(key: u8) -> ScsiSense {
    ScsiSense {
        sense_key: key,
        asc: 0,
        ascq: 0,
    }
}

#[test]
fn is_marginal_matches_exactly_the_recoverable_keys() {
    // Doc contract: marginal == {NO SENSE(0), RECOVERED(1),
    // NOT READY(2), MEDIUM ERROR(3), ABORTED COMMAND(B)}.
    // Everything else is non-marginal. Walk every 4-bit key value.
    let marginal: [u8; 5] = [
        SENSE_KEY_NO_SENSE,
        SENSE_KEY_RECOVERED_ERROR,
        SENSE_KEY_NOT_READY,
        SENSE_KEY_MEDIUM_ERROR,
        SENSE_KEY_ABORTED_COMMAND,
    ];
    for key in 0u8..=0x0F {
        let expect = marginal.contains(&key);
        assert_eq!(
            s(key).is_marginal(),
            expect,
            "key {key:#x} marginal classification"
        );
    }
}

#[test]
fn each_specific_predicate_is_exclusive() {
    // Each is_* predicate matches exactly its one key and no other.
    // Catches a copy-paste bug where e.g. is_not_ready compared the
    // wrong constant.
    type SenseCase = (u8, fn(&ScsiSense) -> bool);
    let cases: &[SenseCase] = &[
        (SENSE_KEY_MEDIUM_ERROR, ScsiSense::is_medium_error),
        (SENSE_KEY_HARDWARE_ERROR, ScsiSense::is_hardware_error),
        (SENSE_KEY_NOT_READY, ScsiSense::is_not_ready),
        (SENSE_KEY_UNIT_ATTENTION, ScsiSense::is_unit_attention),
        (SENSE_KEY_DATA_PROTECT, ScsiSense::is_data_protect),
        (SENSE_KEY_ILLEGAL_REQUEST, ScsiSense::is_illegal_request),
        (SENSE_KEY_ABORTED_COMMAND, ScsiSense::is_aborted_command),
    ];
    for &(key, pred) in cases {
        for other in 0u8..=0x0F {
            let got = pred(&s(other));
            assert_eq!(
                got,
                other == key,
                "predicate for key {key:#x} fired on {other:#x}"
            );
        }
    }
}

#[test]
fn none_constant_and_default_agree_and_are_no_sense() {
    // SPC-4 §4.5.3: empty sense reply is NO SENSE (key 0). Both the
    // NONE constant and Default must be the all-zero triple and be
    // classified marginal (NO SENSE is in the marginal set).
    assert_eq!(ScsiSense::NONE, ScsiSense::default());
    assert_eq!(ScsiSense::NONE.sense_key, SENSE_KEY_NO_SENSE);
    assert!(ScsiSense::NONE.is_marginal());
}

// is_css_locked must require the exact 05/6F/03 triple (all three
// fields ANDed, not ORed) — the CSS crack scan relies on it to
// distinguish "encrypted but locked" from "unreadable".
#[test]
fn is_css_locked_requires_exact_key_asc_ascq_triple() {
    // The real signature: true.
    assert!(
        ScsiSense {
            sense_key: SENSE_KEY_ILLEGAL_REQUEST,
            asc: 0x6F,
            ascq: 0x03,
        }
        .is_css_locked()
    );
    // Right key, wrong ASC only -> must be false (rules out `||`
    // between key and asc, and rules out the `true` constant mutant).
    assert!(
        !ScsiSense {
            sense_key: SENSE_KEY_ILLEGAL_REQUEST,
            asc: 0x00,
            ascq: 0x03,
        }
        .is_css_locked()
    );
    // Right key, right ASC, wrong ASCQ -> must be false (rules out `||`
    // between asc and ascq).
    assert!(
        !ScsiSense {
            sense_key: SENSE_KEY_ILLEGAL_REQUEST,
            asc: 0x6F,
            ascq: 0x00,
        }
        .is_css_locked()
    );
    // Right ASC/ASCQ but wrong key (e.g. a bare ILLEGAL REQUEST with
    // unrelated ASC/ASCQ would already fail above; here flip the key
    // instead) -> must be false.
    assert!(
        !ScsiSense {
            sense_key: SENSE_KEY_MEDIUM_ERROR,
            asc: 0x6F,
            ascq: 0x03,
        }
        .is_css_locked()
    );
}
