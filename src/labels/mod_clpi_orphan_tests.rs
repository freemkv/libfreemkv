// Build a CLPI ProgramInfo section for one program from (pid, stream_coding_info) pairs,
// per crate::clpi::parse_program_info.
fn build_program_info(streams: &[(u16, Vec<u8>)]) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(0); // reserved
    body.push(1); // num_programs = 1
    body.extend_from_slice(&0u32.to_be_bytes()); // spn_program_sequence_start
    body.extend_from_slice(&0u16.to_be_bytes()); // program_map_pid
    body.push(streams.len() as u8); // num_streams
    body.push(0); // num_groups
    for (pid, sci) in streams {
        body.extend_from_slice(&pid.to_be_bytes());
        body.push(sci.len() as u8);
        body.extend_from_slice(sci);
    }
    let mut out = Vec::new();
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

// Build a full CLPI buffer (HDMV header + ProgramInfo) for (pid, coding_type, lang)
// streams.
pub(super) fn build_clpi(streams: &[(u16, u8, &str)]) -> Vec<u8> {
    use crate::consts::coding_type as c;
    let sci_streams: Vec<(u16, Vec<u8>)> = streams
        .iter()
        .map(|(pid, coding, lang)| {
            let lang_bytes = lang.as_bytes();
            let sci = match *coding {
                c::PG | c::IG => {
                    let mut v = vec![*coding];
                    v.extend_from_slice(lang_bytes);
                    v
                }
                _ => {
                    let mut v = vec![*coding, 0x61];
                    v.extend_from_slice(lang_bytes);
                    v
                }
            };
            (*pid, sci)
        })
        .collect();
    let pi = build_program_info(&sci_streams);
    let mut buf = vec![0u8; 60];
    buf[0..4].copy_from_slice(b"HDMV");
    buf[4..8].copy_from_slice(b"0200");
    let prog_info_start: u32 = 60;
    buf[12..16].copy_from_slice(&prog_info_start.to_be_bytes());
    buf[56..60].copy_from_slice(&1000u32.to_be_bytes()); // source_packet_count
    buf.extend_from_slice(&pi);
    buf
}
