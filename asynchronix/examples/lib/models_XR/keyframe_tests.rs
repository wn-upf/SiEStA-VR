use super::*;

/// The byte-by-byte HEVC scan `is_keyframe` used before memchr (same loop bound, same start-code rules).
fn reference_hevc(frame: &[u8]) -> bool {
    if frame.len() < 3 {
        return false;
    }
    for i in 0..frame.len().saturating_sub(5) {
        if (frame[i] == 0 && frame[i + 1] == 0 && frame[i + 2] == 1)
            || (frame[i] == 0 && frame[i + 1] == 0 && frame[i + 2] == 0 && frame[i + 3] == 1)
        {
            let start_code_len = if frame[i + 2] == 0 { 4 } else { 3 };
            let nal_header_pos = i + start_code_len;
            if nal_header_pos < frame.len() && (16..=21).contains(&((frame[nal_header_pos] >> 1) & 0x3F)) {
                return true;
            }
        }
    }
    false
}

#[test]
fn hevc_keyframe_scan_matches_the_byte_by_byte_one() {
    // bytes drawn from a small alphabet so start codes, 4-byte forms and IRAP headers (16..=21 << 1) occur often,
    // at every position including the last bytes, whose handling the loop bound decides
    let alphabet = [0u8, 0, 0, 1, 1, 16 << 1, 19 << 1, 21 << 1, 22 << 1, 1 << 1, 0xFF, 0x40];
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    };
    let (mut cases, mut keyframes) = (0, 0);
    for len in 0..48usize {
        for _ in 0..4000 {
            let frame: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            let want = reference_hevc(&frame);
            assert_eq!(is_keyframe(&frame, VideoCodec::HEVC), want, "frame {frame:?}");
            cases += 1;
            keyframes += want as usize;
        }
    }
    assert!(keyframes > 1000 && keyframes < cases, "the generator must hit both answers ({keyframes} of {cases})");
    // a long frame with one IRAP start code near the end, and one just past the scanned range
    let mut big = vec![7u8; 140_000];
    big[139_990..139_994].copy_from_slice(&[0, 0, 1, 19 << 1]);
    assert_eq!(is_keyframe(&big, VideoCodec::HEVC), reference_hevc(&big));
    let mut edge = vec![7u8; 1000];
    edge[995..999].copy_from_slice(&[0, 0, 1, 19 << 1]);
    assert_eq!(is_keyframe(&edge, VideoCodec::HEVC), reference_hevc(&edge));
}
