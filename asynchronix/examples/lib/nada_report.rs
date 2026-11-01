//! A NADA receiver report on the client's clock (RFC 8698 §5.2: one every 100 ms whether or not video arrived), for
//! `XRSIM_NADA_FIX=2` (main's `xr_protocol::nada_report`, backported for the paper's revision). It rides in a tracking
//! packet, after the `Tracking` header: this build's tracking packet has no zeroed padding, so the packet that carries a
//! report is `LEN` bytes longer (one packet in ~27 at 90 fps). Fields little-endian:
//!
//! | bytes | field |
//! |---|---|
//! | 0-3 | magic `NAD2` |
//! | 4-7 | `seq` (u32), the report's number in the session |
//! | 8-15 | `xcurr` (f64), the aggregate congestion signal, ms |
//! | 16 | `rmode` (i8), 0 accelerated ramp-up, 1 gradual update |
//! | 17-24 | `recv` (i64), the receive rate over the report's interval, b/s |
//! | 25-32 | `plr` (f64), the smoothed loss ratio |
//! | 33-40 | `d_tilde` (f64), ms |
//! | 41-48 | `d_queue` (i64), us |

pub const MAGIC: [u8; 4] = *b"NAD2";
pub const LEN: usize = 49;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NadaTimerReport {
    pub seq: u32,
    pub xcurr: f64,
    pub rmode: i8,
    pub recv: i64,
    pub plr: f64,
    pub d_tilde: f64,
    pub d_queue: i64,
}

impl NadaTimerReport {
    pub fn encode(&self) -> [u8; LEN] {
        let mut b = [0u8; LEN];
        b[0..4].copy_from_slice(&MAGIC);
        b[4..8].copy_from_slice(&self.seq.to_le_bytes());
        b[8..16].copy_from_slice(&self.xcurr.to_le_bytes());
        b[16] = self.rmode as u8;
        b[17..25].copy_from_slice(&self.recv.to_le_bytes());
        b[25..33].copy_from_slice(&self.plr.to_le_bytes());
        b[33..41].copy_from_slice(&self.d_tilde.to_le_bytes());
        b[41..49].copy_from_slice(&self.d_queue.to_le_bytes());
        b
    }

    /// The report at the start of `b` (a tracking packet's padding), if there is one.
    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < LEN || b[0..4] != MAGIC {
            return None;
        }
        let f = |r: std::ops::Range<usize>| -> [u8; 8] { b[r].try_into().unwrap() };
        Some(Self {
            seq: u32::from_le_bytes(b[4..8].try_into().unwrap()),
            xcurr: f64::from_le_bytes(f(8..16)),
            rmode: b[16] as i8,
            recv: i64::from_le_bytes(f(17..25)),
            plr: f64::from_le_bytes(f(25..33)),
            d_tilde: f64::from_le_bytes(f(33..41)),
            d_queue: i64::from_le_bytes(f(41..49)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_report_round_trips_and_zeros_are_none() {
        let r = NadaTimerReport { seq: 7, xcurr: 12.5, rmode: 1, recv: 61_000_000, plr: 0.01, d_tilde: 2.5, d_queue: 3_000 };
        let mut pad = [0u8; 120];
        pad[..LEN].copy_from_slice(&r.encode());
        assert_eq!(NadaTimerReport::decode(&pad), Some(r));
        assert_eq!(NadaTimerReport::decode(&[0u8; 120]), None);
        assert_eq!(NadaTimerReport::decode(&pad[..LEN - 1]), None);
    }
}
