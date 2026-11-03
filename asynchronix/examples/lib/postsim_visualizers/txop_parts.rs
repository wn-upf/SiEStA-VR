//! The parts of an SU TXOP of this MAC, for the overhead overlay (`O`). Every TXOP is RTS, SIFS, CTS, SIFS, the data PPDU
//! (EHT preamble, then data symbols; no packet extension), SIFS and a BlockAck, all inside the airtime `airtime_ampdu`
//! (mod.rs) returns, and a collision is RTS, SIFS, CTS (`collision_delay`). The control frames' lengths here use the same
//! expressions; the data symbols are what is left of the airtime.

use crate::lib::{eht_phy_duration, LEGACY_PHY_DURATION, SIFS, SYMBOL_TIME_11AX, SYMBOL_TIME_LEGACY};

/// Basic-rate bits per legacy symbol (16-QAM 1/2, 48 subcarriers), SERVICE and tail bits, as `airtime_ampdu` uses them.
const BASIC_BITS_PER_SYMBOL: f64 = 1.0 / 2.0 * 4.0 * 48.0;
const SERVICE_BITS: f64 = 16.0;
const TAIL_BITS: f64 = 18.0;

fn legacy_frame(mac_bits: f64) -> f64 {
    LEGACY_PHY_DURATION + ((SERVICE_BITS + mac_bits + TAIL_BITS) / BASIC_BITS_PER_SYMBOL).ceil() * SYMBOL_TIME_LEGACY
}

/// RTS (20 bytes).
pub fn rts_s() -> f64 {
    legacy_frame(160.0)
}

/// CTS (14 bytes).
pub fn cts_s() -> f64 {
    legacy_frame(112.0)
}

/// BlockAck for an A-MPDU of `n_mpdus`: compressed bitmap up to 64 MPDUs, 256-bit bitmap above.
pub fn block_ack_s(n_mpdus: u32) -> f64 {
    let bitmap_bytes = if n_mpdus <= 64 { 8.0 } else { 32.0 };
    legacy_frame((24.0 + bitmap_bytes) * 8.0)
}

/// An SU TXOP of `airtime` seconds carrying `n_mpdus` MPDUs, split into its parts.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SuParts {
    pub rts: f64,
    pub cts: f64,
    pub preamble: f64,
    /// Data symbols of the PPDU.
    pub data: f64,
    pub block_ack: f64,
}

impl SuParts {
    /// None when the airtime is too short to hold the exchange (not a TXOP of this MAC).
    pub fn of(airtime: f64, n_mpdus: u32) -> Option<SuParts> {
        let (rts, cts, preamble, block_ack) = (rts_s(), cts_s(), eht_phy_duration(), block_ack_s(n_mpdus));
        let data = airtime - (rts + SIFS + cts + SIFS + preamble + SIFS + block_ack);
        // the MAC's airtime is rounded to the nanosecond
        (data > 0.5 * SYMBOL_TIME_11AX).then_some(SuParts { rts, cts, preamble, data, block_ack })
    }

    /// Start and end of the data symbols for a TXOP from `t` to `end`.
    pub fn data_window(&self, t: f64, end: f64) -> (f64, f64) {
        (t + self.rts + SIFS + self.cts + SIFS + self.preamble, end - SIFS - self.block_ack)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::{airtime_ampdu, collision_delay, padded_mpdu_bits, su_spatial_streams, Coords};

    /// (bits per subcarrier, coding rate) of each MCS of `airtime_ampdu`.
    const MCS: [(f64, f64); 14] = [(1.0, 0.5), (2.0, 0.5), (2.0, 0.75), (4.0, 0.5), (4.0, 0.75), (6.0, 0.5),
        (6.0, 2.0 / 3.0), (6.0, 0.75), (8.0, 0.75), (8.0, 5.0 / 6.0), (10.0, 0.75), (10.0, 5.0 / 6.0), (12.0, 0.75),
        (12.0, 5.0 / 6.0)];

    fn data_subcarriers(width: usize) -> f64 {
        match width { 320 => 3920.0, 160 => 1960.0, 80 => 980.0, 40 => 468.0, _ => 234.0 }
    }

    /// The parts add up to the MAC's airtime, and what is left for the data is the PPDU's own whole number of data
    /// symbols (computed here from the MCS the airtime function picked), for every width, distance and A-MPDU size,
    /// on both sides of the 64-MPDU BlockAck bitmap.
    #[test]
    fn parts_sum_to_the_airtime() {
        let mut checked = 0;
        for width in [20usize, 40, 80, 160, 320] {
            for d in [1.0, 4.0, 9.0, 15.0, 25.0, 40.0, 60.0, 90.0] {
                for n in [1u32, 2, 7, 33, 63, 64, 65, 100, 256] {
                    for app_bits in [800.0, 11_520.0] {
                        let bits = padded_mpdu_bits(app_bits) * n as f64;
                        let (src, dst) = (Coords::with_coords(0.0, 0.0, 0.0), Coords::with_coords(d, 0.0, 0.0));
                        let (airtime, mcs) = airtime_ampdu(bits, n as i32, src, dst, 20.0, width);
                        let (b, cr) = MCS[mcs as usize];
                        let rate = su_spatial_streams() * b * cr * data_subcarriers(width);
                        let symbols = ((16.0 + n as f64 * 32.0 + bits + 18.0) / rate).ceil();
                        let p = SuParts::of(airtime, n).expect("a TXOP of this MAC");
                        assert!((p.data - symbols * SYMBOL_TIME_11AX).abs() < 1e-12,
                                "width {width} d {d} n {n}: data {} vs {} symbols", p.data, symbols);
                        let sum = p.rts + SIFS + p.cts + SIFS + p.preamble + p.data + SIFS + p.block_ack;
                        assert!((sum - airtime).abs() < 1e-12);
                        // the TXOP as the viewer gets it, rounded to the nanosecond like the MAC's `Duration`
                        let (t, end) = (1.0, 1.0 + std::time::Duration::from_secs_f64(airtime).as_secs_f64());
                        let (d0, d1) = SuParts::of(end - t, n).unwrap().data_window(t, end);
                        assert!(((d1 - d0) - p.data).abs() < 2e-9);
                        checked += 1;
                    }
                }
            }
        }
        assert_eq!(checked, 5 * 8 * 9 * 2);
        for (got, us) in [(rts_s(), 32.0), (cts_s(), 28.0), (block_ack_s(64), 36.0), (block_ack_s(65), 44.0)] {
            assert!((got * 1e6 - us).abs() < 1e-9, "{got} s vs {us} us");
        }
    }

    /// A collision is the RTS, SIFS and CTS of an exchange.
    #[test]
    fn collision_is_rts_sifs_cts() {
        assert_eq!(collision_delay(), (rts_s() + SIFS + cts_s()) as f32);
    }
}
