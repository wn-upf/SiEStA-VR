



const PE_DURATION: f64 = 0.0;         // 802.11ax/be Packet Extension (4-20 us, Max 20us for QAM-4096 STAs)
pub const LEGACY_PHY_DURATION:f64 = 20E-6; // microseconds
// pub const PHY_DURATION: f64 = 100E-6;
pub const EHT_PHY_DURATION: f64 = 76E-6;    // 802.11be Preamble 
                                                                // L-STF      :   8.00 us
                                                                // L-LTF      :   8.00 us
                                                                // L-SIG      :   4.00 us
                                                                // RL-SIG     :   4.00 us
                                                                // U-SIG      :   8.00 us
                                                                // EHT-SIG    :   8.00 us
                                                                // EHT-STF    :   4.00 us
                                                                // EHT-LTF    :  32.00 us
pub const SLOT: f64 = 9E-6;
pub const SIFS: f64 = 16E-6;

pub const SYMBOL_TIME_LEGACY: f64 = 4E-6; 
pub const SYMBOL_TIME_11AX: f64 = 16E-6; // 12.8 us symbol + 3.2 us guard interval. 

#[inline]
pub fn calculate_distance(x: f64, y: f64, z: f64, x_: f64, y_: f64, z_: f64) -> f64 {
    let dx = x_ - x;
    let dy = y_ - y;
    let dz = z_ - z;
    (dx * dx + dy * dy + dz * dz).sqrt()
}
#[inline]
pub fn path_loss(d: f64) -> f64 {
    let gamma = 2.06067_f64;
    54.12 + 10.0 * gamma * (d).log10() + 5.25 * 0.1467 * d
}

#[inline]
pub fn airtime_ampdu(
    total_bits_transmitted_app: f64,
    n_mpdus: i32,
    coords_src: Coords,
    coords_dest: Coords,
    _p_tx_orig: f64,
    channel_width: usize,
) -> (f64, u8, f64) {
    
    let p_tx_cheated = match channel_width { // small hack, higher widths get higher P_tx
        20 => 20.0,
        40 => 20.0,
        80 => 20.0,
        160 => 23.0,
        320 => 30.05,
        _ => 20.0, // default at 20 dBm
    };

    let effPt: f64 = p_tx_cheated;

    let SU_spatial_streams = 2.0;

    let distance = calculate_distance(
        coords_src.x,
        coords_src.y,
        coords_src.z,
        coords_dest.x,
        coords_dest.y,
        coords_dest.z,
    );

    // print_pink!("coords_src: {:?}, coords_dest: {:?}, DISTANCE = {:.4} m", coords_src, coords_dest, distance);
    let PL = path_loss(distance);
    let mut Pr = effPt - PL;

    // 3. Calculate the noise adjustment for wider channels.
    let noise_adjustment_db = match channel_width {
        40 => 3.01,
        80 => 6.02,
        160 => 9.03,
        320 => 12.04,
        _ => 0.0, // For 20 MHz or any other default
    };

    // 4. Normalize the Pr to its 20 MHz equivalent.
    Pr = Pr - noise_adjustment_db;

    // println!("AP to STA: I'm at {:?} and you're at {:?} |  Distance = {:.2}, PL = {:.2}, P_rx = {:.1}", coords_src, coords_dest, distance, PL, Pr);

    let (bits_symbol, coding_rate, _mcs_val) = match Pr {
        _ if Pr < -82.0 => (1, 1.0 / 2.0, 0), // Could add additional PER in this case
        _ if Pr >= -82.0 && Pr < -79.0 => (1, 1.0 / 2.0, 0),
        _ if Pr >= -79.0 && Pr < -77.0 => (2, 1.0 / 2.0, 1),
        _ if Pr >= -77.0 && Pr < -74.0 => (2, 3.0 / 4.0, 2),
        _ if Pr >= -74.0 && Pr < -70.0 => (4, 1.0 / 2.0, 3),
        _ if Pr >= -70.0 && Pr < -66.0 => (4, 3.0 / 4.0, 4),
        _ if Pr >= -66.0 && Pr < -65.0 => (6, 1.0 / 2.0, 5),
        _ if Pr >= -65.0 && Pr < -64.0 => (6, 2.0 / 3.0, 6),
        _ if Pr >= -64.0 && Pr < -59.0 => (6, 3.0 / 4.0, 7),
        _ if Pr >= -59.0 && Pr < -57.0 => (8, 3.0 / 4.0, 8),
        _ if Pr >= -57.0 && Pr < -55.0 => (8, 5.0 / 6.0, 9),
        _ if Pr >= -55.0 && Pr < -53.0 => (10, 3.0 / 4.0, 10),
        _ if Pr >= -53.0 && Pr < -49.0 => (10, 5.0 / 6.0, 11),
        _ if Pr >= -49.0 && Pr < -46.0 => (12, 3.0 / 4.0, 12), // MCS 12, TODO: find a good reference for 802.11be SNR
        _ if Pr >= -46.0 => (12, 5.0 / 6.0, 13),               // MCS 13
        _ => (1, 1.0 / 2.0, 1),                                // Catch-all for Pr out of range
    };


    let Subcarriers = match channel_width {
        320 => 3920, // 320 MHz: data subcarriers (EHT / Wi-Fi7)
        160 => 1960, // 160 MHz: data subcarriers (HE/Wi-Fi6)
        80 => 980,   // https://www.arubanetworks.com/assets/wp/WP_802.11AX.pdf, page 12
        40 => 468,
        20 => 234,
        _ => 0, // Default case,  fallback
    };

    let ORate: f64 = SU_spatial_streams * bits_symbol as f64 * coding_rate * Subcarriers as f64;
    let OBasicRate: f64 = 1.0 / 2.0 * 1.0 * 48.0; // 1 bit symbol * 1/2 CR 
    let app_payload_per_mpdu = total_bits_transmitted_app / n_mpdus as f64; 
    
    // 2. Network Stack Overhead: LLC/SNAP (8B) + IPv4 (20B) + UDP (8B) = 36 Bytes (288 bits)
    let L_avg = app_payload_per_mpdu + 288.0; // added protocol headers per-MPDU
    // let L: f64 = total_bits_transmitted / n_mpdus as f64; // TODO: Check if it's correct to have a size as f32 (in reality not, but as avg model? )

    let SF = 16.0;
    let TB = 18.0;
    let MD = 32.0;
    let MAC_H_size = 288.0;

    let T_RTS: f64 = LEGACY_PHY_DURATION + ((SF + 160.0 + TB) / OBasicRate).ceil() * SYMBOL_TIME_LEGACY; // legacy symbol time is 4E-6
    let T_CTS: f64 = LEGACY_PHY_DURATION + ((SF + 112.0 + TB) / OBasicRate).ceil() * SYMBOL_TIME_LEGACY;
    
    let mpdu_length_bits = L_avg + MAC_H_size; // Payload + MAC Header
    let padded_mpdu_size = (mpdu_length_bits / 32.0).ceil() * 32.0 ; // Round up to 32-bit boundary for padding
    
    let T_DATA: f64 = EHT_PHY_DURATION + ((SF + n_mpdus as f64 * (MD + padded_mpdu_size) + TB) / ORate).ceil() * SYMBOL_TIME_11AX + PE_DURATION; // 802.11ax symbol time 4 times greates for 16E-6 s
    
    let ba_base_bytes = 24.0; // Frame Control, Dur, RA, TA, BA Ctrl, Seq Ctrl, FCS
    let ba_bitmap_bytes = if n_mpdus <= 64 {
        8.0  // Standard Compressed (64 bits)
    } else {
        32.0 // HE Extended Compressed (256 bits)
    };

    let block_ack_bits = (ba_base_bytes + ba_bitmap_bytes) * 8.0; // 256 bits with 64-sized A-MPDUs
    
    let T_ACK: f64 = LEGACY_PHY_DURATION + ((SF + block_ack_bits + TB) / OBasicRate).ceil() * SYMBOL_TIME_LEGACY; 

    // let phy_time = T_RTS + SIFS + T_CTS + SIFS + T_DATA + SIFS + T_ACK; // ⬅  removed DIFS + SLOT + BO, it happens in EDCA now.
    let phy_time = T_DATA + SIFS + T_ACK; // ⬅  removed DIFS + SLOT + BO, it happens in EDCA now.

    // let rts_cts_overhead_time: f64 = T_RTS + SIFS + T_CTS + SIFS;                            // ONLY FOR DEBUG
    // let _rts_cts_overhead_percent = (rts_cts_overhead_time / phy_time) * 100.0;              // ONLY FOR DEBUG
    // print_dblue!("[AMPDU airtime = {:.3} ms] Bits: {} Channel Width: {:?} MHz, O_rate: {:.2}, eff_Pt={}, Pr: {:.3}\n\t\t| distance = {:.3} |  PathLoss = {:.3} | RTS/CTS Overhead: {:.1} % |"
    //              ,phy_time * 1000.0, total_bits_transmitted,  channel_width, ORate, effPt, Pr, distance, PL, rts_cts_overhead_percent,);
    (phy_time, _mcs_val as u8, T_DATA)
}



#[derive( Clone, Copy)]
pub struct Coords {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

fn main() {

    let n_mpdus = 64; 
    let total_bits_transmitted = 1500 *8 * n_mpdus; 

    let coords_src = Coords{x:0.0,y: 0.0,z: 0.0 }; 
    let coords_dest = Coords {x: 2.5, y: 0.0, z:0.0}; 

    let _p_tx_orig = 20; // dBm
    let channel_width = 80;  

    let (phy_time, mcs, T_data) = airtime_ampdu(total_bits_transmitted as f64 , n_mpdus, coords_src, coords_dest, _p_tx_orig as f64, channel_width); 


    println!("The airtime for {:.0} packets ({:.0} bits) is {:.5} s ({:.5} microseconds) at MCS{:.0}.\n Total PHY Time: {:.4} microseconds", n_mpdus, total_bits_transmitted, T_data, T_data * 1e6, mcs, phy_time * 1e6); 

}
