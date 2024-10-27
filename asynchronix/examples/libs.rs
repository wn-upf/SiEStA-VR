use std::collections::VecDeque;
use std::f64;

use csv::Writer;
use std::fs::OpenOptions;
use tai_time::TaiTime;

use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct CsvType {
    pub first_t: bool,
    pub v_timestamp: Vec<f64>,
    pub v_packet_id: Vec<usize>,
    pub v_queue_size: Vec<usize>,
    pub v_queue_ts: Vec<f64>,
    pub v_queue_tq: Vec<f64>,
    pub v_packet_l: Vec<usize>,
    // v_queue_ts_sliding_avg_mcs: Vec<f64>, // Uncomment if needed
}

const CW_MIN: i32 = 15;
const CHANNEL_WIDTH: usize = 80; //MHz

const LEGACY_PHY_DURATION: f64 = 20E-6; // microseconds
const PHY_DURATION: f64 = 100E-6;
const SLOT: f64 = 9E-6;
const SIFS: f64 = 16E-6;
const DIFS: f64 = 31E-6;

#[macro_export]
macro_rules! format_timestamp {
    ($elapsed:expr) => {{
        let total_seconds =
            $elapsed.as_secs() as f64 + ($elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
        format!("{:.9}", total_seconds)
    }};
}

impl CsvType {
    pub fn new() -> Self {
        Self {
            first_t: true,
            v_timestamp: Vec::new(),
            v_packet_id: Vec::new(),
            v_queue_size: Vec::new(),
            v_queue_ts: Vec::new(),
            v_queue_tq: Vec::new(),
            v_packet_l: Vec::new(),
            // v_queue_ts_sliding_avg_mcs: Vec::new(), // Uncomment if needed
        }
    }

    pub fn update_stats(
        &mut self,
        now: tai_time::TaiTime<0>,
        id_packet: usize,
        queue_size: usize,
        Ts: f64,
        Tq: f64,
        length_packet: usize,
    ) {
        // println!("[DBG STATS]");

        // Open or create the CSV file in append mode
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open("stats.csv")
            .expect("Failed to open or create CSV file");

        // Create a new CSV writer using the file
        let mut writer = Writer::from_writer(file);

        // Write the header if this is the first entry
        if self.first_t {
            writer
                .write_record(&[
                    "timestamp",
                    "packet_id",
                    "queue_size",
                    "Ts",
                    "Tq",
                    "packet_length",
                ])
                .expect("Failed to write header to CSV");
            self.first_t = false; // Set to false to avoid writing the header again
        }
        let formatted_timestamp = format_timestamp!(now);

        // Write data as a new row to the CSV
        writer
            .write_record(&[
                formatted_timestamp.to_string(),
                id_packet.to_string(),
                queue_size.to_string(),
                Ts.to_string(),
                Tq.to_string(),
                length_packet.to_string(),
            ])
            .expect("Failed to write record to CSV");

        // Ensure the writer flushes to file
        writer.flush().expect("Failed to flush CSV writer");
    }
}

#[derive(Clone)]
pub struct CumulativeStats {
    values: VecDeque<f64>,
    sum: f64,
    sum_of_squares: f64,
    sta_id: i32,
}

impl CumulativeStats {
    // Constructor
    pub fn new(sta_id: i32) -> Self {
        Self {
            values: VecDeque::new(),
            sum: 0.0,
            sum_of_squares: 0.0,
            sta_id,
        }
    }

    // Add a new value
    fn add(&mut self, value: f64) {
        self.values.push_back(value);
        self.sum += value;
        self.sum_of_squares += value * value;
    }

    // Get average
    fn get_average(&self) -> f64 {
        if self.values.is_empty() {
            0.0
        } else {
            self.sum / self.values.len() as f64
        }
    }

    // Get standard deviation
    fn get_std_dev(&self) -> f64 {
        if self.values.len() < 2 {
            return 0.0;
        }

        let mean = self.get_average();
        let sum_sq_diff: f64 = self
            .values
            .iter()
            .map(|&value| {
                let diff = value - mean;
                diff * diff
            })
            .sum();

        (sum_sq_diff / (self.values.len() as f64 - 1.0)).sqrt()
    }

    // Get coefficient of variation
    fn get_coefficient_variation(&self) -> f64 {
        let mean = self.get_average();
        if mean == 0.0 {
            0.0
        } else {
            self.get_std_dev() / mean
        }
    }

    // Get second moment
    fn get_2nd_moment(&self) -> f64 {
        if self.values.is_empty() {
            0.0
        } else {
            self.sum_of_squares / self.values.len() as f64
        }
    }

    // Get last value
    fn get_last_value(&self) -> Option<f64> {
        if let Some(&last_value) = self.values.back() {
            Some(last_value)
        } else {
            eprintln!("[ERROR!] No values have been added yet.");
            None
        }
    }
}

pub fn compute_steady_state_probabilities(rho: f64, k: i32) -> Vec<f64> {
    let mut probabilities = Vec::new();

    // Compute the normalization constant
    let p0 = 1.0 - rho; // Probability of 0 customers
    probabilities.push(p0);

    // Compute the probability for n customers (n from 1 to k)
    for n in 1..=k {
        let pn = p0 * rho.powi(n);
        probabilities.push(pn);
    }
    probabilities
}

#[derive(Debug)]
pub struct LittleTheoremMM1K {
    pub k: i32,      // Max capacity of system (u)
    pub lambda: f64, // Arrival rate
    pub mu: f64,     // Service rate
    pub rho: f64,    // Utilization factor
    pub n: f64,      // Average number of packets in the system
    pub n_q: f64,    // Average number of packets in queue
    pub p_0: f64,    // Probability of 0 packets in the system
    pub p_k: f64,    // Blocking probability
    pub t: f64,      // Average time in the system
    pub t_q: f64,    // avg. Waiting time in queue
    pub t_s: f64,    // avg. Service time
}

pub fn compute_mm1k_metrics(
    bandwidth_source: f64,
    l_packets: f64,
    bandwidth_departures: f64,
    k: usize,
) -> LittleTheoremMM1K {
    let lambda = bandwidth_source / l_packets as f64;
    let mu = bandwidth_departures / l_packets as f64;
    let rho = lambda / mu;
    let k_i: i32 = k as i32;

    if rho >= 1.0 {
        eprintln!("Unstable system: lambda must be less than mu.");
    }

    // Probability of 0 packets in the system
    let p_0 = (1.0 - rho) / (1.0 - rho.powi(k_i + 1));

    // Blocking probability
    let p_k = p_0 * rho.powi(k_i);

    // Average number of packets in the system
    let n = rho / (1.0 - rho) - ((k + 1) as f64 * rho.powi(k_i + 1)) / (1.0 - rho.powi(k_i + 1));
    let n_q = n - (1.0 - p_0);

    // Average time in the system
    let t = n / (lambda * (1.0 - p_k));
    let t_s = l_packets as f64 / bandwidth_departures;
    let t_q = t - t_s;

    LittleTheoremMM1K {
        k: k_i,
        lambda,
        mu,
        rho,
        n,
        n_q,
        p_0,
        p_k,
        t,
        t_q,
        t_s,
    }
}

// #[derive(Debug)] // unused (todo)
// pub struct MG1K {
//     pub rho: f64,
//     pub n_q: f64,
//     pub n: f64,
//     pub t: f64,
//     pub t_q: f64,
//     pub t_s: f64,
//     pub mean_t_s: f64,
//     pub std_t_s: f64,
//     pub cv_t_s: f64,
//     pub t_residual: f64,
// }

#[derive(Debug, Clone, Copy)]
pub struct MpduPacket {
    pub packet_id: usize,
    pub length_packet: usize,
    pub queue_in_instant: TaiTime<0>,
    pub queue_out_instant: TaiTime<0>,
    pub sink_in_instant: Instant,
    pub T_q: Duration,
    pub T_s: Duration,
    pub expected_T_s: Duration,

    pub sta_dest_id: i32,
    pub sta_coords: Coords,
}

impl MpduPacket {
    pub fn new() -> Self {
        Self {
            packet_id: 0,
            length_packet: 0,
            queue_in_instant: TaiTime::default(),
            queue_out_instant: TaiTime::default(),
            sink_in_instant: Instant::now(),
            T_q: Duration::ZERO,
            T_s: Duration::ZERO,
            expected_T_s: Duration::ZERO,
            sta_dest_id: 0,
            sta_coords: Coords::new(),
        }
    }

    pub fn print(&self) {
        println!("Packet ID: {}, L: {}", self.packet_id, self.length_packet);
    }
}
#[derive(Debug, Clone)]
pub struct AmpduPacket {
    pub mpdu_packets: Vec<MpduPacket>, // Container for MPDU packets
    pub total_length: usize,           // Total length of aggregated packets
    pub sta_id: i32,                   // ID for the source STA
    pub size: i32,
    pub coordinates: Coords,
}

impl AmpduPacket {
    pub fn new() -> Self {
        AmpduPacket {
            mpdu_packets: Vec::new(), // Initialize an empty vector for MPDU packets
            total_length: 0,          // Initialize total length to 0
            sta_id: -1, // Initialize STA_ID to -1 (assuming -1 indicates uninitialized)
            size: 0,    // Initialize size to 0
            coordinates: Coords {
                x: 0.0,
                y: 0.0,
                z: 0.0,
            }, // Initialize coordinates to (0.0, 0.0, 0.0)
        }
    }
    // Method to print AMPDU_packet values
    pub fn print(&self) {
        println!(
            "\x1b[33m[AMPDU INFO] \t\tSize: {}, STA_ID: {}, Total Length: {}\x1b[0m",
            self.size, self.sta_id, self.total_length
        );
        for packet in &self.mpdu_packets {
            println!(
                "\x1b[33m\t\t\t\t\t\t\t- Packet ID: {:.0}\x1b[0m",
                packet.packet_id
            );
        }
    }

    // Method to reinitialize all values
    pub fn reset(&mut self) {
        self.mpdu_packets.clear(); // Clear the vector of MPDU packets
        self.total_length = 0; // Reset total length
        self.size = 0; // Reset size
        self.sta_id = -1; // Reset STA_ID (assuming -1 is an uninitialized value)
        self.coordinates = Coords {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        }; // Reset coordinates to default (0.0, 0.0)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Coords {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Coords {
    pub fn new() -> Self {
        Self {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        }
    }
}

#[derive(Debug, Default)]
pub struct ResultsFrameTXDelay {
    pub service_delay: f64,
    pub data_service_delay: f64,
    pub pathloss: f64,
    pub p_rx: f64, // Rust doesn't have a separate `long double`, so f64 is used
    pub o_rate: f64,
}

impl ResultsFrameTXDelay {
    pub fn clear(&mut self) {
        self.service_delay = 0.0;
        self.data_service_delay = 0.0;
        self.pathloss = 0.0;
        self.p_rx = 0.0;
        self.o_rate = 0.0;
    }
}

pub fn calculate_distance(x: f64, y: f64, z: f64, x_: f64, y_: f64, z_: f64) -> f64 {
    let dx = x_ - x;
    let dy = y_ - y;
    let dz = z_ - z;

    (dx * dx + dy * dy + dz * dz).sqrt()
}

pub fn path_loss(d: f64) -> f64 {
    let gamma = 2.06067_f64;
    54.12 + 10.0 * gamma * (d).log10() + 5.25 * 0.1467 * d
}
pub fn frametransmission_delay(
    total_bits_transmitted: f64,
    n_mpdus: i32,
    coords_src: Coords,
    coords_dest: Coords,
    p_tx: f64,
) -> ResultsFrameTXDelay {
    let channel_width: usize = CHANNEL_WIDTH;

    // Effective Pt
    let mut effPt = p_tx;
    if channel_width > 20 {
        effPt = effPt - 3.0 * (channel_width as f64 / 20.0);
    }
    let distance = calculate_distance(
        coords_src.x,
        coords_src.y,
        coords_src.z,
        coords_dest.x,
        coords_dest.y,
        coords_dest.z,
    );

    let PL = path_loss(distance);
    let Pr = effPt - PL;

    let (bits_symbol, coding_rate) = match Pr {
        _ if Pr < -82.0 => (1, 1.0 / 2.0),
        _ if Pr >= -82.0 && Pr < -79.0 => (1, 1.0 / 2.0),
        _ if Pr >= -79.0 && Pr < -77.0 => (2, 1.0 / 2.0),
        _ if Pr >= -77.0 && Pr < -74.0 => (2, 3.0 / 4.0),
        _ if Pr >= -74.0 && Pr < -70.0 => (4, 1.0 / 2.0),
        _ if Pr >= -70.0 && Pr < -66.0 => (4, 3.0 / 4.0),
        _ if Pr >= -66.0 && Pr < -65.0 => (6, 1.0 / 2.0),
        _ if Pr >= -65.0 && Pr < -64.0 => (6, 2.0 / 3.0),
        _ if Pr >= -64.0 && Pr < -59.0 => (6, 3.0 / 4.0),
        _ if Pr >= -59.0 && Pr < -57.0 => (8, 3.0 / 4.0),
        _ if Pr >= -57.0 && Pr < -55.0 => (6, 5.0 / 6.0),
        _ if Pr >= -55.0 && Pr < -53.0 => (10, 3.0 / 4.0),
        _ if Pr >= -53.0 => (10, 5.0 / 6.0),
        _ => (1, 1.0 / 2.0), // Catch-all for Pr out of range
    };

    let Subcarriers = match channel_width {
        // https://www.arubanetworks.com/assets/wp/WP_802.11AX.pdf, page 12
        80 => 980,
        40 => 468,
        20 => 234,
        _ => 0, // Default case,  fallback
    };

    let SU_spatial_streams = 2.0;
    let ORate: f64 = SU_spatial_streams * bits_symbol as f64 * coding_rate * Subcarriers as f64;

    let OBasicRate: f64 = 1.0 / 2.0 * 1.0 * 48.0;

    let L: f64 = total_bits_transmitted / n_mpdus as f64;

    let SF = 16.0;
    let TB = 18.0;
    let MD = 32.0;
    let MAC_H_size = 240.0;

    let T_RTS: f64 = LEGACY_PHY_DURATION + ((SF + 160.0 + TB) / OBasicRate).ceil() * 4E-6; // legacy symbol time is 4E-6
    let T_CTS: f64 = LEGACY_PHY_DURATION + ((SF + 112.0 + TB) / OBasicRate).ceil() * 4E-6;
    let T_DATA: f64 =
        PHY_DURATION + ((SF + n_mpdus as f64 * (MD + MAC_H_size + L) + TB) / ORate).ceil() * 16E-6;
    let T_ACK: f64 = LEGACY_PHY_DURATION + ((SF + 240.0 + TB) / OBasicRate).ceil() * 4E-6;

    let T_DETERMINISTIC_BACKOFF = (CW_MIN as f64 - 1.0) / 2.0 * SLOT; // add small time constant between consecutive TX to model backoff

    let T =
        T_RTS + SIFS + T_CTS + SIFS + T_DATA + SIFS + T_ACK + DIFS + SLOT + T_DETERMINISTIC_BACKOFF;

    // printf("DBG T = %.6f, T_DATA = %.6f, T_backoff = %.6f\n", T, T_DATA,  T_DETERMINISTIC_BACKOFF);

    // printf("[DBG] T = %.6f, T_DATA = %.6f\n", T, T_DATA);
    ResultsFrameTXDelay {
        pathloss: PL,
        p_rx: Pr,
        o_rate: ORate,
        service_delay: T,
        data_service_delay: T_DATA,
    }
}
