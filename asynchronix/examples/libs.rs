use std::f64;
use std::collections::VecDeque; 


use std::fs::OpenOptions;
use std::io::Write;
use csv::Writer;


#[derive(Clone)]
pub struct CsvType {
    pub v_timestamp: Vec<f64>,
    pub v_packet_id: Vec<usize>,
    pub v_queue_size: Vec<usize>,
    pub v_queue_ts: Vec<f64>,
    pub v_queue_tq: Vec<f64>,
    pub v_packet_l: Vec<usize>,
    // v_queue_ts_sliding_avg_mcs: Vec<f64>, // Uncomment if needed
}

impl CsvType {
    pub fn new() -> Self {
        Self {
            v_timestamp: Vec::new(),
            v_packet_id: Vec::new(),
            v_queue_size: Vec::new(),
            v_queue_ts: Vec::new(),
            v_queue_tq: Vec::new(),
            v_packet_l: Vec::new(),

            // v_queue_ts_sliding_avg_mcs: Vec::new(), // Uncomment if needed
        }
    }
    // pub fn update_stats(&mut self, now: f64, ID_packet: usize, queue_size: usize, Ts: f64, 
    //                     Tq: f64, length_packet: usize )
    //     {
    //         println!("[DBG STATS]"); 
    //         self.v_timestamp.push(now); 
    //         self.v_packet_id.push(ID_packet); 
    //         self.v_queue_size.push(queue_size);
    //         self.v_queue_ts.push(Ts); 
    //         self.v_queue_tq.push(Tq); 
    //         self.v_packet_l.push(length_packet); 
    // }
    pub fn update_stats(&self, now: tai_time::TaiTime<0>, id_packet: usize, queue_size: usize, Ts: f64, 
        Tq: f64, length_packet: usize) 
        {
            println!("[DBG STATS]");

            // Open or create the CSV file in append mode
            let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open("stats.csv")
            .expect("Failed to open or create CSV file");

            // Create a new CSV writer using the file
            let mut writer = Writer::from_writer(file);

            // Write data as a new row to the CSV
            writer.write_record(&[
            now.to_string(),
            id_packet.to_string(),
            queue_size.to_string(),
            Ts.to_string(),
            Tq.to_string(),
            length_packet.to_string(),
            ]).expect("Failed to write record to CSV");

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
        let sum_sq_diff: f64 = self.values
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
    let p0 = 1.0 - rho;  // Probability of 0 customers
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
    pub k: i32,            // Max capacity of system (u) 
    pub lambda: f64,       // Arrival rate
    pub mu: f64,           // Service rate
    pub rho: f64,          // Utilization factor
    pub n: f64,            // Average number of packets in the system
    pub n_q: f64,          // Average number of packets in queue
    pub p_0: f64,          // Probability of 0 packets in the system
    pub p_k: f64,          // Blocking probability
    pub t: f64,            // Average time in the system
    pub t_q: f64,          // avg. Waiting time in queue
    pub t_s: f64,          // avg. Service time
}

#[derive(Debug, Default)]
pub struct ResultsFrameTXDelay {
    pub service_delay: f64,
    pub data_service_delay: f64,
    pub pathloss: f64,
    pub p_rx: f64,
    pub o_rate: f64,
}

impl ResultsFrameTXDelay {
    pub fn clear(&mut self) {
        *self = ResultsFrameTXDelay::default();
    }
}

pub fn compute_mm1k_metrics(
    bandwidth_source: f64,
    l_packets: f64,
    bandwidth_departures: f64,
    k: usize ) -> LittleTheoremMM1K {
        
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

#[derive(Debug, Default)]
pub struct Coords {
    pub x: f64,
    pub y: f64,
    pub z: f64,
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