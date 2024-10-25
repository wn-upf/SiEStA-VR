use std::f64;




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
    k: usize
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