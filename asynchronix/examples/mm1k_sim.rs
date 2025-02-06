// !```text
// !                     ┌────────────────────────────────────────────────────┐
// !                     │                                                    │
// !                     │                   Packet Flow                      │
// !                     │   ┌──────────────┐                ┌──────────────┐ │
// !    MpduPacket   ●──►│──►│ PoissonSource├───────────────►│ QueueModule  ├──► AmpduPacket
// !                     │   │              │    output_port │              │ │    output_port
// !                     │   └──────────────┘                └──────────────┘ │
// !                     │                                                    │
// !                     └────────────────────────────────────────────────────┘
// !```
#![allow(non_snake_case)]
#![allow(unused)]
use crate::lib::models_XR::STA_extended;
use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;
use lib::models_mm1k::STA_source;
use std::collections::HashMap;
use std::fs;
use std::hash::Hash;

use std::time::Duration;

use std::sync::{Arc, Mutex};

// mod lib; // for calling m own local library
mod lib;
use crate::lib::{
    compute_mm1k_metrics, exponential, frametransmission_delay, perStaLockStats,
    write_all_sta_csvs, Coords, CsvData, DebugColor, MAX_AMPDU_SIZE, P_TX,
};

use crate::lib::models_mm1k::{QueueModule, QueueStats, Sink};
use asynchronix::model::Model;
use std::env;

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////// SIMULATION ////////////////////////////////////////////////////////////////////////////////////

// fn simple_MM1K(
//     // 1st scenario: Single STA -> Queue -> Sink
//     stoptime: f64,
//     mean_length: f64,
//     k_queue: usize,
//     rate_bps_in: f64,
//     rate_queue_bps: f64,
//     distance: f64,
// ) {
//     let num_STAs = 1;
//     let coords_sta = Coords {
//         x: distance,
//         y: 0.0,
//         z: 0.0,
//     };
//     let results = frametransmission_delay(
//         mean_length as f64,
//         MAX_AMPDU_SIZE,
//         Coords::new(),
//         coords_sta,
//         P_TX,
//     );

//     let effective_rate = mean_length / results.service_delay;

//     let LT = compute_mm1k_metrics(rate_bps_in, mean_length, effective_rate, k_queue);

//     let rate_service_bps: f64 = 20E3;

//     let mut source: STA_source = STA_source::new(
//         rate_bps_in,
//         mean_length,
//         0,
//         2,
//         coords_sta,
//         true,
//         rate_service_bps,
//     ); // STAs 0

//     let mut vec_ids_stas = Vec::new();
//     vec_ids_stas.push(source.sta_id);

//     let mut queue: QueueModule = QueueModule::new(
//         num_STAs,
//         k_queue - 1 as usize,
//         0.00,
//         vec_ids_stas,

//     );
//     let sink = Sink::new();

//     // mutex data handles to be able to access simulator variables, as csv vecs or CumulativeStats
//     let csv_data_handle: Arc<Mutex<CsvData>> = queue.csv_metrics.get_data_handle();
//     let queuestats_data_handle = queue.get_queue_stats_handle();
//     let stats_sta_data_handle: Arc<Mutex<HashMap<usize, perStaLockStats>>> =
//         queue.get_stas_stats_handle();

//     let mbox_src = Mailbox::new();
//     let mbox_src_address = mbox_src.address();

//     let mbox_queue = Mailbox::new();
//     let queue_address = mbox_queue.address();

//     let sink_mbox = Mailbox::new();
//     let sink_mbox_address = sink_mbox.address();

//     // CONNECT COMPONENTS
//     // source.output_port.connect(Sink::input, &sink_mbox);

//     source.output_port.connect(QueueModule::input, &mbox_queue);
//     queue.output_port_sta1.connect(Sink::input, &sink_mbox);
//     queue.output_port_sta2.connect(Sink::input, &sink_mbox);

//     let t0 = MonotonicTime::EPOCH;

//     let mut simu = SimInit::new()
//         .add_model(source, mbox_src, "STA BG")
//         .add_model(queue, mbox_queue, "Queue")
//         .add_model(sink, sink_mbox, "Sink")
//         .init(t0);

//     let scheduler = simu.scheduler();

//     // ----------
//     // Simulation.
//     // ----------

//     // Check initial conditions.

//     let t = t0;

//     assert_eq!(simu.time(), t);

//     // START WITH FIRST EVENT
//     scheduler
//         .schedule_event(
//             Duration::from_millis(1),
//             STA_source::send_packet,
//             (),
//             &mbox_src_address,
//         )
//         .unwrap();

//     simu.step_by(Duration::from_secs_f64(stoptime)); //works

//     // Create the directory if it doesn't exist
//     let dir = "Results/";
//     if !fs::metadata(dir).is_ok() {
//         fs::create_dir_all(dir).expect("Failed to create Results directory");
//     }

//     let mbps = rate_bps_in / 1e6;
//     let filename = format!("{:.1}Mbps", mbps);

//     // After simulation, write the CSV data
//     if let Ok(data) = csv_data_handle.lock() {
//         if let Err(e) = data.write_to_csv(&filename) {
//             eprintln!("Failed to write CSV file: {}", e);
//         }
//     }

//     if let Ok(data) = stats_sta_data_handle.lock() {
//         if let Err(e) = write_all_sta_csvs(&data, &filename, &"Results") {
//             eprintln!("Error writing STA CSV files: {}", e);
//         }
//     }

//     // println!("************ END RESULTS ***********\n LT: {:#?}", LT);
//     LT.print_results();

//     if let Ok(queue_stats) = queuestats_data_handle.lock() {
//         println!(
//             "Waiting time mean: {}",
//             queue_stats.waiting_time_cum.get_average()
//         );
//         println!(
//             "Waiting time std dev: {}",
//             queue_stats.waiting_time_cum.get_std_dev()
//         );
//         println!(
//             "Service time mean: {}",
//             queue_stats.service_time_cum.get_average()
//         );
//         println!(
//             "Service time std dev: {}",
//             queue_stats.service_time_cum.get_std_dev()
//         );
//     } else {
//         eprintln!("Failed to lock queue stats");
//     };
// }

// fn simple_MM1K(
//     // 1st scenario: Single STA -> Queue -> Sink
//     stoptime: f64,
//     mean_length: f64,
//     k_queue: usize,
//     rate_bps_in: f64,
//     rate_queue_bps: f64,
//     distance: f64,
// ) {
//     let num_STAs = 1;
//     let coords_sta = Coords {
//         x: distance,
//         y: 0.0,
//         z: 0.0,
//     };
//     let results = frametransmission_delay(
//         mean_length as f64,
//         MAX_AMPDU_SIZE,
//         Coords::new(),
//         coords_sta,
//         P_TX,
//     );

//     let effective_rate = mean_length / results.service_delay;

//     let LT = compute_mm1k_metrics(rate_bps_in, mean_length, effective_rate, k_queue);

//     let rate_service_bps: f64 = 20E3;

//     let mut source: STA_source = STA_source::new(
//         rate_bps_in,
//         mean_length,
//         0,
//         2,
//         coords_sta,
//         true,
//         rate_service_bps,
//     ); // STAs 0

//     let mut vec_ids_stas = Vec::new();
//     vec_ids_stas.push(source.sta_id);

//     let mut queue: QueueModule = QueueModule::new(
//         num_STAs,
//         k_queue - 1 as usize,
//         0.00,
//         vec_ids_stas,

//     );
//     let sink = Sink::new();

//     // mutex data handles to be able to access simulator variables, as csv vecs or CumulativeStats
//     let csv_data_handle: Arc<Mutex<CsvData>> = queue.csv_metrics.get_data_handle();
//     let queuestats_data_handle = queue.get_queue_stats_handle();
//     let stats_sta_data_handle: Arc<Mutex<HashMap<usize, perStaLockStats>>> =
//         queue.get_stas_stats_handle();

//     let mbox_src = Mailbox::new();
//     let mbox_src_address = mbox_src.address();

//     let mbox_queue = Mailbox::new();
//     let queue_address = mbox_queue.address();

//     let sink_mbox = Mailbox::new();
//     let sink_mbox_address = sink_mbox.address();

//     // CONNECT COMPONENTS
//     // source.output_port.connect(Sink::input, &sink_mbox);

//     source.output_port.connect(QueueModule::input, &mbox_queue);
//     queue.output_port_sta1.connect(Sink::input, &sink_mbox);
//     queue.output_port_sta2.connect(Sink::input, &sink_mbox);

//     let t0 = MonotonicTime::EPOCH;

//     let mut simu = SimInit::new()
//         .add_model(source, mbox_src, "STA BG")
//         .add_model(queue, mbox_queue, "Queue")
//         .add_model(sink, sink_mbox, "Sink")
//         .init(t0);

//     let scheduler = simu.scheduler();

//     // ----------
//     // Simulation.
//     // ----------

//     // Check initial conditions.

//     let t = t0;

//     assert_eq!(simu.time(), t);

//     // START WITH FIRST EVENT
//     scheduler
//         .schedule_event(
//             Duration::from_millis(1),
//             STA_source::send_packet,
//             (),
//             &mbox_src_address,
//         )
//         .unwrap();

//     simu.step_by(Duration::from_secs_f64(stoptime)); //works

//     // Create the directory if it doesn't exist
//     let dir = "Results/";
//     if !fs::metadata(dir).is_ok() {
//         fs::create_dir_all(dir).expect("Failed to create Results directory");
//     }

//     let mbps = rate_bps_in / 1e6;
//     let filename = format!("{:.1}Mbps", mbps);

//     // After simulation, write the CSV data
//     if let Ok(data) = csv_data_handle.lock() {
//         if let Err(e) = data.write_to_csv(&filename) {
//             eprintln!("Failed to write CSV file: {}", e);
//         }
//     }

//     if let Ok(data) = stats_sta_data_handle.lock() {
//         if let Err(e) = write_all_sta_csvs(&data, &filename, &"Results") {
//             eprintln!("Error writing STA CSV files: {}", e);
//         }
//     }

//     // println!("************ END RESULTS ***********\n LT: {:#?}", LT);
//     LT.print_results();

//     if let Ok(queue_stats) = queuestats_data_handle.lock() {
//         println!(
//             "Waiting time mean: {}",
//             queue_stats.waiting_time_cum.get_average()
//         );
//         println!(
//             "Waiting time std dev: {}",
//             queue_stats.waiting_time_cum.get_std_dev()
//         );
//         println!(
//             "Service time mean: {}",
//             queue_stats.service_time_cum.get_average()
//         );
//         println!(
//             "Service time std dev: {}",
//             queue_stats.service_time_cum.get_std_dev()
//         );
//     } else {
//         eprintln!("Failed to lock queue stats");
//     };
// }

// // // SCENARIO 2: TWO STAS AS BG TRAFFIC, 1 STA AS SINK
// fn multiple_STA_sim(
//     num_STAs: usize,
//     stoptime: f64,
//     mean_length: f64,
//     k_queue: usize,
//     rate_bps_in: f64,
//     rate_queue_bps: f64,
//     distance: f64,
// ) {
//     let v_distance = vec![1.0, distance, distance]; // just some random values

//     const NUM_STAS_UL: usize = 1; //for now

//     let coords_sta1 = Coords {
//         x: v_distance[0],
//         y: 0.0,
//         z: 0.0,
//     };
//     let coords_sta2 = Coords {
//         x: v_distance[1],
//         y: 0.0,
//         z: 0.0,
//     };
//     let coords_sta3 = Coords {
//         x: v_distance[2],
//         y: 0.0,
//         z: 0.0,
//     };

//     let vec_coords = vec![coords_sta1, coords_sta2, coords_sta3];
//     println!("vec_coords: {:?}\n", vec_coords);

//     let results1 = frametransmission_delay(
//         mean_length * MAX_AMPDU_SIZE as f64, // optimistic assumption of max throughput
//         MAX_AMPDU_SIZE,
//         Coords::new(),
//         coords_sta1,
//         P_TX,
//     );

//     let results2 = frametransmission_delay(
//         mean_length * MAX_AMPDU_SIZE as f64,
//         MAX_AMPDU_SIZE,
//         Coords::new(),
//         coords_sta2,
//         P_TX,
//     );

//     let effective_rate1 = mean_length / results1.service_delay;
//     let effective_rate2 = mean_length / results2.service_delay;
//     let effective_rate = (effective_rate1 + effective_rate2) / 2.0;

//     let aggregated_rate_in = (num_STAs - NUM_STAS_UL) as f64 * rate_bps_in;

//     println!("*******************************************************************");
//     println!(
//         "Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}",
//         aggregated_rate_in, mean_length, effective_rate, k_queue
//     );

//     let LT = compute_mm1k_metrics(
//         aggregated_rate_in,
//         mean_length as f64,
//         effective_rate,
//         k_queue,
//     );

//     let mut sta1_bg: STA_source = STA_source::new(
//         rate_bps_in,
//         mean_length,
//         0,
//         2,
//         coords_sta1,
//         true,
//         effective_rate1,
//     ); // STAs 0 and 1 send traffic to 5 through AP
//     let mut sta2_bg: STA_source = STA_source::new(
//         rate_bps_in,
//         mean_length,
//         1,
//         2,
//         coords_sta2,
//         true,
//         effective_rate2,
//     );

//     println!("STA1 PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
//         results1.pathloss, results1.p_rx, results1.service_delay * 1000.0, results1.data_service_delay * 1000.0, (1.0 / results1.service_delay) * mean_length);

//     println!("STA2 PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
//         results2.pathloss, results2.p_rx, results2.service_delay * 1000.0, results2.data_service_delay * 1000.0, (1.0 / results2.service_delay) * mean_length);

//     // let sta5_ul: STA_source = STA_source::new(rate_bps_in, mean_length, 2, 7, coords_sta3, false); // RX STA, acts as sink with coordinates
//     let sink: Sink = Sink::new();
//     let mbox_sink: Mailbox<Sink> = Mailbox::new();

//     let mut vec_ids_stas = Vec::new();
//     vec_ids_stas.push(sta1_bg.sta_id);
//     vec_ids_stas.push(sta2_bg.sta_id);

//     let mut queue: QueueModule = QueueModule::new(
//         vec_ids_stas.len(),
//         k_queue - 1 as usize,
//         0.0,
//         vec_ids_stas,
//     );

//     // mutex data handles to be able to access simulator variables, as csv vecs or CumulativeStats

//     queue.STA_coords_grid.resize(num_STAs, Coords::new());
//     for i in 0..num_STAs {
//         queue.STA_coords_grid[i] = vec_coords[i];
//     }
//     let csv_data_handle = queue.csv_metrics.get_data_handle();
//     let queuestats_data_handle: Arc<Mutex<QueueStats>> = queue.get_queue_stats_handle();
//     let stats_sta_data_handle: Arc<Mutex<HashMap<usize, perStaLockStats>>> =
//         queue.get_stas_stats_handle();
//     let sinkstats_data_handle = sink.get_data_handle();

//     let mbox_sta1 = Mailbox::new();
//     let mbox_sta2 = Mailbox::new();

//     let sta1_address = mbox_sta1.address();
//     let sta2_address = mbox_sta2.address();
//     // let sta3_address = mbox_sink.address();

//     let mbox_queue = Mailbox::new();
//     // let queue_address = mbox_queue.address();

//     // let sink_mbox = Mailbox::new();
//     // let sink_mbox_address = sink_mbox.address();

//     // CONNECT COMPONENTS
//     // source.output_port.connect(Sink::input, &sink_mbox);

//     sta1_bg.output_port.connect(QueueModule::input, &mbox_queue); // Two DL STAs send
//     sta2_bg.output_port.connect(QueueModule::input, &mbox_queue);

//     queue.output_port_sta1.connect(Sink::input, &mbox_sink);
//     // queue.output_port_sta2.connect(Sink::input, &mbox_sink);
//     //
//     let t0 = MonotonicTime::EPOCH;

//     let mut simu: asynchronix::simulation::Simulation = SimInit::with_num_threads(64)
//         .add_model(sta1_bg, mbox_sta1, "STA1 (BG)")
//         .add_model(sta2_bg, mbox_sta2, "STA2 (BG)")
//         .add_model(queue, mbox_queue, "Queue")
//         .add_model(sink, mbox_sink, "SINK")
//         .init(t0);

//     let scheduler = simu.scheduler();
//     // ----------
//     // Simulation.
//     // ----------
//     // Check initial conditions.

//     let t = t0;

//     assert_eq!(simu.time(), t);

//     // START WITH FIRST EVENT
//     let epsilon1 = Duration::from_secs_f64(exponential(0.9));
//     let epsilon2 = Duration::from_secs_f64(exponential(0.9));

//     let duration_scheduled1 = Duration::from_secs(10) + epsilon1;
//     let duration_scheduled2 = Duration::from_secs(10) + epsilon2;

//     scheduler
//         .schedule_event(
//             duration_scheduled1,
//             STA_source::send_packet,
//             (),
//             &sta1_address,
//         )
//         .unwrap();

//     scheduler
//         .schedule_event(
//             duration_scheduled2,
//             STA_source::send_packet,
//             (),
//             &sta2_address,
//         )
//         .unwrap();

//     simu.step_by(Duration::from_secs_f64(stoptime)); //works

//     // After simulation, write the CSV data
//     let mbps = rate_bps_in / 1e6;
//     let filename = format!("{:.1}Mbps", mbps);

//     // Ensure the directory exists
//     let dir_path = format!("Results/{}", filename);
//     // Create the directory if it doesn't exist
//     if let Err(e) = fs::create_dir_all(&dir_path) {
//         eprintln!("Failed to create directory: {}", e);
//         return; // Stop execution if the directory creation fails
//     }

//     if let Ok(data) = csv_data_handle.lock() {
//         if let Err(e) = data.write_to_csv(&filename) {
//             eprintln!("Failed to write CSV file: {}", e);
//         }
//     }
//     if let Ok(stats_vec) = stats_sta_data_handle.lock() {
//         // Now stats_vec is a MutexGuard<Vec<perStaLockStats>>
//         for (id, sta_stats) in stats_vec.iter() {
//             if let Ok(sta_data) = sta_stats.data.lock() {
//                 sta_data.print_nicely();
//             }
//         }

//         if let Err(e) = write_all_sta_csvs(&stats_vec, &filename, &"Results/") {
//             eprintln!("Error writing STA CSV files: {}", e);
//         }
//     }

//     if let Ok(queue_stats) = queuestats_data_handle.lock() {
//         // println!("[DEBUGDEBUGDEBU]!!!! T_s : {}, T_q : {} !", T_s_f64, T_q_f64);
//         queue_stats.print_nicely();
//     }

//     if let Ok(sink_stats) = sinkstats_data_handle.lock() {
//         sink_stats.print_nicely();
//     }

//     // println!("************ END RESULTS STAS***********\n LT: ");

//     println!("*******************************************************************");
//     println!(
//         "Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}",
//         aggregated_rate_in, mean_length, effective_rate, k_queue
//     );

//     LT.print_results();
// }

// SCENARIO 3: selectable downlink,uplink or both ways traffic using extended_sta
fn downlink_uplink_scenario_flexible(
    num_STAs: usize,
    stoptime: f64,
    mean_length: f64,
    k_queue: usize,
    rate_bps_in: f64,
    distance: f64,
    is_uplink: bool,
    is_downlink: bool,
    BG_rate: f64,
    distance_sta0: f64,
) {
    const UPLINK_CONSTANT: usize = 20;

    // Generate coordinates for STAs
    let mut vec_coords = Vec::with_capacity(num_STAs);
    let mut id_src_coords = Vec::with_capacity(num_STAs);
    let mut map_coords: HashMap<usize, Coords> = HashMap::new();
    println!("\n\n----------------------\nnum_STAs: {}", num_STAs);
    for i in 0..num_STAs {
        let coords = Coords {
            x: if i == 0 {
                distance_sta0
            } else {
                distance as f64
            },
            y: 0.0,
            z: 0.0,
        };
        println!("i = {}, coords: {:?}", i, coords);
        vec_coords.push(coords);

        id_src_coords.push(
            // if i == 0 { 0 } else { 5 }
            if is_downlink {
                UPLINK_CONSTANT
            } else if is_uplink {
                i
            } else {
                999
            },
        );
        map_coords.insert(id_src_coords[i], coords);
    }

    // Compute frame transmission delays and effective rates
    let mut results_vec = Vec::with_capacity(num_STAs);
    let mut effective_rates = Vec::with_capacity(num_STAs);

    for coords in &vec_coords {
        let results = frametransmission_delay(
            mean_length * MAX_AMPDU_SIZE as f64,
            MAX_AMPDU_SIZE,
            Coords::new(),
            *coords,
            P_TX,
        );
        results_vec.push(results.clone());
        effective_rates.push(mean_length / results.service_delay);
    }

    let aggregated_rate_in = num_STAs as f64 * rate_bps_in;
    let effective_rate = effective_rates.iter().sum::<f64>() / effective_rates.len() as f64;

    let LT = compute_mm1k_metrics(
        aggregated_rate_in,
        mean_length as f64,
        effective_rate,
        k_queue,
    );

    let t0 = MonotonicTime::EPOCH;

    // Prepare STAs based on uplink/downlink configuration
    let mut sta_bg_models: Vec<STA_extended> = Vec::with_capacity(num_STAs);
    let mut mbox_stas: Vec<Mailbox<STA_extended>> = Vec::with_capacity(num_STAs);
    let mut sta_addresses = Vec::with_capacity(num_STAs);

    for (i, coords) in vec_coords.iter().enumerate() {
        // Determine if this STA should be used based on uplink/downlink
        // let is_valid_uplink = is_uplink && i < num_STAs / 2;
        // let is_valid_downlink = is_downlink && i >= num_STAs / 2;
        let is_valid_uplink = is_uplink;
        let is_valid_downlink = is_downlink;
        println!(
            "STA {} is valid uplink: {}, is valid downlink: {}",
            i, is_valid_uplink, is_valid_downlink
        );

        if is_valid_uplink || is_valid_downlink {
            let sta = STA_extended::new(
                rate_bps_in,
                mean_length,
                i as i32,
                2,
                *coords,
                true,
                effective_rates[i],
                t0,
                true,
                BG_rate,
            );

            let mbox_sta = Mailbox::new();
            sta_addresses.push(mbox_sta.address());
            mbox_stas.push(mbox_sta);
            sta_bg_models.push(sta);
        }
    }

    // Prepare queue
    let vec_ids_stas: Vec<i32> = sta_bg_models.iter().map(|sta| sta.sta_id).collect();
    let num_stas_mod = vec_ids_stas.len();

    let mut queue: QueueModule = QueueModule::new(num_stas_mod, k_queue - 1, 0.0, vec_ids_stas);

    // Setup coordinates
    queue.STA_coords_grid.resize(num_stas_mod, Coords::new());
    for i in 0..num_stas_mod {
        queue.STA_coords_grid[i] = vec_coords[i];
    }
    queue.STA_coords_map = map_coords.clone();

    let csv_data_handle = queue.csv_metrics.get_data_handle();
    let queuestats_data_handle: Arc<Mutex<QueueStats>> = queue.get_queue_stats_handle();
    let stats_sta_data_handle: Arc<Mutex<HashMap<usize, perStaLockStats>>> =
        queue.get_stas_stats_handle();

    // Prepare other components
    let sink = Sink::new();
    let mbox_sink: Mailbox<Sink> = Mailbox::new();
    let mbox_queue = Mailbox::new();
    let sinkstats_data_handle = sink.get_data_handle();

    // With this:
    for sta in &mut sta_bg_models {
        sta.output_network_port
            .connect(QueueModule::input, &mbox_queue);
    }
    queue.output_port_sta1.connect(Sink::input, &mbox_sink);

    // Initialize simulation dynamically
    let mut simu_builder = SimInit::with_num_threads(64);

    // Add all STAs dynamically
    for (i, (sta, mbox)) in sta_bg_models.into_iter().zip(mbox_stas).enumerate() {
        simu_builder = simu_builder.add_model(sta, mbox, &format!("STA{} (BG)", i));
    }

    let mut simu = simu_builder
        .add_model(queue, mbox_queue, "Queue")
        .add_model(sink, mbox_sink, "SINK")
        .init(t0);

    let scheduler = simu.scheduler();

    // Schedule first events with random delays
    for address in sta_addresses {
        let epsilon = Duration::from_secs_f64(exponential(0.1));
        let duration_scheduled = epsilon;

        scheduler
            .schedule_event(
                duration_scheduled,
                STA_extended::send_packet_BG,
                (),
                &address,
            )
            .unwrap();
    }

    // Run simulation
    simu.step_by(Duration::from_secs_f64(stoptime));

    // Data handling and output (similar to previous implementation)
    let mbps = BG_rate / 1e6;
    let filename = format!("{:.1}Mbps", mbps);

    let mut dir_path = String::new();
    if is_uplink {
        dir_path = format!("Results_NBG{:.0}_UL/{:.1}Mbps/", num_STAs, mbps);
    } else if is_downlink {
        dir_path = format!("Results_NBG{:.0}_DL/{:.1}Mbps/", num_STAs, mbps);
    }

    // fs::create_dir_all(&dir_path).expect("Failed to create Results directory");
    if let Err(e) = fs::create_dir_all(&dir_path) {
        eprintln!("Failed to create directory: {}", e);
        return;
    }

    if let Ok(data) = csv_data_handle.lock() {
        if let Err(e) = data.write_to_csv(&filename, &dir_path) {
            eprintln!("Failed to write CSV file: {}", e);
        }
    }

    if let Ok(data) = csv_data_handle.lock() {
        if let Err(e) = data.write_to_csv(&filename, &dir_path) {
            eprintln!("Failed to write CSV file: {}", e);
        }
    }
    if let Ok(stats_vec) = stats_sta_data_handle.lock() {
        // Now stats_vec is a MutexGuard<Vec<perStaLockStats>>
        for (id, sta_stats) in stats_vec.iter() {
            if let Ok(sta_data) = sta_stats.data.lock() {
                sta_data.print_nicely();
            }
        }

        if let Err(e) = write_all_sta_csvs(&stats_vec, &filename, &dir_path) {
            eprintln!("Error writing STA CSV files: {}", e);
        }
    }

    if let Ok(queue_stats) = queuestats_data_handle.lock() {
        // println!("[DEBUGDEBUGDEBU]!!!! T_s : {}, T_q : {} !", T_s_f64, T_q_f64);
        queue_stats.print_nicely();
    }

    if let Ok(sink_stats) = sinkstats_data_handle.lock() {
        sink_stats.print_nicely();
    }
    LT.print_results();
}

// fn downlink_uplink_scenario(
//     num_STAs: usize,
//     stoptime: f64,
//     mean_length: f64,
//     k_queue: usize,
//     rate_bps_in: f64,
//     distance: f64,
//     is_uplink: bool,
//     is_downlink: bool,
//     BG_rate_bps: f64,
// ) {
//     let v_distance = vec![1.0, distance, distance]; // just some random values

//     let coords_sta1 = Coords {
//         x: v_distance[0],
//         y: 0.0,
//         z: 0.0,
//     };
//     let coords_sta2 = Coords {
//         x: v_distance[1],
//         y: 0.0,
//         z: 0.0,
//     };
//     let coords_sta3 = coords_sta1;
//     let coords_sta4 = coords_sta2;

//     let vec_coords = vec![coords_sta1, coords_sta2, coords_sta3, coords_sta4];
//     let id_src_coords = vec![0, 1, 5, 5];

//     let mut map_coords: HashMap<usize, Coords> = HashMap::new();
//     assert!(vec_coords.len() == id_src_coords.len());

//     let mut ccounter = 0;
//     for id in id_src_coords {
//         map_coords.insert(id, vec_coords[ccounter]);
//         ccounter += 1;
//     }

//     println!("vec_coords: {:?}\n", vec_coords);

//     let results1 = frametransmission_delay(
//         mean_length * MAX_AMPDU_SIZE as f64, // optimistic assumption of max throughput
//         MAX_AMPDU_SIZE,
//         Coords::new(),
//         coords_sta1,
//         P_TX,
//     );

//     let results2 = frametransmission_delay(
//         mean_length * MAX_AMPDU_SIZE as f64,
//         MAX_AMPDU_SIZE,
//         Coords::new(),
//         coords_sta2,
//         P_TX,
//     );

//     let effective_rate1 = mean_length / results1.service_delay;
//     let effective_rate2 = mean_length / results2.service_delay;
//     let effective_rate = (effective_rate1 + effective_rate2) / 2.0;

//     let aggregated_rate_in = (num_STAs) as f64 * rate_bps_in;

//     println!("*******************************************************************");
//     println!(
//         "Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}",
//         aggregated_rate_in, mean_length, effective_rate, k_queue
//     );

//     let LT = compute_mm1k_metrics(
//         aggregated_rate_in,
//         mean_length as f64,
//         effective_rate,
//         k_queue,
//     );

//     let t0 = MonotonicTime::EPOCH;
//     let coords_ap = Coords::new();

//     let mut sta1_bg: STA_extended = STA_extended::new(
//         rate_bps_in,
//         mean_length,
//         0,
//         2,
//         coords_sta1,
//         true,
//         effective_rate1,
//         t0,
//         true,
//         BG_rate_bps,
//     );
//     let mut sta2_bg: STA_extended = STA_extended::new(
//         rate_bps_in,
//         mean_length,
//         1,
//         2,
//         coords_sta2,
//         true,
//         effective_rate2,
//         t0,
//         true,
//         BG_rate_bps,
//     );

//     // DOWNLINK DIRECTION (AP IS 2)
//     let mut sta3_bg: STA_extended = STA_extended::new(
//         rate_bps_in,
//         mean_length,
//         5,
//         0,
//         coords_sta1, // although not entirely accurate, 'cause frametransmissiondelay is computed
//         //                         between AP coords (0,0,0) and the coords of the source of the packets.
//         //                         Since the delay is bidirectional, it works although it caused an error before. TODO: REFACTOR!
//         true,
//         effective_rate1,
//         t0,
//         true,
//         BG_rate_bps,
//     );
//     let mut sta4_bg: STA_extended = STA_extended::new(
//         rate_bps_in,
//         mean_length,
//         5,
//         1,
//         coords_sta2,
//         true,
//         effective_rate2,
//         t0,
//         true,
//         BG_rate_bps,
//     );

//     println!("STA1 PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
//         results1.pathloss, results1.p_rx, results1.service_delay * 1000.0, results1.data_service_delay * 1000.0, (1.0 / results1.service_delay) * mean_length);

//     println!("STA2 PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
//         results2.pathloss, results2.p_rx, results2.service_delay * 1000.0, results2.data_service_delay * 1000.0, (1.0 / results2.service_delay) * mean_length);

//     // let sta5_ul: STA_source = STA_source::new(rate_bps_in, mean_length, 2, 7, coords_sta3, false); // RX STA, acts as sink with coordinates
//     let sink: Sink = Sink::new();
//     let mbox_sink: Mailbox<Sink> = Mailbox::new();

//     let mut queue: QueueModule = QueueModule::new(num_STAs, k_queue - 1 as usize, rate_queue_bps, 0.0);

//     // mutex data handles to be able to access simulator variables, as csv vecs or CumulativeStats
//     println!("LEN BEFORE: {}", queue.STA_coords_grid.len());
//     queue.STA_coords_grid.resize(num_stas_mod, Coords::new());
//     println!("LEN AFTER: {}", queue.STA_coords_grid.len());

//     for i in 0..num_stas_mod {
//         queue.STA_coords_grid[i] = vec_coords[i];
//         println!("ID {} =  {:?} ", i, vec_coords[i]);
//     }

//     queue.STA_coords_map = map_coords.clone();

//     let csv_data_handle = queue.csv_metrics.get_data_handle();
//     let queuestats_data_handle: Arc<Mutex<QueueStats>> = queue.get_queue_stats_handle();
//     let stats_sta_data_handle: Arc<Mutex<HashMap<usize, perStaLockStats>>> =
//         queue.get_stas_stats_handle();
//     let sinkstats_data_handle = sink.get_data_handle();

//     let mbox_sta1 = Mailbox::new();
//     let mbox_sta2 = Mailbox::new();

//     let mbox_sta3 = Mailbox::new();
//     let mbox_sta4 = Mailbox::new();

//     let sta1_address = mbox_sta1.address();
//     let sta2_address = mbox_sta2.address();

//     let sta3_address = mbox_sta3.address();
//     let sta4_address = mbox_sta4.address();
//     // let sta3_address = mbox_sink.address();

//     let mbox_queue = Mailbox::new();

//     if (is_uplink) {
//         sta1_bg
//             .output_network_port
//             .connect(QueueModule::input, &mbox_queue); // Two UL STAs send
//         sta2_bg
//             .output_network_port
//             .connect(QueueModule::input, &mbox_queue);
//     }
//     if (is_downlink) {
//         sta3_bg
//             .output_network_port
//             .connect(QueueModule::input, &mbox_queue);
//         sta4_bg
//             .output_network_port
//             .connect(QueueModule::input, &mbox_queue);
//     }

//     queue.output_port_sta1.connect(Sink::input, &mbox_sink);
//     // queue.output_port_sta2.connect(Sink::input, &mbox_sink);

//     let t0 = MonotonicTime::EPOCH;

//     let mut simu: asynchronix::simulation::Simulation = SimInit::with_num_threads(64)
//         .add_model(sta1_bg, mbox_sta1, "STA1 (BG_UL)")
//         .add_model(sta2_bg, mbox_sta2, "STA2 (BG_UL)")
//         .add_model(sta3_bg, mbox_sta3, "STA3 (BG DL)")
//         .add_model(sta4_bg, mbox_sta4, "STA4 (BG DL)")
//         .add_model(queue, mbox_queue, "Queue")
//         .add_model(sink, mbox_sink, "SINK")
//         .init(t0);

//     let scheduler = simu.scheduler();
//     // ----------
//     // Simulation.
//     // ----------
//     // Check initial conditions.

//     let t = t0;

//     assert_eq!(simu.time(), t);

//     // START WITH FIRST EVENT
//     let epsilon1 = Duration::from_secs_f64(exponential(0.9));
//     let epsilon2 = Duration::from_secs_f64(exponential(0.9));

//     let duration_scheduled1 = Duration::from_secs(10) + epsilon1;
//     let duration_scheduled2 = Duration::from_secs(10) + epsilon2;

//     // Initialize sta3 and 4 for Downlink, 1 and 2 for Uplink
//     if is_uplink {
//         scheduler
//             .schedule_event(
//                 duration_scheduled1,
//                 STA_extended::send_packet_BG,
//                 (),
//                 &sta1_address,
//             )
//             .unwrap();

//         scheduler
//             .schedule_event(
//                 duration_scheduled2,
//                 STA_extended::send_packet_BG,
//                 (),
//                 &sta2_address,
//             )
//             .unwrap();
//     }

//     if is_downlink {
//         scheduler
//             .schedule_event(
//                 duration_scheduled1,
//                 STA_extended::send_packet_BG,
//                 (),
//                 &sta3_address,
//             )
//             .unwrap();

//         scheduler
//             .schedule_event(
//                 duration_scheduled2,
//                 STA_extended::send_packet_BG,
//                 (),
//                 &sta4_address,
//             )
//             .unwrap();
//     }

//     simu.step_by(Duration::from_secs_f64(stoptime)); //works

//     // After simulation, write the CSV data

//     let filename = format!("MM1K_sim"); 
//     if let Ok(data) = csv_data_handle.lock() {
//         if let Err(e) = data.write_to_csv(&filename, &dir_path) {
//             eprintln!("Failed to write CSV file: {}", e);
//         }
//     }
//     if let Ok(stats_vec) = stats_sta_data_handle.lock() {
//         // Now stats_vec is a MutexGuard<Vec<perStaLockStats>>
//         for (id, sta_stats) in stats_vec.iter() {
//             if let Ok(sta_data) = sta_stats.data.lock() {
//                 sta_data.print_nicely();
//             }
//         }

//         if let Err(e) = write_all_sta_csvs(&stats_vec, &filename, &dir_path) {
//             eprintln!("Error writing STA CSV files: {}", e);
//         }
//     }

//     if let Ok(queue_stats) = queuestats_data_handle.lock() {
//         // println!("[DEBUGDEBUGDEBU]!!!! T_s : {}, T_q : {} !", T_s_f64, T_q_f64);
//         queue_stats.print_nicely();
//     }

//     if let Ok(sink_stats) = sinkstats_data_handle.lock() {
//         sink_stats.print_nicely();
//     }

//     // println!("************ END RESULTS STAS***********\n LT: ");

//     println!("*******************************************************************");
//     println!(
//         "Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}",
//         aggregated_rate_in, mean_length, effective_rate, k_queue
//     );

//     LT.print_results();
// }

fn downlink_uplink_scenario_1BG(
    num_STAs: usize,
    stoptime: f64,
    mean_length: f64,
    k_queue: usize,
    rate_bps_in: f64,
    distance: f64,
    is_uplink: bool,
    is_downlink: bool,
    BG_rate_bps: f64,
    distance_sta0: f64,
) {
    let v_distance = vec![distance_sta0, distance, distance]; // just some random values

    let coords_sta1 = Coords {
        x: v_distance[0],
        y: 0.0,
        z: 0.0,
    };

    let coords_sta3 = coords_sta1;

    let vec_coords = vec![coords_sta1, coords_sta3];
    let id_src_coords = vec![0, 5];

    let mut map_coords: HashMap<usize, Coords> = HashMap::new();
    assert!(vec_coords.len() == id_src_coords.len());

    let mut ccounter = 0;
    for id in id_src_coords {
        map_coords.insert(id, vec_coords[ccounter]);
        ccounter += 1;
    }

    println!("vec_coords: {:?}\n", vec_coords);

    let results1 = frametransmission_delay(
        mean_length * MAX_AMPDU_SIZE as f64, // optimistic assumption of max throughput
        MAX_AMPDU_SIZE,
        Coords::new(),
        coords_sta1,
        P_TX,
    );

    let effective_rate1 = mean_length / results1.service_delay;
    let effective_rate = (effective_rate1);

    let aggregated_rate_in = (num_STAs) as f64 * rate_bps_in;

    println!("*******************************************************************");
    println!(
        "Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}",
        aggregated_rate_in, mean_length, effective_rate, k_queue
    );

    let LT = compute_mm1k_metrics(
        aggregated_rate_in,
        mean_length as f64,
        effective_rate,
        k_queue,
    );

    let t0 = MonotonicTime::EPOCH;
    let coords_ap = Coords::new();

    let mut sta1_bg: STA_extended = STA_extended::new(
        rate_bps_in,
        mean_length,
        0,
        2,
        coords_sta1,
        true,
        effective_rate1,
        t0,
        true,
        BG_rate_bps,
    );

    // DOWNLINK DIRECTION (AP IS 5, dest is 1m away)
    let mut sta3_bg: STA_extended = STA_extended::new(
        rate_bps_in,
        mean_length,
        5,
        0,
        coords_sta1,
        true,
        effective_rate1,
        t0,
        true,
        BG_rate_bps,
    );

    println!("STA1 RATE: {:.2} PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
        BG_rate_bps, results1.pathloss, results1.p_rx, results1.service_delay * 1000.0, results1.data_service_delay * 1000.0, (1.0 / results1.service_delay) * mean_length);

    // println!("STA2 PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
    //     results2.pathloss, results2.p_rx, results2.service_delay * 1000.0, results2.data_service_delay * 1000.0, (1.0 / results2.service_delay) * mean_length);

    // let sta5_ul: STA_source = STA_source::new(rate_bps_in, mean_length, 2, 7, coords_sta3, false); // RX STA, acts as sink with coordinates
    let sink: Sink = Sink::new();
    let mbox_sink: Mailbox<Sink> = Mailbox::new();

    let mut vec_ids_stas = Vec::new();
    let mut num_stas_mod = 0;

    if is_uplink {
        vec_ids_stas.push(sta1_bg.sta_id);
        num_stas_mod += 1;
    }
    if is_downlink {
        vec_ids_stas.push(sta3_bg.sta_id);
        num_stas_mod += 1;
    }

    let mut queue: QueueModule =
        QueueModule::new(num_stas_mod, k_queue - 1 as usize, 0.0, vec_ids_stas);

    // mutex data handles to be able to access simulator variables, as csv vecs or CumulativeStats
    println!("LEN BEFORE: {}", queue.STA_coords_grid.len());
    queue.STA_coords_grid.resize(num_stas_mod, Coords::new());
    println!("LEN AFTER: {}", queue.STA_coords_grid.len());

    for i in 0..num_stas_mod {
        queue.STA_coords_grid[i] = vec_coords[i];
        println!("ID {} =  {:?} ", i, vec_coords[i]);
    }

    queue.STA_coords_map = map_coords.clone();

    let csv_data_handle = queue.csv_metrics.get_data_handle();
    let queuestats_data_handle: Arc<Mutex<QueueStats>> = queue.get_queue_stats_handle();
    let stats_sta_data_handle: Arc<Mutex<HashMap<usize, perStaLockStats>>> =
        queue.get_stas_stats_handle();
    let sinkstats_data_handle = sink.get_data_handle();

    let mbox_sta1 = Mailbox::new();

    let mbox_sta3 = Mailbox::new();

    let sta1_address = mbox_sta1.address();

    let sta3_address = mbox_sta3.address();
    // let sta3_address = mbox_sink.address();

    let mbox_queue = Mailbox::new();

    if (is_uplink) {
        sta1_bg
            .output_network_port
            .connect(QueueModule::input, &mbox_queue); // Two UL STAs send
    }
    if (is_downlink) {
        sta3_bg
            .output_network_port
            .connect(QueueModule::input, &mbox_queue);
    }

    queue.output_port_sta1.connect(Sink::input, &mbox_sink);
    // queue.output_port_sta2.connect(Sink::input, &mbox_sink);

    let t0 = MonotonicTime::EPOCH;

    let mut simu: asynchronix::simulation::Simulation = SimInit::with_num_threads(64)
        .add_model(sta1_bg, mbox_sta1, "STA1 (BG_UL)")
        .add_model(sta3_bg, mbox_sta3, "STA3 (BG DL)")
        .add_model(queue, mbox_queue, "Queue")
        .add_model(sink, mbox_sink, "SINK")
        .init(t0);

    let scheduler = simu.scheduler();
    // ----------
    // Simulation.
    // ----------
    // Check initial conditions.

    let t = t0;

    assert_eq!(simu.time(), t);

    // START WITH FIRST EVENT
    let epsilon1 = Duration::from_secs_f64(exponential(0.9));
    let epsilon2 = Duration::from_secs_f64(exponential(0.9));

    let duration_scheduled1 = Duration::from_secs(10) + epsilon1;
    let duration_scheduled2 = Duration::from_secs(10) + epsilon2;

    // Initialize sta3 and 4 for Downlink, 1 and 2 for Uplink
    if is_uplink {
        scheduler
            .schedule_event(
                duration_scheduled1,
                STA_extended::send_packet_BG,
                (),
                &sta1_address,
            )
            .unwrap();
    }

    if is_downlink {
        scheduler
            .schedule_event(
                duration_scheduled1,
                STA_extended::send_packet_BG,
                (),
                &sta3_address,
            )
            .unwrap();
    }

    simu.step_by(Duration::from_secs_f64(stoptime)); //works

    // After simulation, write the CSV data
    let mbps = BG_rate_bps / 1e6;
    let filename = format!("{:.1}Mbps", mbps);
    let mut dir: String = format!("Results_ERRORRR/");
    if is_uplink {
        dir = format!("Results_NBG{:.0}_UL/{:.1}Mbps/", num_STAs, mbps);
    } else if is_downlink {
        dir = format!("Results_NBG{:.0}_DL/{:.1}Mbps/", num_STAs, mbps);
    }
    // let dir: String = format!("Results_NBG{:.0}_UL/", num_STAs);
    if !fs::metadata(&dir).is_ok() {
        fs::create_dir_all(&dir).expect("Failed to create Results directory");
    }

    // Ensure the directory exists
    let dir_path = dir.clone();
    // Create the directory if it doesn't exist
    if let Err(e) = fs::create_dir_all(&dir_path) {
        eprintln!("Failed to create directory: {}", e);
        return; // Stop execution if the directory creation fails
    }

    if let Ok(data) = csv_data_handle.lock() {
        if let Err(e) = data.write_to_csv(&filename, &dir_path) {
            eprintln!("FILENAMEEE: {}   aaand {}", filename, dir_path);
            eprintln!("Failed to write CSV file: {}", e);
        }
    }
    if let Ok(stats_vec) = stats_sta_data_handle.lock() {
        // Now stats_vec is a MutexGuard<Vec<perStaLockStats>>
        for (id, sta_stats) in stats_vec.iter() {
            if let Ok(sta_data) = sta_stats.data.lock() {
                sta_data.print_nicely();
            }
        }

        if let Err(e) = write_all_sta_csvs(&stats_vec, &filename, &dir_path) {
            eprintln!("FILENAMEEE: {}, dir_path: {}", filename, dir_path);
            eprintln!("Error writing STA CSV files: {}", e);
        }
    }

    if let Ok(queue_stats) = queuestats_data_handle.lock() {
        // println!("[DEBUGDEBUGDEBU]!!!! T_s : {}, T_q : {} !", T_s_f64, T_q_f64);
        queue_stats.print_nicely();
    }

    if let Ok(sink_stats) = sinkstats_data_handle.lock() {
        sink_stats.print_nicely();
    }

    // println!("************ END RESULTS STAS***********\n LT: ");

    println!("*******************************************************************");
    println!(
        "Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}",
        aggregated_rate_in, mean_length, effective_rate, k_queue
    );

    LT.print_results();
}

fn main() {
    env::set_var("RUST_BACKTRACE", "1"); // for debug backtrace!

    // READ COMMAND-LINE ARGUMENTS
    let args: Vec<String> = env::args().collect();

    if args.len() != 10 {
        eprintln!(
            "Usage: {} <mean_length> <k_queue> <rate_bps_IN> <distance> <bandwidth_STA> <is_UL> <N_BG> <distance_STA0>",
            args[0]
        );
        println!("ARGS: {:#?}", args); 
        return;
    }
    let stoptime: f64 = args[1].parse().expect("Invalid T_END");
    let mean_length: f64 = args[2].parse().expect("Invalid mean_length");
    let k_queue: usize = args[3].parse().expect("Invalid k_queue");
    let rate_bps_in: f64 = args[4].parse().expect("Invalid rate_bps_in");
    let distance: f64 = args[5].parse().expect("Invalid STA distance");
    let BG_rate: f64 = args[6].parse().expect("Invalid BG_rate");

    let is_uplink: usize = args[7].parse().expect("Invalid is_UL");
    let N_BG: usize = args[8].parse().expect("Invalid N_BG");
    let distance_sta0: f64 = args[9].parse().expect("Invalid STA distance");

    println!("is_uplink: {}, N_BG: {}", is_uplink, N_BG);

    let mut is_downlink = false;
    let mut is_ul_arg = false;

    if is_uplink == 0 {
        is_downlink = true;
        is_ul_arg = false;
    } else {
        println!("UPlink scenario");
        is_downlink = false;
        is_ul_arg = true;
    }
    if N_BG == 1 {
        downlink_uplink_scenario_1BG(
            N_BG,
            stoptime,
            mean_length,
            k_queue,
            rate_bps_in,
            distance,
            is_ul_arg,
            is_downlink,
            BG_rate,
            distance_sta0,
        );
    } else if N_BG >= 2 {
        // downlink_uplink_scenario(
        //     N_BG,
        //         stoptime,
        //         mean_length,
        //         k_queue,
        //         rate_bps_in,
        //         distance,
        //         is_ul_arg ,
        //         is_downlink,
        //         BG_rate,
        // )
        downlink_uplink_scenario_flexible(
            N_BG,
            stoptime,
            mean_length,
            k_queue,
            rate_bps_in,
            distance,
            is_uplink == 1,
            is_downlink,
            BG_rate,
            distance_sta0,
        );
    }

    println!("END DLUL scenario");
}
