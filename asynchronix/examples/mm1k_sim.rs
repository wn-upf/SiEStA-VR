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

use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;
use lib::models_mm1k::STA_source;

use std::time::Duration;

use std::sync::{Arc, Mutex};

// mod lib; // for calling m own local library
mod lib;
use crate::lib::{
    compute_mm1k_metrics, exponential, frametransmission_delay, perStaLockStats,
    write_all_sta_csvs, Coords, CsvData, DebugColor, MAX_AMPDU_SIZE, P_TX,
};

use crate::lib::models_mm1k::{QueueModule, QueueStats, Sink};
// use crate::lib::{AmpduPacket, MpduPacket, exponential, Coords, CumulativeStats, CsvType};
// use crate::{debug_print, format_elapsed, format_timestamp};

use std::env;

use asynchronix::model::Model;


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////// SIMULATION ////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn simple_MM1K(
    // 1st scenario: Single STA -> Queue -> Sink
    stoptime: f64,
    mean_length: f64,
    k_queue: usize,
    rate_bps_in: f64,
    rate_queue_bps: f64,
    distance: f64,
) {
    let num_STAs = 1;
    let coords_sta = Coords {
        x: distance,
        y: 0.0,
        z: 0.0,
    };
    let results = frametransmission_delay(
        mean_length as f64,
        MAX_AMPDU_SIZE,
        Coords::new(),
        coords_sta,
        P_TX,
    );

    let effective_rate = mean_length / results.service_delay;

    let LT = compute_mm1k_metrics(rate_bps_in, mean_length, effective_rate, k_queue);

    let rate_service_bps: f64 = 20E3;

    let mut source: STA_source = STA_source::new(
        rate_bps_in,
        mean_length,
        0,
        2,
        coords_sta,
        true,
        rate_service_bps,
    ); // STAs 0
    let mut queue: QueueModule = QueueModule::new(num_STAs, k_queue - 1 as usize, rate_queue_bps);
    let sink = Sink::new();

    // mutex data handles to be able to access simulator variables, as csv vecs or CumulativeStats
    let csv_data_handle: Arc<Mutex<CsvData>> = queue.csv_metrics.get_data_handle();
    let queuestats_data_handle = queue.get_queue_stats_handle();
    let stats_sta_data_handle: Arc<Mutex<Vec<perStaLockStats>>> = queue.get_stas_stats_handle();

    let mbox_src = Mailbox::new();
    let mbox_src_address = mbox_src.address();

    let mbox_queue = Mailbox::new();
    let queue_address = mbox_queue.address();

    let sink_mbox = Mailbox::new();
    let sink_mbox_address = sink_mbox.address();

    // CONNECT COMPONENTS
    // source.output_port.connect(Sink::input, &sink_mbox);

    source.output_port.connect(QueueModule::input, &mbox_queue);
    queue.output_port_sta1.connect(Sink::input, &sink_mbox);
    queue.output_port_sta2.connect(Sink::input, &sink_mbox);


    let t0 = MonotonicTime::EPOCH;

    let mut simu = SimInit::new()
        .add_model(source, mbox_src, "STA BG")
        .add_model(queue, mbox_queue, "Queue")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0);

    let scheduler = simu.scheduler();

    // ----------
    // Simulation.
    // ----------

    // Check initial conditions.

    let t = t0;

    assert_eq!(simu.time(), t);

    // START WITH FIRST EVENT
    scheduler
        .schedule_event(
            Duration::from_millis(1),
            STA_source::send_packet,
            (),
            &mbox_src_address,
        )
        .unwrap();

    simu.step_by(Duration::from_secs_f64(stoptime)); //works

    // After simulation, write the CSV data
    if let Ok(data) = csv_data_handle.lock() {
        if let Err(e) = data.write_to_csv("") {
            eprintln!("Failed to write CSV file: {}", e);
        }
    }

    if let Ok(data) = stats_sta_data_handle.lock() {
        if let Err(e) = write_all_sta_csvs(&data, "") {
            eprintln!("Error writing STA CSV files: {}", e);
        }
    }

    // println!("************ END RESULTS ***********\n LT: {:#?}", LT);
    LT.print_results();

    if let Ok(queue_stats) = queuestats_data_handle.lock() {
        println!(
            "Waiting time mean: {}",
            queue_stats.waiting_time_cum.get_average()
        );
        println!(
            "Waiting time std dev: {}",
            queue_stats.waiting_time_cum.get_std_dev()
        );
        println!(
            "Service time mean: {}",
            queue_stats.service_time_cum.get_average()
        );
        println!(
            "Service time std dev: {}",
            queue_stats.service_time_cum.get_std_dev()
        );
    } else {
        eprintln!("Failed to lock queue stats");
    };
}

// SCENARIO 2: TWO STAS AS BG TRAFFIC, 1 STA AS SINK
fn multiple_STA_sim(
    num_STAs: usize,
    stoptime: f64,
    mean_length: f64,
    k_queue: usize,
    rate_bps_in: f64,
    rate_queue_bps: f64,
    distance: f64,
) {
    let v_distance = vec![1.0, distance, distance]; // just some random values

    const NUM_STAS_UL: usize = 1; //for now

    let coords_sta1 = Coords {
        x: v_distance[0],
        y: 0.0,
        z: 0.0,
    };
    let coords_sta2 = Coords {
        x: v_distance[1],
        y: 0.0,
        z: 0.0,
    };
    let coords_sta3 = Coords {
        x: v_distance[2],
        y: 0.0,
        z: 0.0,
    };

    let vec_coords = vec![coords_sta1, coords_sta2, coords_sta3];
    println!("vec_coords: {:?}\n", vec_coords);

    let results1 = frametransmission_delay(
        mean_length * MAX_AMPDU_SIZE as f64, // optimistic assumption of max throughput
        MAX_AMPDU_SIZE,
        Coords::new(),
        coords_sta1,
        P_TX,
    );

    let results2 = frametransmission_delay(
        mean_length * MAX_AMPDU_SIZE as f64,
        MAX_AMPDU_SIZE,
        Coords::new(),
        coords_sta2,
        P_TX,
    );

    let effective_rate1 = mean_length / results1.service_delay;
    let effective_rate2 = mean_length / results2.service_delay;
    let effective_rate = (effective_rate1 + effective_rate2) / 2.0;

    let aggregated_rate_in = (num_STAs - NUM_STAS_UL) as f64 * rate_bps_in;

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

    let mut sta1_bg: STA_source = STA_source::new(
        rate_bps_in,
        mean_length,
        0,
        2,
        coords_sta1,
        true,
        effective_rate1,
    ); // STAs 0 and 1 send traffic to 5 through AP
    let mut sta2_bg: STA_source = STA_source::new(
        rate_bps_in,
        mean_length,
        1,
        2,
        coords_sta2,
        true,
        effective_rate2,
    );

    println!("STA1 PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
        results1.pathloss, results1.p_rx, results1.service_delay * 1000.0, results1.data_service_delay * 1000.0, (1.0 / results1.service_delay) * mean_length);

    println!("STA2 PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
        results2.pathloss, results2.p_rx, results2.service_delay * 1000.0, results2.data_service_delay * 1000.0, (1.0 / results2.service_delay) * mean_length);

    // let sta5_ul: STA_source = STA_source::new(rate_bps_in, mean_length, 2, 7, coords_sta3, false); // RX STA, acts as sink with coordinates
    let sink: Sink = Sink::new();
    let mbox_sink: Mailbox<Sink> = Mailbox::new();

    let mut queue: QueueModule = QueueModule::new(num_STAs, k_queue - 1 as usize, rate_queue_bps, 0.0);

    // mutex data handles to be able to access simulator variables, as csv vecs or CumulativeStats

    queue.STA_coords_grid.resize(num_STAs, Coords::new());
    for i in 0..num_STAs {
        queue.STA_coords_grid[i] = vec_coords[i];
    }

    let csv_data_handle = queue.csv_metrics.get_data_handle();
    let queuestats_data_handle: Arc<Mutex<QueueStats>> = queue.get_queue_stats_handle();
    let stats_sta_data_handle: Arc<Mutex<Vec<perStaLockStats>>> = queue.get_stas_stats_handle();
    let sinkstats_data_handle = sink.get_data_handle();

    let mbox_sta1 = Mailbox::new();
    let mbox_sta2 = Mailbox::new();

    let sta1_address = mbox_sta1.address();
    let sta2_address = mbox_sta2.address();
    // let sta3_address = mbox_sink.address();

    let mbox_queue = Mailbox::new();
    // let queue_address = mbox_queue.address();

    // let sink_mbox = Mailbox::new();
    // let sink_mbox_address = sink_mbox.address();

    // CONNECT COMPONENTS
    // source.output_port.connect(Sink::input, &sink_mbox);

    sta1_bg.output_port.connect(QueueModule::input, &mbox_queue); // Two DL STAs send
    sta2_bg.output_port.connect(QueueModule::input, &mbox_queue);

    queue.output_port_sta1.connect(Sink::input, &mbox_sink);
    queue.output_port_sta2.connect(Sink::input, &mbox_sink);

    let t0 = MonotonicTime::EPOCH;

    let mut simu: asynchronix::simulation::Simulation = SimInit::with_num_threads(64)
        .add_model(sta1_bg, mbox_sta1, "STA1 (BG)")
        .add_model(sta2_bg, mbox_sta2, "STA2 (BG)")
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

    scheduler
        .schedule_event(
            duration_scheduled1,
            STA_source::send_packet,
            (),
            &sta1_address,
        )
        .unwrap();

    scheduler
        .schedule_event(
            duration_scheduled2,
            STA_source::send_packet,
            (),
            &sta2_address,
        )
        .unwrap();

    simu.step_by(Duration::from_secs_f64(stoptime)); //works

    // After simulation, write the CSV data

    let filename = format!("MM1K_sim"); 
    if let Ok(data) = csv_data_handle.lock() {
        if let Err(e) = data.write_to_csv(&filename) {
            eprintln!("Failed to write CSV file: {}", e);
        }
    }
    if let Ok(stats_vec) = stats_sta_data_handle.lock() {
        // Now stats_vec is a MutexGuard<Vec<perStaLockStats>>
        for sta_stats in stats_vec.iter() {
            if let Ok(sta_data) = sta_stats.data.lock() {
                sta_data.print_nicely();
            }
        }

        if let Err(e) = write_all_sta_csvs(&stats_vec, &filename) {
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
    if args.len() != 7 {
        eprintln!(
            "Usage: {} <mean_length> <k_queue> <rate_bps> <rate_queue_bps> <distance>",
            args[0]
        );
        return;
    }
    let stoptime: f64 = args[1].parse().expect("Invalid T_END");
    let mean_length: f64 = args[2].parse().expect("Invalid mean_length");
    let k_queue: usize = args[3].parse().expect("Invalid k_queue");
    let rate_bps_in: f64 = args[4].parse().expect("Invalid rate_bps_in");
    let rate_queue_bps: f64 = args[5].parse().expect("Invalid rate_queue_bps");
    let distance: f64 = args[6].parse().expect("Invalid STA distance");

    /// SCENARIO 1: MM1K WITH POISSON, QUEUE, SINK
    simple_MM1K(
        stoptime,
        mean_length,
        k_queue,
        rate_bps_in,
        rate_queue_bps,
        distance,
    );

    // multiple_STA_sim(
    //     3,
    //     stoptime,
    //     mean_length,
    //     k_queue,
    //     rate_bps_in,
    //     rate_queue_bps,
    //     distance,
    // );
}
