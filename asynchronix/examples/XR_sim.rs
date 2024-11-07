#[allow(unused_imports)]
#[allow(dead_code)]
////////////////////////////////////// XR SIMULATOR ////////////////////////////
///
///  Mixing up connection.rs and bitratemanager to simplify the process of generating frames.
///     * Will try to stay accurate to packet latencies in all parts of the pipeline ( for now, linear terms with maybe some randomness)
///
/// TODO:   
///     * XRServer sending packets , rate corresponding to FPS and bitrate (90 fps, 100 Mbps) to sink, with correct headers.
//      * Decoder queue of XRClient

///
use asynchronix::simulation::{Mailbox, Scheduler, SimInit};
use asynchronix::time::MonotonicTime;
// use futures_util::Stream;
// use lib::alvr_stream_socket::{Buffer, StreamReceiver};

// use tai_time::TaiTime;
use crate::lib::alvr_stream_socket::{
    AUDIO, HAPTICS, INITIAL_FRAMERATE_FPS, MAX_HISTORY_SIZE, STATISTICS, TRACKING, VIDEO,
};
mod lib; // for calling m own local library
use crate::lib::models_mm1k::{QueueModule, QueueStats, Sink};
use crate::lib::{
    exponential,
    frametransmission_delay,
    perStaLockStats,
    // AmpduPacket,
    Coords,
    DebugColor,
    //   MpduPacket, SlidingWindowAverage,
    MAX_AMPDU_SIZE,
    P_TX,
};
use lib::write_all_sta_csvs;
use std::env;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::lib::models_XR::{STA_extended, XRClient, XRServer};
// use rand::Rng;
// use rand_distr::{Distribution, Normal};

// use std::intrinsics::size_of;
// use serde::{Deserialize, Serialize};
// use std::mem;

// Instant};

// use std::time::SystemTime;

// mod mm1k_sim;
// use crate::mm1k_sim::{QueueModule, QueueStats, Sink, DataSink};

// use crate::lib::alvr_packets::{ClientStatistics, NetworkStatisticsPacket};
// use crate::lib::alvr_stream_socket::{
// parse_shard_data, AnyhowToCon, DscpTos, Haptics, ReceiverData, SocketBufferSize,
// SocketProtocol, SocketReader, StreamSender, StreamSocketBuilder, Tracking, VideoPacketHeader,
// };

// use std::cmp::{self, max};

// use rand::random;
// use std::collections::VecDeque;
// use std::f64::consts::PI;
// use std::future::Future;

// use asynchronix::model::{Context, Model};
// use asynchronix::ports::Output;

// use std::collections::HashMap;
// use std::sync::RwLock;

// mod alvr_statistics_manager;  //TODO!
// use alvr_statistics_manager::*;

// use crate::lib::alvr_statistics::StatisticsManager;

const RETRY_CONNECT_MIN_INTERVAL: Duration = Duration::from_secs(1);
const STREAMING_RECV_TIMEOUT: Duration = Duration::from_millis(2000);

const INITIAL_BITRATE_MBPS_SIM: f32 = 5.0;

// use crate::lib::{AmpduPacket, MpduPacket, exponential, Coords, CumulativeStats, CsvType};
// use crate::{debug_print, format_elapsed, format_timestamp};

use crate::lib::DEBUG_PRINT_ENABLED;

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

    const num_STAs: usize = 2;

    let v_distance = vec![1.0, distance, distance]; // just some random values

    const NUM_STAS_UL: usize = 1; //for now

    let coords_staxr = Coords {
        x: v_distance[0],
        y: 0.0,
        z: 0.0,
    };
    let coords_sink = Coords {
        x: v_distance[1],
        y: 0.0,
        z: 0.0,
    };

    let vec_coords = vec![coords_staxr, coords_sink];
    println!("vec_coords: {:?}\n", vec_coords);

    // TODO: Make this dynamic based on Vec<Coords> and Vec<ResultsFrameTXDelay> with a function

    let results1 = frametransmission_delay(
        INITIAL_BITRATE_MBPS_SIM as f64 * 1E6, // optimistic assumption of max throughput
        MAX_AMPDU_SIZE,
        Coords::new(),
        coords_staxr,
        P_TX,
    );

    let results2 = frametransmission_delay(
        mean_length * MAX_AMPDU_SIZE as f64,
        MAX_AMPDU_SIZE,
        Coords::new(),
        coords_sink, // TO TEST
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

    let ip_src = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let ip_dest = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));

    let t0 = MonotonicTime::EPOCH;
    let mut xr_server = XRServer::new(ip_src, ip_dest, t0);
    let mut xr_client_app = XRClient::new(ip_src);

    let mut sta1_xr: STA_extended = STA_extended::new(
        INITIAL_BITRATE_MBPS_SIM as f64 * 1E6,
        mean_length,
        0,
        2,
        coords_staxr,
        true,
        effective_rate1,
        t0,
    ); // STAs 0 and 1 send traffic to 5 through AP

    println!("STA XR Server PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
        results1.pathloss, results1.p_rx, results1.service_delay * 1000.0, results1.data_service_delay * 1000.0, (1.0 / results1.service_delay) * mean_length);

    println!("STA XR Client PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
        results2.pathloss, results2.p_rx, results2.service_delay * 1000.0, results2.data_service_delay * 1000.0, (1.0 / results2.service_delay) * mean_length);

    let mut sta_client = STA_extended::new(0.0, 1.0, 2, 0, coords_sink, true, effective_rate2, t0);

    let mut queue: QueueModule = QueueModule::new(num_STAs, k_queue - 1 as usize, rate_queue_bps);

    // mutex data handles to be able to access simulator variables, as csv vecs or CumulativeStats

    queue.STA_coords_grid.resize(num_STAs, Coords::new());
    for i in 0..num_STAs {
        queue.STA_coords_grid[i] = vec_coords[i];
    }

    let mbox_xr_server = Mailbox::new();
    let mut mbox_sta_xr_server = Mailbox::new();
    let mbox_queue = Mailbox::new();
    let mbox_sta_client_xr = Mailbox::new();
    let mbox_client_xr_app = Mailbox::new();

    let xr_server_address = mbox_xr_server.address();
    let sta1_address = mbox_sta_xr_server.address();
    let queue_address = mbox_queue.address();
    let sta_client_address = mbox_sta_client_xr.address();
    let xr_client_app_address = mbox_client_xr_app.address();

    let csv_data_handle = queue.csv_metrics.get_data_handle(); // all queue stats for csv (per packet)
    let queuestats_data_handle: Arc<Mutex<QueueStats>> = queue.get_queue_stats_handle(); // cumulative averages, sliding windows
    let stats_sta_data_handle: Arc<Mutex<Vec<perStaLockStats>>> = queue.get_stas_stats_handle(); // cumulative averages, per-sta
                                                                                                 // let sinkstats_data_handle = sink.get_data_handle();               // counters at sink

    // CONNECT COMPONENTS
    xr_server
        .outport_videoapp_network
        .connect(STA_extended::input_XR_app, &mbox_sta_xr_server);

    sta1_xr
        .output_network_port
        .connect(QueueModule::input, &mbox_queue);
    queue
        .output_port
        .connect(STA_extended::input_wireless, &mbox_sta_client_xr);
    // queue.output_port.connect(STA_extended::input_wireless, &mbox_sta_xr_server);

    sta_client
        .to_app_socket
        .connect(XRClient::in_from_network, &mbox_client_xr_app);

    sta1_xr
        .to_app_socket
        .connect(XRServer::in_from_network, &mbox_xr_server);
    sta_client
        .output_network_port
        .connect(QueueModule::input, &mbox_queue);

    // // connect applications to STAs:
    // sta1_xr.to_app_socket.connect(XRServer::in_from_network, &mbox_sta_xr );
    // sta_client.to_app_socket.connect(XRClient::in_from_network, &mbox_sta_client_xr);

    // xr_client.outport_streams.connect(STA_extended::input_XR_app, &mbox_sta_client_xr);

    let mut simu: asynchronix::simulation::Simulation = SimInit::with_num_threads(64)
        .add_model(xr_server, mbox_xr_server, "ALVR Server")
        .add_model(sta1_xr, mbox_sta_xr_server, "STA1 (XR_s)")
        .add_model(queue, mbox_queue, "Queue")
        .add_model(sta_client, mbox_sta_client_xr, "STA 2 (XR Client)")
        .add_model(xr_client_app, mbox_client_xr_app, "ALVR Client")
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

    let duration_scheduled1 = Duration::from_secs(10) + epsilon1;

    scheduler
        .schedule_event(
            duration_scheduled1,
            XRServer::connection_pipeline,
            ip_dest,
            &xr_server_address,
        )
        .unwrap();
    scheduler
        .schedule_event(
            Duration::from_nanos(1),
            XRClient::configure_streams,
            (),
            &xr_client_app_address,
        )
        .unwrap();

    simu.step_by(Duration::from_secs_f64(stoptime)); //works

    // After simulation, write the CSV data
    if let Ok(data) = csv_data_handle.lock() {
        if let Err(e) = data.write_to_csv() {
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

        if let Err(e) = write_all_sta_csvs(&stats_vec) {
            eprintln!("Error writing STA CSV files: {}", e);
        }
    }

    if let Ok(queue_stats) = queuestats_data_handle.lock() {
        // println!("[DEBUGDEBUGDEBU]!!!! T_s : {}, T_q : {} !", T_s_f64, T_q_f64);
        queue_stats.print_nicely();
    }

    // if let Ok(sink_stats) = sinkstats_data_handle.lock() {
    //     sink_stats.print_nicely();
    // }

    // println!("************ END RESULTS STAS***********\n LT: ");

    println!("*******************************************************************");
    println!(
        "Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}",
        aggregated_rate_in, mean_length, effective_rate, k_queue
    );
}
