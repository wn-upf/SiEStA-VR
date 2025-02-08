use asynchronix::model::Context;
#[allow(unused_imports)]
#[allow(dead_code)]
#[allow(unused)]
////////////////////////////////////// XR SIMULATOR ////////////////////////////
///
///  Mixing up connection.rs and bitratemanager to simplify the process of generating frames.
///     * Will try to stay accurate to packet latencies in all parts of the pipeline ( for now, linear terms with maybe some randomness)
///
///
use asynchronix::simulation::{Mailbox, Scheduler, SimInit};
use asynchronix::time::MonotonicTime;
use std::collections::HashMap;
// use futures_util::Stream;
// use lib::alvr_stream_socket::{Buffer, StreamReceiver};

// use tai_time::TaiTime;

use crate::lib::alvr_stream_socket::INITIAL_FRAMERATE_FPS;
mod lib; // for calling m own local library

use crate::lib::models_mm1k::{QueueModule, QueueStats};
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
use std::fs;
use std::path::Path;

use lib::write_all_sta_csvs;
use std::env;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::lib::models_XR::{STA_extended, SinkVideo_XR, XRClient, XRServer};

// use crate::lib::INITIAL_BITRATE_MBPS_SIM;

// use crate::lib::{AmpduPacket, MpduPacket, exponential, Coords, CumulativeStats, CsvType};
// use crate::{debug_print, format_elapsed, format_timestamp};

fn main_old() {
    env::set_var("RUST_BACKTRACE", "1"); // for debug backtrace!
                                         // std::env::set_var("RUST_BACKTRACE", "full");
                                         // READ COMMAND-LINE ARGUMENTS
    let args: Vec<String> = env::args().collect();

    if args.len() != 9 {
        eprintln!(
            "Usage: {} <mean_length> <k_queue> <rate_bps> <rate_queue_bps> <distance> <bitrate> <PL_prob>",
            args[0]
        );
        println!("ARGS: {:#?}", args);

        return;
    }
    
    let stoptime: f64 = args[1].parse().expect("Invalid T_END");
    let mean_length: f64 = args[2].parse().expect("Invalid mean_length");
    let k_queue: usize = args[3].parse().expect("Invalid k_queue");
    let rate_bps_in: f64 = args[4].parse().expect("Invalid rate_bps_in");
    let rate_queue_bps: f64 = args[5].parse().expect("Invalid rate_queue_bps");
    let distance: f64 = args[6].parse().expect("Invalid STA distance");
    let initial_bitrate: f64 = args[7].parse().expect("Invalid bitrate");
    let pl_prob: f64 = args[8].parse().expect("Invalid PL probability");

    let name_folder = format!(
        "sim_T{:.0}_Plen{:.0}_K{}_Rq{:.0}_D{:.0}_Br{:.0}_PL{:.6}",
        stoptime,        // T: simulation end time (stoptime)
        mean_length,     // ML: mean packet length
        k_queue,         // K: queue capacity
        rate_queue_bps,  // Rq: queue bitrate
        distance,        // D: STA distance
        initial_bitrate, // Br: initial bitrate
        pl_prob
    );
    println!("NAME_FOLDER: {:?}", name_folder);
    println!("PL_probability: {:?}", pl_prob);

    std::thread::sleep(Duration::from_secs(2));

    let output_path = format!("Results/{}", name_folder);
    let path = Path::new(&output_path);

    // Ensure the folder exists or create it
    if let Err(e) = fs::create_dir_all(path) {
        eprintln!("Error creating directory {}: {}", output_path, e);
    } else {
        println!("Directory created or exists at {}", output_path);
    }

    const NUM_STAS: usize = 2;

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
        initial_bitrate as f64 * 1E6, // optimistic assumption of max throughput
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

    let aggregated_rate_in = (NUM_STAS - NUM_STAS_UL) as f64 * rate_bps_in;

    println!("*******************************************************************");
    println!(
        "Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}",
        aggregated_rate_in, mean_length, effective_rate, k_queue
    );

    let ip_src = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let ip_dest = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));

    let t0 = MonotonicTime::EPOCH;
    let mut xr_server = XRServer::new(
        ip_src,
        ip_dest,
        t0,
        INITIAL_FRAMERATE_FPS,
        initial_bitrate as f32,
        &name_folder,
    );
    let mut xr_client_app = XRClient::new(ip_src, INITIAL_FRAMERATE_FPS);

    let vec_ids = vec![0, 2];

    let mut sta1_xr: STA_extended = STA_extended::new(
        initial_bitrate as f64 * 1E6,
        mean_length,
        vec_ids[0],
        vec_ids[1],
        coords_staxr,
        true,
        effective_rate1,
        t0,
        false,
        0.0, 
        

    ); // STAs 0 and 1 send traffic to 5 through AP
    // pub fn new(
    //     arrival_rate_bps: f64,
    //     mean_length: f64,
    //     src: i32,
    //     dest: i32,
    //     coordinates: Coords,
    //     does_sta_transmit: bool,
    //     rate_service_bps: f64,
    //     t0_sim: TaiTime<0>,
    //     is_bg_sta: bool,
    //     arrival_rate_BG: f64,



    let mut sta_client = STA_extended::new(
        0.0,
        1.0,
        2,
        0,
        coords_sink,
        true,
        effective_rate2,
        t0,
        false,
        0.0, 
    );
    println!("STA XR Server PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
        results1.pathloss, results1.p_rx, results1.service_delay * 1000.0, results1.data_service_delay * 1000.0, (1.0 / results1.service_delay) * mean_length);

    println!("STA XR Client PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
        results2.pathloss, results2.p_rx, results2.service_delay * 1000.0, results2.data_service_delay * 1000.0, (1.0 / results2.service_delay) * mean_length);

    let mut queue: QueueModule =
        QueueModule::new(NUM_STAS, k_queue - 1 as usize, pl_prob, vec_ids.clone());
        // pub fn new(num_stas: usize, queue_size: usize, PL_prob: f64, vec_ids: Vec<i32>) -> Self {


    // mutex data handles to be able to access simulator variables, as csv vecs or CumulativeStats

    queue.STA_coords_grid.resize(NUM_STAS, Coords::new());
    for i in 0..NUM_STAS {
        queue.STA_coords_grid[i] = vec_coords[i];
    }

    let mbox_xr_server_app = Mailbox::new();
    let mbox_xr_client_app = Mailbox::new();

    let mbox_queue = Mailbox::new();

    let mbox_sta_client_xr = Mailbox::new();
    let mbox_sta_xr_server = Mailbox::new();

    let xr_server_app_address = mbox_xr_server_app.address();

    let decoder_video_sink = SinkVideo_XR::new();
    let mbox_decoder_video = Mailbox::new();
    let decoded_video_address = mbox_decoder_video.address();

    let sta1_address = mbox_sta_xr_server.address();
    let queue_address = mbox_queue.address();
    let sta_client_address = mbox_sta_client_xr.address();
    let xr_client_app_address = mbox_xr_client_app.address();

    let csv_data_handle = queue.csv_metrics.get_data_handle(); // all queue stats for csv (per packet)
    let queuestats_data_handle: Arc<Mutex<QueueStats>> = queue.get_queue_stats_handle(); // cumulative averages, sliding windows
    let stats_sta_data_handle: Arc<Mutex<HashMap<usize, perStaLockStats>>> =
        queue.get_stas_stats_handle(); // cumulative averages, per-sta

    // let sinkstats_data_handle = sink.get_data_handle();               // counters at sink

    // CONNECT COMPONENTS
    xr_server
        .outport_videoapp_network
        .connect(STA_extended::input_XR_app, &mbox_sta_xr_server);

    sta1_xr
        .output_network_port
        .connect(QueueModule::input, &mbox_queue);

    sta_client
        .output_network_port
        .connect(QueueModule::input_UL, &mbox_queue);

    queue
        .output_port_sta1
        .connect(STA_extended::input_wireless, &mbox_sta_client_xr);

    queue
        .output_port_sta1
        .connect(STA_extended::input_wireless, &mbox_sta_xr_server); // UL CONNECTION QUEUE

    sta_client
        .to_app_socket
        .connect(XRClient::in_from_network, &mbox_xr_client_app);

    sta1_xr
        .to_app_socket
        .connect(XRServer::in_from_network, &mbox_xr_server_app);

    xr_client_app
        .output_app_network
        .connect(STA_extended::input_XR_app, &mbox_sta_client_xr);

    xr_client_app
        .out_video_decoded
        .connect(SinkVideo_XR::in_video, &mbox_decoder_video);
    // // connect applications to STAs:
    // sta1_xr.to_app_socket.connect(XRServer::in_from_network, &mbox_sta_xr );
    // sta_client.to_app_socket.connect(XRClient::in_from_network, &mbox_sta_client_xr);

    // xr_client.outport_streams.connect(STA_extended::input_XR_app, &mbox_sta_client_xr);

    let mut simu: asynchronix::simulation::Simulation = SimInit::new()
        .add_model(xr_server, mbox_xr_server_app, "ALVR Server")
        .add_model(sta1_xr, mbox_sta_xr_server, "STA1 (XR_s)")
        .add_model(queue, mbox_queue, "Queue")
        .add_model(sta_client, mbox_sta_client_xr, "STA 2 (XR Client)")
        .add_model(xr_client_app, mbox_xr_client_app, "ALVR Client")
        .add_model(decoder_video_sink, mbox_decoder_video, "Video decode Sink")
        .init(t0);

    let scheduler = simu.scheduler();
    // ----------
    // Simulation.
    // ----------
    // Check initial conditions.

    let t = t0;

    assert_eq!(simu.time(), t);

    // START WITH FIRST EVENT
    let epsilon1 = Duration::from_secs_f64(exponential(12.0));

    let duration_scheduled1 = Duration::from_secs(10);

    scheduler // Configure XRClient before sending packets to it
        .schedule_event(
            Duration::from_nanos(1),
            XRClient::configure_streams,
            (),
            &xr_client_app_address,
        )
        .unwrap();

    scheduler
        .schedule_event(
            duration_scheduled1,
            XRServer::connection_pipeline,
            ip_dest,
            &xr_server_app_address,
        )
        .unwrap();

    scheduler
        .schedule_event(
            duration_scheduled1,
            XRClient::vsync,
            (),
            &xr_client_app_address,
        )
        .unwrap();
    // scheduler.schedule_periodic_event(Duration::from_millis(10), Duration::from_millis(10), XRClient::video_receive_thread, (), &xr_client_app_address).unwrap();  // video receiver thread of ALVR

    println!("Scheduling EMULATOR TX");
    scheduler
        .schedule_event(
            duration_scheduled1,
            QueueModule::self_scheduled_emu_queue_tx,
            (),
            &queue_address,
        )
        .unwrap();

    simu.step_by(Duration::from_secs_f64(stoptime)); //works

    let path_inter = format!("XR RESULTS");

    // After simulation, write the CSV data
    if let Ok(data) = csv_data_handle.lock() {
        if let Err(e) = data.write_to_csv(&name_folder, &path_inter ) {
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

        if let Err(e) = write_all_sta_csvs(&stats_vec, &name_folder, &path_inter) {
            println!("name_folder: {name_folder}");
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

struct VRPair {
    xr_server: XRServer,
    xr_client: XRClient,
    sta_server: STA_extended,
    sta_client: STA_extended,
    mbox_xr_server: Mailbox<XRServer>,
    mbox_xr_client: Mailbox<XRClient>,
    mbox_sta_server: Mailbox<STA_extended>,
    mbox_sta_client: Mailbox<STA_extended>,
}

impl VRPair {
    fn new(
        pair_index: usize,
        t0: MonotonicTime,
        mean_length: f64,
        initial_bitrate: f64,
        distance: f64,
        name_folder: &str,
    ) -> Self {
        let server_id = 100 + pair_index as i32;
        let client_id = 200 + pair_index as i32;
        let server_ip = IpAddr::V4(Ipv4Addr::new(127, 0, pair_index as u8, 1));
        let client_ip = IpAddr::V4(Ipv4Addr::new(127, 0, pair_index as u8, 2));

        let server_coords = Coords::with_coords(1.0, 0.0, 0.0);
        let client_coords = Coords::with_coords(distance, 0.0, 0.0);

        let server_tx = frametransmission_delay(
            initial_bitrate * 1e6,
            MAX_AMPDU_SIZE,
            Coords::new(),
            server_coords,
            P_TX,
        );
        let client_tx = frametransmission_delay(
            initial_bitrate * 1e6,
            MAX_AMPDU_SIZE,
            Coords::new(),
            client_coords,
            P_TX,
        );

        let mut xr_server = XRServer::new(
            server_ip,
            client_ip,
            t0,
            INITIAL_FRAMERATE_FPS,
            initial_bitrate as f32,
            name_folder,
        );
        let mut xr_client = XRClient::new(client_ip, INITIAL_FRAMERATE_FPS);

        let mut sta_server = STA_extended::new(
            initial_bitrate * 1e6,
            mean_length,
            server_id,
            client_id,
            server_coords,
            true,
            mean_length / server_tx.service_delay,
            t0,
            false,
            0.0,
        );
        let mut sta_client = STA_extended::new(
            initial_bitrate * 1e6,
            mean_length,
            client_id,
            server_id,
            client_coords,
            true,
            mean_length / client_tx.service_delay,
            t0,
            false,
            0.0,
        );

        let mbox_xr_server = Mailbox::new();
        let mbox_xr_client = Mailbox::new();
        let mbox_sta_server = Mailbox::new();
        let mbox_sta_client = Mailbox::new();

        xr_server.outport_videoapp_network.connect(
            STA_extended::input_XR_app,
            &mbox_sta_server,
        );
        sta_server.to_app_socket.connect(
            XRServer::in_from_network,
            &mbox_xr_server,
        );
        xr_client.output_app_network.connect(
            STA_extended::input_XR_app,
            &mbox_sta_client,
        );
        sta_client.to_app_socket.connect(
            XRClient::in_from_network,
            &mbox_xr_client,
        );

        VRPair {
            xr_server,
            xr_client,
            sta_server,
            sta_client,
            mbox_xr_server,
            mbox_xr_client,
            mbox_sta_server,
            mbox_sta_client,
        }
    }
}

fn main_works() {
    env::set_var("RUST_BACKTRACE", "1");
    let args: Vec<String> = env::args().collect();
    if args.len() != 10 {
        eprintln!("Usage: {} <mean_length> <k_queue> <rate_bps> <rate_queue_bps> <distance> <bitrate> <PL_prob>", args[0]);
        return;
    }

    // Parse arguments
    let stoptime: f64 = args[1].parse().unwrap();
    let mean_length: f64 = args[2].parse().unwrap();
    let k_queue: usize = args[3].parse().unwrap();
    let rate_bps_in: f64 = args[4].parse().expect("Invalid rate_bps_in");
    let rate_queue_bps: f64 = args[5].parse().expect("Invalid rate_queue_bps");
    let distance: f64 = args[6].parse().unwrap();
    let initial_bitrate: f64 = args[7].parse().unwrap();
    let pl_prob: f64 = args[8].parse().unwrap();
    let n_xr: usize = args[9].parse().unwrap();


    // Create output directory
    let name_folder = format!(
        "sim_T{:.0}_Plen{:.0}_K{}_D{:.0}_Br{:.0}_PL{:.6}",
        stoptime, mean_length, k_queue, distance, initial_bitrate, pl_prob
    );
    let output_path = format!("Results/{}", name_folder);
    fs::create_dir_all(&output_path).expect("Failed to create directory");

    let t0 = MonotonicTime::EPOCH;
    let mut all_sta_ids = Vec::new();
    let mut vr_pairs = Vec::new();
    let mut xr_client_addresses = Vec::new();
    let mut xr_server_addresses = Vec::new();

    // Create XR pairs
    for i in 0..n_xr {
        let vr = VRPair::new(i, t0, mean_length, initial_bitrate, distance, &name_folder);
        all_sta_ids.push(100 + i as i32);
        all_sta_ids.push(200 + i as i32);
        xr_client_addresses.push(vr.mbox_xr_client.address());
        xr_server_addresses.push(vr.mbox_xr_server.address());
        vr_pairs.push(vr);
    }

    // Create and configure queue
    let mut queue = QueueModule::new(
        all_sta_ids.len(),
        k_queue.saturating_sub(1),
        pl_prob,
        all_sta_ids.clone(),
    );
    let mbox_queue = Mailbox::new();
    let queue_address = mbox_queue.address();
    let csv_data: Arc<Mutex<lib::CsvData>> = queue.csv_metrics.get_data_handle();
    let queue_stats = queue.get_queue_stats_handle();
    let sta_stats = queue.get_stas_stats_handle();

    // Connect all STAs to queue
    for vr in vr_pairs.iter_mut() {
        vr.sta_server.output_network_port.connect(QueueModule::input, &mbox_queue);
        vr.sta_client.output_network_port.connect(QueueModule::input_UL, &mbox_queue);
        queue.output_port_sta1.connect(STA_extended::input_wireless, &vr.mbox_sta_server);
        queue.output_port_sta1.connect(STA_extended::input_wireless, &vr.mbox_sta_client);
    }

    // Build simulation
    let mut sim_builder = SimInit::new()
        .add_model(queue, mbox_queue, "Queue")
        .add_model(SinkVideo_XR::new(), Mailbox::new(), "Video Sink");

    for (i, vr) in vr_pairs.into_iter().enumerate() {
        sim_builder = sim_builder
            .add_model(vr.xr_server, vr.mbox_xr_server, format!("XR Server {}", i))
            .add_model(vr.xr_client, vr.mbox_xr_client, format!("XR Client {}", i))
            .add_model(vr.sta_server, vr.mbox_sta_server, format!("STA Server {}", i))
            .add_model(vr.sta_client, vr.mbox_sta_client, format!("STA Client {}", i));
    }

    let mut simu = sim_builder.init(t0);
    let scheduler = simu.scheduler();

    // Schedule events
    for addr in &xr_client_addresses {
        scheduler.schedule_event(Duration::from_nanos(1), XRClient::configure_streams, (), addr).unwrap();
        scheduler.schedule_event(Duration::from_secs(10), XRClient::vsync, (), addr).unwrap();
    }

    for (i, addr) in xr_server_addresses.iter().enumerate() {
        let dest_ip = IpAddr::V4(Ipv4Addr::new(127, 0, i as u8, 2));
        scheduler.schedule_event(Duration::from_secs(10), XRServer::connection_pipeline, dest_ip, addr).unwrap();
    }

    scheduler.schedule_event(Duration::from_secs(10), QueueModule::self_scheduled_emu_queue_tx, (), &queue_address).unwrap();

    // Run simulation
    simu.step_by(Duration::from_secs_f64(stoptime));

    // Save results


    if let Ok(data) = csv_data.lock() {
        data.write_to_csv(&name_folder, &output_path).unwrap();
    }; 
    if let Ok(stats) = sta_stats.lock() {
        write_all_sta_csvs(&stats, &name_folder, &output_path).unwrap();
    }; 
    if let Ok(stats) = queue_stats.lock() {
        stats.print_nicely();
    }; 
}
fn main() {
    env::set_var("RUST_BACKTRACE", "1");
    let args: Vec<String> = env::args().collect();
    if args.len() != 11 {
        eprintln!("Usage: {} <stoptime> <mean_length> <k_queue> <rate_bps_in> <rate_queue_bps> <distance> <bitrate> <pl_prob> <n_xr> <n_bg>", args[0]);
        return;
    }

    // Parse arguments
    let stoptime: f64 = args[1].parse().unwrap();
    let mean_length: f64 = args[2].parse().unwrap();
    let k_queue: usize = args[3].parse().unwrap();
    let rate_bps_in: f64 = args[4].parse().expect("Invalid rate_bps_in");
    let rate_queue_bps: f64 = args[5].parse().expect("Invalid rate_queue_bps");
    let distance: f64 = args[6].parse().unwrap();
    let initial_bitrate: f64 = args[7].parse().unwrap();
    let pl_prob: f64 = args[8].parse().unwrap();
    let n_xr: usize = args[9].parse().unwrap();
    let n_bg: usize = args[10].parse().unwrap();  // New parameter for background STAs

    // Create output directory
    let name_folder = format!(
        "sim_T{:.0}_Plen{:.0}_K{}_D{:.0}_Br{:.0}_PL{:.6}_BG{}",
        stoptime, mean_length, k_queue, distance, initial_bitrate, pl_prob, n_bg
    );
    let output_path = format!("Results/{}", name_folder);
    fs::create_dir_all(&output_path).expect("Failed to create directory");

    let t0 = MonotonicTime::EPOCH;
    let mut all_sta_ids = Vec::new();
    let mut vr_pairs = Vec::new();
    let mut xr_client_addresses = Vec::new();
    let mut xr_server_addresses = Vec::new();
    let mut bg_sta_models = Vec::new();
    let mut bg_sta_mailboxes = Vec::new();
    let mut bg_sta_addresses = Vec::new();

    // Create XR pairs
    for i in 0..n_xr {
        let vr = VRPair::new(i, t0, mean_length, initial_bitrate, distance, &name_folder);
        all_sta_ids.push(100 + i as i32);
        all_sta_ids.push(200 + i as i32);
        xr_client_addresses.push(vr.mbox_xr_client.address());
        xr_server_addresses.push(vr.mbox_xr_server.address());
        vr_pairs.push(vr);
    }

    // Create background STAs
    for i in 0..n_bg {
        let sta_id = 300 + i as i32;
        let coords = Coords {
            x: distance,
            y: 0.0,
            z: 0.0,
        };
        
        let bg_sta = STA_extended::new(
            rate_bps_in,
            mean_length,
            sta_id,
            2,  // Default destination (AP)
            coords,
            true,
            rate_bps_in,  // Using input rate as effective rate for simplicity
            t0,
            true,
            rate_bps_in,  // Background traffic rate
        );

        let mbox_bg_sta = Mailbox::new();
        bg_sta_addresses.push(mbox_bg_sta.address());
        bg_sta_mailboxes.push(mbox_bg_sta);
        bg_sta_models.push(bg_sta);
        all_sta_ids.push(sta_id);
    }

    // Create and configure queue
    let mut queue = QueueModule::new(
        all_sta_ids.len(),
        k_queue.saturating_sub(1),
        pl_prob,
        all_sta_ids.clone(),
    );
    let mbox_queue = Mailbox::new();
    let queue_address = mbox_queue.address();
    let csv_data: Arc<Mutex<lib::CsvData>> = queue.csv_metrics.get_data_handle();
    let queue_stats = queue.get_queue_stats_handle();
    let sta_stats = queue.get_stas_stats_handle();

    // Connect all STAs to queue
    for vr in vr_pairs.iter_mut() {
        vr.sta_server.output_network_port.connect(QueueModule::input, &mbox_queue);
        vr.sta_client.output_network_port.connect(QueueModule::input_UL, &mbox_queue);
        queue.output_port_sta1.connect(STA_extended::input_wireless, &vr.mbox_sta_server);
        queue.output_port_sta1.connect(STA_extended::input_wireless, &vr.mbox_sta_client);
    }

    // Connect background STAs to queue
    for bg_sta in bg_sta_models.iter_mut() {
        bg_sta.output_network_port.connect(QueueModule::input, &mbox_queue);
    }

    // Build simulation
    let mut sim_builder = SimInit::new()
        .add_model(queue, mbox_queue, "Queue")
        .add_model(SinkVideo_XR::new(), Mailbox::new(), "Video Sink");

    // Add XR pairs to simulation
    for (i, vr) in vr_pairs.into_iter().enumerate() {
        sim_builder = sim_builder
            .add_model(vr.xr_server, vr.mbox_xr_server, format!("XR Server {}", i))
            .add_model(vr.xr_client, vr.mbox_xr_client, format!("XR Client {}", i))
            .add_model(vr.sta_server, vr.mbox_sta_server, format!("STA Server {}", i))
            .add_model(vr.sta_client, vr.mbox_sta_client, format!("STA Client {}", i));
    }

    // Add background STAs to simulation
    for (i, (bg_sta, mbox)) in bg_sta_models.into_iter().zip(bg_sta_mailboxes).enumerate() {
        sim_builder = sim_builder.add_model(bg_sta, mbox, format!("BG STA {}", i));
    }

    let mut simu = sim_builder.init(t0);
    let scheduler = simu.scheduler();

    // Schedule XR events
    for addr in &xr_client_addresses {
        let epsilon = Duration::from_secs_f64(exponential(0.5));
        scheduler.schedule_event(Duration::from_nanos(1) + epsilon, XRClient::configure_streams, (), addr).unwrap();
        scheduler.schedule_event(Duration::from_secs(10) + epsilon, XRClient::vsync, (), addr).unwrap();
    }

    for (i, addr) in xr_server_addresses.iter().enumerate() {
        let epsilon = Duration::from_secs_f64(exponential(1.5));
        let dest_ip = IpAddr::V4(Ipv4Addr::new(127, 0, i as u8, 2));
        scheduler.schedule_event(Duration::from_secs(10) + epsilon, XRServer::connection_pipeline, dest_ip, addr).unwrap();
    }

    // Schedule background STA events
    for address in &bg_sta_addresses {
        let epsilon = Duration::from_secs_f64(exponential(0.1));
        scheduler.schedule_event(
            epsilon,
            STA_extended::send_packet_BG,
            (),
            address,
        ).unwrap();
    }

    scheduler.schedule_event(Duration::from_secs(10), QueueModule::self_scheduled_emu_queue_tx, (), &queue_address).unwrap();

    // Run simulation
    simu.step_by(Duration::from_secs_f64(stoptime));

    // Save results
    if let Ok(data) = csv_data.lock() {
        data.write_to_csv(&name_folder, &output_path).unwrap();
    };
    if let Ok(stats) = sta_stats.lock() {
        write_all_sta_csvs(&stats, &name_folder, &output_path).unwrap();
    };
    if let Ok(stats) = queue_stats.lock() {
        stats.print_nicely();
    };
}