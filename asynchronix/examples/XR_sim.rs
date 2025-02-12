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
use ffmpeg_next::packet::packet;
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
        let mut xr_client = XRClient::new(client_ip, INITIAL_FRAMERATE_FPS, t0);

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

        xr_client.outport_tracking_network.connect(
            STA_extended::input_XR_app, 
            &mbox_sta_client, 
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

fn main() {
    env::set_var("RUST_BACKTRACE", "1");
    let args: Vec<String> = env::args().collect();
    if args.len() != 12 {
        eprintln!("Usage: {} <stoptime> <mean_length> <k_queue> <rate_bps_in> <rate_queue_bps> <distance> <bitrate> <pl_prob> <n_xr> <n_bg> <IS_UL>", args[0]);
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

    let is_ul_bg_traffic: usize = args[11].parse().unwrap(); 

    let is_ul: bool = is_ul_bg_traffic == 1;

    // Create output directory
    let name_folder = format!(
        "sim_T{:.0}_D{:.0}_Br{:.0}_PL{:.3}_NXR{:.0}_NBG{:.0}_UL{:.0}",
        stoptime, distance, initial_bitrate, pl_prob, n_xr ,n_bg, is_ul_bg_traffic, 
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

    // Create background STAs   TODO: SEPARATE UL/DL TRAFFIC for BG STAs!!!!
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
        name_folder, 
        
    );
    let mbox_queue = Mailbox::new();
    let queue_address = mbox_queue.address();

    // let csv_data: Arc<Mutex<lib::CsvData>> = queue.csv_metrics.get_data_handle();
    let queue_stats = queue.get_queue_stats_handle();
    // let sta_stats = queue.get_stas_stats_handle();

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

    let packet_size = 1400; 


    // Schedule XR events
    for addr in &xr_client_addresses {
        // let epsilon = Duration::from_secs_f64(exponential(0.5));
        let epsilon = Duration::from_secs_f64(0.01); 
        scheduler.schedule_event(Duration::from_secs(10) + epsilon, XRClient::configure_streams, packet_size, addr).unwrap(); // Why pass packet_size? -> compiler complains if no other arg is found when context is needed:) 
        scheduler.schedule_event(Duration::from_secs(10) + epsilon, XRClient::vsync, (), addr).unwrap();
    }

    for (i, addr) in xr_server_addresses.iter().enumerate() {
        let epsilon = Duration::from_secs_f64(exponential(0.3));
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
    // if let Ok(data) = csv_data.lock() {
    //     if let Ok(data2) = data.write_to_csv(&name_folder, &output_path){
    //         println!("CSV data saved correctly!!");
    //     }
    //     else{
    //         println!("ERROOOOOOOOR SAVING CSV DATA!! ! ! ! \n\n"); 
    //     }

    // };
    // if let Ok(stats) = sta_stats.lock() {
    //     write_all_sta_csvs(&stats, &name_folder, &output_path).unwrap();
    // };
    if let Ok(stats) = queue_stats.lock() {
        stats.print_nicely();
    };
}