#[allow(unused_imports)]
#[allow(dead_code)]
#[allow(unused)]
////////////////////////////////////// XR SIMULATOR ////////////////////////////
///
///     Mixing up connection.rs and bitratemanager to simplify the process of generating frames.
///     

use asynchronix::simulation::{Mailbox, Scheduler, SimInit};
use asynchronix::time::MonotonicTime;
use lib::models_mm1k::NetworkPattern;

// use futures_util::Stream;
// use lib::alvr_stream_socket::{Buffer, StreamReceiver};

// use tai_time::TaiTime;

use rand::{thread_rng, SeedableRng};
use rand::rngs::StdRng;

use rand::Rng;

mod lib; // for calling m own local library

use crate::lib::models_mm1k::{EmulatedLink, QueueModule, MAX_EMULATED_QUEUE_PACKETS};
use crate::lib::{
    exponential,
    PREFIX_ID_DOWNLINK, PREFIX_ID_UPLINK, PREFIX_ID_BG, 
    // frametransmission_delay,
    // AmpduPacket,
    Coords,
    DebugColor,
    //   MpduPacket, SlidingWindowAverage,
    // MAX_AMPDU_SIZE,
    // P_TX,
};
use std::fs;

use std::env;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use crate::lib::models_XR::{NestVrProfile, STA_extended, XRClient, XRServer};
use crate::lib::UPLINK_QUEUE_SIZE;

pub const SIM_START_TIME: u64 = 10;



struct VRPair {
    xr_server: XRServer,
    xr_client: XRClient,
    sta_server: STA_extended,
    sta_client: STA_extended,
    emu_link: EmulatedLink, 
    mbox_xr_server:  Mailbox<XRServer>,
    mbox_xr_client:  Mailbox<XRClient>,
    mbox_sta_server: Mailbox<STA_extended>,
    mbox_sta_client: Mailbox<STA_extended>,
    mbox_emu_link:  Mailbox<EmulatedLink>, 
}



const ROOM_W: f64 = 24.0;
const ROOM_H: f64 = 12.0;

/// Access Point at room center (if you need the coords elsewhere)
pub const AP_X: f64 = ROOM_W / 2.0;
pub const AP_Y: f64 = ROOM_H / 2.0;

/// Draw a uniform random starting point inside the room.
fn random_room_coords<R: Rng>(rng: &mut R) -> Coords {
    let x = rng.gen_range(0.0..ROOM_W);
    let y = rng.gen_range(0.0..ROOM_H);
    Coords::with_coords(x, y, 0.0)
}

impl VRPair {
    fn new(
        pair_index: usize,
        t0: MonotonicTime,
        // mean_length_BG: f64,
        initial_bitrate: f64,
        distance: f64,
        name_folder: &str,
        test: &str,
        patterns: &[NetworkPattern], 
        file_name_video: &str, 
        fps: f32, 
        gop_size: usize, 
        intrarefresh: bool, 
        abr_enabled: usize, 
        nest_vr_profile: &NestVrProfile, 
        netem_values_tests: Option<(bool,bool,bool,bool)>,
        test_distances_everest_bool: bool, 


    ) -> Self {
        let server_id = PREFIX_ID_DOWNLINK + pair_index as i32;
        let client_id = PREFIX_ID_UPLINK + pair_index as i32;
        let server_ip = IpAddr::V4(Ipv4Addr::new(127, 0, pair_index as u8, 1));
        let client_ip = IpAddr::V4(Ipv4Addr::new(127, 0, pair_index as u8, 2));

        let server_coords = Coords::with_coords(AP_X, AP_Y, 0.0);
        let mut client_coords = Coords::with_coords(distance, 0.0, 0.0);
        if !test_distances_everest_bool{
        }
        else{
            
            let mut rng = thread_rng(); 
            client_coords = random_room_coords(&mut rng); 
        }

        let mut xr_server = XRServer::new(
            server_ip,
            client_ip,
            t0,
            fps,
            initial_bitrate as f32,
            name_folder,
            patterns, 
            file_name_video, 
            gop_size, 
            intrarefresh, 
            abr_enabled, 
            nest_vr_profile, 
        );

        let everest_enabled = if abr_enabled == 2 { true } else {false}; 

        let mut xr_client = XRClient::new(client_ip, fps, t0, name_folder, test, everest_enabled);

        let mut sta_server = STA_extended::new(
            // initial_bitrate * 1e6,
            0.,
            server_id,
            client_id,
            server_coords,
            true,
            t0,
            false,
            0.0,
        );
        let mut sta_client = STA_extended::new(
            // initial_bitrate * 1e6,
            0.,
            client_id,
            server_id,
            client_coords,
            true,
            t0,
            false,
            0.0,
        );

        let mut emu_link = EmulatedLink::new(MAX_EMULATED_QUEUE_PACKETS, t0, netem_values_tests); 

        let mbox_xr_server = Mailbox::new();
        let mbox_xr_client = Mailbox::new();
        let mbox_sta_server = Mailbox::new();
        let mbox_sta_client = Mailbox::new();

        let mbox_emu_link = Mailbox::new(); 

        // xr_server
        //     .outport_videoapp_network
        //     .connect(STA_extended::input_XR_app, &mbox_sta_server);

        xr_server.outport_videoapp_network.connect(EmulatedLink::input, &mbox_emu_link); // Add the netem module in the middle. 
        emu_link.output.connect(STA_extended::input_XR_app, &mbox_sta_server) ; 

        xr_client
            .outport_tracking_network
            .connect(STA_extended::input_XR_app, &mbox_sta_client);

        sta_server
            .to_app_socket
            .connect(XRServer::in_from_network, &mbox_xr_server);
        xr_client
            .output_app_network
            .connect(STA_extended::input_XR_app, &mbox_sta_client);
        
        sta_client
            .to_app_socket
            .connect(XRClient::in_from_network, &mbox_xr_client);

        VRPair {
            xr_server,
            xr_client,
            sta_server,
            sta_client,
            emu_link, 
            mbox_xr_server,
            mbox_xr_client,
            mbox_sta_server,
            mbox_sta_client,
            mbox_emu_link, 
        }
    }
}


/// Truncated exponential sampler with mean `mean` before truncation and hard bounds [a,b].
/// We adjust lambda to match the target mean approximately after truncation.
fn truncated_exponential_seconds<R: Rng>(rng: &mut R, mean: f64, a: f64, b: f64) -> f64 {
    // Guard rails
    let a = a.max(0.0);
    let b = b.max(a + 1e-6);
    // Simple fixed-point refinement for λ so E[X|a<=X<=b]≈mean (good enough here).
    let mut lambda = 1.0 / mean.max(1e-6);
    for _ in 0..6 {
        let ea = (-lambda * a).exp();
        let eb = (-lambda * b).exp();
        let z  = ea - eb;
        // E[X | a<=X<=b] for Exp(λ) truncated to [a,b]
        let ex_trunc = (1.0 / lambda) + (a * ea - b * eb) / z;
        lambda *= ex_trunc / mean;
    }
    // Inverse CDF for truncated exp
    let u: f64 = rng.gen();
    let ea = (-lambda * a).exp();
    let eb = (-lambda * b).exp();
    let x = - ( (u * (eb - ea) + ea).ln() ) / lambda;
    x.clamp(a, b)
}

fn generate_session_timeline<R: Rng>(
    rng: &mut R,
    sim_init_time: f64, 
    stoptime: f64,
) -> Vec<(f64, f64)> {
    let start_time_pause = truncated_exponential_seconds(rng, 30.0, 10.0, 60.0);
    // let start_time_pause = sim_init_time; 

    let mut t = start_time_pause;
    let mut sessions = Vec::new();
    
    while t < stoptime {
        let dur = rng.gen_range(90.0..=110.0);

        let pause = truncated_exponential_seconds(rng, 30.0, 10.0, 60.0);

        let start = t;
        let end = (t + dur).min(stoptime);
        sessions.push((start, end));

        t += dur + pause;
    }

    sessions
}

fn main() {
    env::set_var("RUST_BACKTRACE", "1");

    // println!("CUDA_VISIBLE_DEVICES={:?}", std::env::var("CUDA_VISIBLE_DEVICES"));

    let args: Vec<String> = env::args().collect();
    if args.len() != 22 {
        eprintln!("Usage: {} <stoptime> <mean_length_BG> <k_queue>
        <distance> <bitrate> <pl_prob> <n_xr> <n_bg> <rate_bps_BG> <IS_UL> <test_type> <video_filename> <FPS> <N_close_users> <distance_close_users> <seed> <GoP_size> <Intra-refresh enabled> <ABR enabled> <nest-vr_profile> <Coords_everest_movement_test>", args[0]);
        return;
    }

    // Parse arguments
    let stoptime: f64           =       args[1].parse().unwrap();
    let mean_length_bg: f64     =       args[2].parse().unwrap();
    let k_queue: usize          =       args[3].parse().unwrap();
    let distance: f64           =       args[4].parse().expect("Invalid distance");
    let initial_bitrate: f64    =       args[5].parse().expect("Invalid bitrate");
    let pl_prob: f64            =       args[6].parse().expect("Invalid PL");
    let n_xr: usize             =       args[7].parse().expect("Invalid N_xr");
    let n_bg: usize             =       args[8].parse().expect("Invalid N_bg"); // New parameter for background STAs
    let rate_bps_bg_in     =       args[9].parse().expect("Invalid BG arrival rate");
    let is_ul_bg_traffic: usize =       args[10].parse().expect("Invalid IS_UL");
    let test_type: String       =       args[11].parse().expect("Invalid emulated Test"); // New test type parameter
    let video_filename: String  =       args[12].parse().expect("Invalid video filename"); 
    let fps:f32                 =       args[13].parse().expect("Invalid FPS"); 
    let n_close: usize          =       args[14].parse().expect("Invalid N_close_users"); 
    let distance_close: f64     =       args[15].parse().expect("Invalid Distance_close_users"); 
    let seed: u64               =       args[16].parse().unwrap();
    let gop_size: usize         =       args[17].parse().expect("Invalid GoP size"); 
    let intra_refresh: usize    =       args[18].parse().expect("Invalid intra-refresh (0 or 1)"); 
    let abr: usize              =       args[19].parse().expect("Invalid ABR (0 or 1) "); 
    let nest_vr_choice     =       args[20].parse().expect("Invalid NeSt profile"); 
    let test_distances_everest: usize =        args[21].parse().expect("Invalid Coordinates option"); 
    
    let test_distances_everest_bool = test_distances_everest != 0;

    
    // Set test constants based on test_type parameter
    let (test_bandwidth, test_jitter, test_pl, test_random) = match test_type.as_str() {
        "BW" => (true, false, false, false),
        "JI" => (false, true, false, false),
        "PL" => (false, false, true, false),
        "RANDOM" => (false,false, false, true), 
        _ => (false, false, false, false), // Default/STD case
    };
    // Use the test type from parameter as suffix directly
    let suffix = if ["BW", "JI", "PL", "STD", "RANDOM"].contains(&test_type.as_str()) {
        test_type.as_str()
    } else {
        "STD" // Default suffix if invalid test type provided
    };
    let mut rng: StdRng = StdRng::seed_from_u64(seed);
    // let abr_bool = abr > 0; 

    let nest_vr_profile = match nest_vr_choice{
        0 => {NestVrProfile::Speedy},
        1 => {NestVrProfile::Balanced},
        2 => {NestVrProfile::Anxious},
        _ => {NestVrProfile::Balanced}, //default to balanced 
    }; 

    // Create output directory
    let name_folder = format!(
        "sim_T{:.0}_D{:.0}_Br{:.1}_PL{:.1}_NXR{:.0}_NBG{:.0}_UL{:.0}_{suffix}_{video_filename}_FPS{:.0}_Nclose{:.0}_dclose{:.1}_S{:.0}_GoP{:.0}_IR{:.0}_ABR{:.0}_nest{:.0}",
        stoptime, distance, initial_bitrate, pl_prob, n_xr, n_bg, is_ul_bg_traffic, fps, n_close, distance_close, seed, gop_size, intra_refresh, abr, nest_vr_choice, 
    );

    let output_path = format!("Results/{}", name_folder);
    fs::create_dir_all(&output_path).expect("Failed to create directory");

    let t0 = MonotonicTime::EPOCH;
    let mut vr_pairs: Vec<VRPair> = Vec::new();
    let mut xr_client_addresses = Vec::new();
    let mut xr_server_addresses = Vec::new();
    let mut bg_sta_models = Vec::new();
    let mut bg_sta_mailboxes = Vec::new();
    let mut bg_sta_addresses = Vec::new();

    let mut emu_addresses = Vec::new();     

    let mut all_sta_ids = Vec::new();
   
    for i in 0..n_close {
        all_sta_ids.push( PREFIX_ID_DOWNLINK + i as i32);
        all_sta_ids.push( PREFIX_ID_UPLINK + i as i32);
    }
    for j in n_close..n_xr {
        all_sta_ids.push( PREFIX_ID_DOWNLINK + j as i32);
        all_sta_ids.push( PREFIX_ID_UPLINK + j as i32);
    }
        // 2) Gather all of the BG STA IDs
    for i in 0..n_bg {
        all_sta_ids.push( PREFIX_ID_BG + i as i32);
    }


    // Create and configure queue
    let mut queue = QueueModule::new(
        all_sta_ids.len(),
        k_queue.saturating_sub(1),
        pl_prob,
        all_sta_ids.clone(),
        name_folder.clone(),
        UPLINK_QUEUE_SIZE,
        Some((test_bandwidth, test_jitter, test_pl, test_random)),
    );
    let mbox_queue = Mailbox::new();
    let _queue_address = mbox_queue.address();

    // let csv_data: Arc<Mutex<lib::CsvData>> = queue.csv_metrics.get_data_handle();
    let queue_stats = queue.get_queue_stats_handle();
    // let sta_stats = queue.get_stas_stats_handle();
    // print_red!("EMU EFFECTS HERE", );

    assert!(n_close <= n_xr, "n_close_users must not exceed total XR users");


    let scratch_link = EmulatedLink::new(MAX_EMULATED_QUEUE_PACKETS, t0, Some((test_bandwidth, test_jitter, test_pl, test_random)));
    let emu_effects: Vec<NetworkPattern> = scratch_link.get_network_patterns().to_vec();


    if !emu_effects.is_empty(){
        print_red!("Emulated patterns: \n{:#?}", emu_effects); 
    }

    for i in 0..n_close{ // to set up variable distance scenarios across users

        let first_vr_pair_distance= VRPair::new(
            
            i,
            t0,
            // mean_length_BG,
            initial_bitrate,
            distance_close,
            &name_folder,
            suffix,
            &emu_effects, 
            &video_filename, 
            fps,
            gop_size, 
            intra_refresh != 0, 
            abr, 
            &nest_vr_profile, 
            Some((test_bandwidth, test_jitter, test_pl, test_random)),
            test_distances_everest_bool, 
        ); 
        // all_sta_ids.push(100 + i as i32);
        // all_sta_ids.push(200 + i as i32);
        xr_client_addresses.push(first_vr_pair_distance.mbox_xr_client.address());
        xr_server_addresses.push(first_vr_pair_distance.mbox_xr_server.address());
        
        emu_addresses.push(first_vr_pair_distance.mbox_emu_link.address()); 
        vr_pairs.push(first_vr_pair_distance);
    }    

 
    // Create extra XR pairs
    for i in n_close..n_xr {
        // let emu_effects: &[lib::models_mm1k::NetworkPattern] = vr_pairs[0].emu_link.get_network_patterns(); 

        let vr = VRPair::new(
            i,
            t0,
            // mean_length_BG,
            initial_bitrate,
            distance,
            &name_folder,
            suffix,
            &emu_effects, 
            &video_filename, 
            fps, 
            gop_size, 
            intra_refresh != 0 , 
            abr, 
            &nest_vr_profile,
            Some((test_bandwidth, test_jitter, test_pl, test_random)),
            test_distances_everest_bool, 

        );
        // all_sta_ids.push(100 + i as i32);
        // all_sta_ids.push(200 + i as i32);
        xr_client_addresses.push(vr.mbox_xr_client.address());
        xr_server_addresses.push(vr.mbox_xr_server.address());
        vr_pairs.push(vr);
    }

    let mut sta_client_addrs = Vec::new();
    for vr in &vr_pairs{
        sta_client_addrs.push(vr.mbox_sta_client.address()); 
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
            // rate_bps_in,
            mean_length_bg,
            sta_id,
            2, // Default destination (AP)
            coords,
            true,
            t0,
            true,
            rate_bps_bg_in, // Background traffic rate
        );

        let mbox_bg_sta = Mailbox::new();
        bg_sta_addresses.push(mbox_bg_sta.address());
        bg_sta_mailboxes.push(mbox_bg_sta);
        bg_sta_models.push(bg_sta);
        // all_sta_ids.push(sta_id);
    }
    

    // Connect all STAs to queue
    for vr in vr_pairs.iter_mut() {

        queue.STA_coords_map.insert(
            vr.sta_server.sta_id as usize,
            vr.sta_server.sta_coordinates.clone(),
        );
        queue.STA_coords_map.insert(
            vr.sta_client.sta_id as usize,
            vr.sta_client.sta_coordinates.clone(),
        );

        vr.sta_server
            .output_network_port
            .connect(QueueModule::input, &mbox_queue);

        vr.sta_client
            .output_network_port
            .connect(QueueModule::input_UL, &mbox_queue);
        
        vr.xr_server.output_perfect_information_bitrate
                .connect(XRClient::input_perfect_information_bitrate, &vr.mbox_xr_client.address()); 

        
        queue
            .output_port_sta1
            .connect(STA_extended::input_wireless, &vr.mbox_sta_server);
        queue
            .output_port_sta1
            .connect(STA_extended::input_wireless, &vr.mbox_sta_client);
    }

    // Connect background STAs to queue
    for bg_sta in bg_sta_models.iter_mut() {
        bg_sta
            .output_network_port
            .connect(QueueModule::input, &mbox_queue);

        queue.STA_coords_map.insert(
            bg_sta.sta_id as usize,
            bg_sta.sta_coordinates.clone(),
        );

    }

    // Build simulation
    let mut sim_builder = SimInit::new().add_model(queue, mbox_queue, "Queue");

    // Add XR pairs to simulation
    for (i, vr) in vr_pairs.into_iter().enumerate() {
        sim_builder = sim_builder
            .add_model(vr.xr_server, vr.mbox_xr_server, format!("XR Server {}", i))
            .add_model(vr.emu_link, vr.mbox_emu_link, format!("EmuLink {}", i))
            .add_model(vr.xr_client, vr.mbox_xr_client, format!("XR Client {}", i))
            .add_model(
                vr.sta_server,
                vr.mbox_sta_server,
                format!("STA Server {}", i),
            )
            .add_model(
                vr.sta_client,
                vr.mbox_sta_client,
                format!("STA Client {}", i),
            );
    }  

    // Add background STAs to simulation
    for (i, (bg_sta, mbox)) in bg_sta_models.into_iter().zip(bg_sta_mailboxes).enumerate() {
        sim_builder = sim_builder.add_model(bg_sta, mbox, format!("BG STA {}", i));
    }

    let mut simu = sim_builder.init(t0);
    let scheduler = simu.scheduler();

    let packet_size = 1400;

    // Schedule XR events
    for addr in &emu_addresses {

        scheduler.schedule_event(
                Duration::from_secs(SIM_START_TIME),
                EmulatedLink::flush_queue, 
                (),
                addr,
        ).unwrap(); 
    }

    for (i, (addr_client, addr_server)) in xr_client_addresses.iter().zip(&xr_server_addresses).enumerate() {
        let init: f64 = SIM_START_TIME as f64; 
        let sessions: Vec<(f64, f64)> = generate_session_timeline(&mut rng, init, stoptime); // Each VR Session gets its own scheduling in the simulation

        print_magenta!("ALL SESSIONS FOR CLIENT {} : {:#?}", i ,sessions); 
        let mut sessions = sessions;
        sessions.sort_by(|a,b| a.0.partial_cmp(&b.0).unwrap());

        for (idx, (start, end)) in sessions.iter().copied().enumerate() {
            // ---- Schedule client start ----
            scheduler.schedule_event(
                Duration::from_secs_f64(start),
                XRClient::configure_streams,
                packet_size,
                addr_client,
            ).unwrap();

            scheduler.schedule_event(
                Duration::from_secs_f64(start),
                XRClient::vsync,
                (),
                addr_client,
            ).unwrap();

            // ---- Look ahead to compute the reboot pause AFTER this session ----
            let pause_after = if let Some((next_start, _next_end)) = sessions.get(idx + 1) {
                let gap = next_start - end;
                if gap < 0.0 {
                    eprintln!("[warn] Overlapping sessions: end {:.3} > next start {:.3}, clamping gap to 0.", end, next_start);
                    0.0
                } else {
                    gap
                }
            } else {
                // Last session: choose what “pause” means.
                // Use 0.0 if your session_end handler doesn’t need trailing idle,
                // or stoptime - end if you want the final idle time as the pause.
                // 0.0 is safest:
                0.0
                // or: (stoptime - end).max(0.0)
            };

            // println!("START AND END: {} and {} -> PauseAfter: {}", start, end, pause_after);

                // ---- Schedule client end with the correct pause_after ----
                scheduler.schedule_event(
                    Duration::from_secs_f64(end),
                    XRClient::session_end,
                    pause_after,
                    addr_client,
                ).unwrap();

                // ---- Server: start and end mirroring client ----
                let dest_ip = IpAddr::V4(Ipv4Addr::new(127, 0, idx as u8, 2));
                scheduler.schedule_event(
                    Duration::from_secs_f64(start),
                    XRServer::connection_pipeline,
                    dest_ip,
                    addr_server,
                ).unwrap();

                scheduler.schedule_event(
                    Duration::from_secs_f64(end),
                    XRServer::session_end,
                    pause_after,
                    addr_server,
                ).unwrap();
            }
       

    }    
    // Use saved addresses for movement
    if test_distances_everest_bool{
        for sta_client_addr in sta_client_addrs.iter() {
            scheduler.schedule_event(
                Duration::from_secs(SIM_START_TIME),
                STA_extended::move_coordinates_everest,
                (),
                sta_client_addr,
            ).unwrap();
        }
    }
   
    // for (i, vr) in vr_pairs.into_iter().enumerate() {
    //     for vr_sta_client in vr.sta_client{
    //         scheduler.schedule_event(
    //             Duration::from_secs(SIM_START_TIME),
    //             STA_extended::move_coordinates_everest, 
    //             (), 
    //             addr_client, 
    //         ).unwrap(); 


    //     }

    // }

    // Schedule background STA events
    for address in &bg_sta_addresses {
        let epsilon = Duration::from_secs_f64(exponential(0.1, &mut rng));
        scheduler
            .schedule_event(epsilon, STA_extended::send_packet_BG, (), address)
            .unwrap();
    }
    // Run simulation
    simu.step_by(Duration::from_secs_f64(stoptime));

    if let Ok(stats) = queue_stats.lock() {
        stats.print_nicely();
    };
}
