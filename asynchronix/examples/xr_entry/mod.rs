// asynchronix/examples/xr_entry.rs
use anyhow::Result;
#[allow(unused)]
////////////////////////////////////// XR SIMULATOR ////////////////////////////
///
///     Mixing up connection.rs and bitratemanager to simplify the process of generating frames.
///     
use asynchronix::simulation::{Mailbox, Scheduler, SimInit};
use asynchronix::time::MonotonicTime;
use tai_time::TaiTime;
// use xkbcommon::xkb::Table;
// use crate::lib::models_XR::BitrateMode;

use rand::seq::SliceRandom;
// use futures_util::Stream;
// use lib::alvr_stream_socket::{Buffer, StreamReceiver};

// use tai_time::TaiTime;

use rand::{thread_rng, SeedableRng};
use rand::rngs::StdRng;

use rand::Rng;

// mod lib; // for calling m own local library

use crate::lib::models_mm1k::{EmulatedLink, QueueModule, MAX_EMULATED_QUEUE_PACKETS, NetworkPattern};
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

use crate::lib::models_XR::{NestVrProfile, STA_extended, XRClient, XRServer, BITRATE_UPDATE_INTERVAL};
use crate::lib::UPLINK_QUEUE_SIZE;

pub const SIM_START_TIME: u64 = 1;




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
        initial_bitrate_orig: f64,
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
        t_end_simu: f64, 
        simu_unique_str: &str, 

    ) -> Self {

        let mut initial_bitrate= initial_bitrate_orig; 

        let mut abr_choice; 
    
        // UNCOMMENT (when not evaluating RL in same scenario) . 
        // if matches!(abr_enabled, 3){  // ABR==3 -> ReinforcementLearner mode, First VR pair is RL, rest is random between CBR, Nest-VR and Everest. 

            if pair_index == 0{ 
                abr_choice = abr_enabled; 
                // do nothing, it's correct
            }
            else{
                // abr_choice = rng.gen_range(0..=2);  // generates 0, 1, or 2 (or 4 for GCC)
                let choices = [0, 1, 2, 4];
                let mut rng = thread_rng();
                abr_choice = *choices.choose(&mut rng).unwrap();


                if abr_choice == 0 { // CBR (RANDOM)
                    let values: Vec<u32> = (5..=25).step_by(5).collect(); // bounding to max CBR 25 Mbps in RL scenario
                    initial_bitrate = *values.choose(&mut rng).unwrap() as f64; 
                }                
            }
        // }
        // else{
            // abr_choice = abr_enabled; //makes all sessions have same ABR choice
        // }

        let bm_string = match abr_choice
            {
                0 => {"CBR"}
                1 => {"Nest-VR"}, 
                2 => {"EveRest"},
                3 => {"RL agent"},
                4 => {"GCC Port"}, 
                5 => {"NADA Port"}, 
                6 => {"FovOptix Port"}
                _ => {"???"}
            }; 

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
            abr_choice, 
            nest_vr_profile, 
            t_end_simu, 
            simu_unique_str, // for identifying each simulation on the RLConnector
        );

        let mut xr_client = XRClient::new(client_ip, fps, t0, name_folder, test, abr_choice, simu_unique_str, bm_string);

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


fn generate_session_timeline_basic(
    // rng: &mut R,
    sim_init_time: f64, 
    stoptime: f64,
) -> Vec<(f64, f64)> {

    let mut sessions = Vec::new();

    let start = sim_init_time;
    let end = stoptime;
    sessions.push((start, end));
    sessions
}

fn generate_session_timeline<R: Rng>(
    rng: &mut R,
    sim_init_time: f64, 
    stoptime: f64,
) -> Vec<(f64, f64)> {
    let start_time_pause = truncated_exponential_seconds(rng, 15.0, 8.0, 25.0);
    // let start_time_pause = sim_init_time; 

    let mut t = start_time_pause;
    let mut sessions = Vec::new();
    
    while t < stoptime {
        let dur = rng.gen_range(8.0..=20.0);

        let pause = truncated_exponential_seconds(rng, 10.0, 8.0, 15.0);

        let start = t;
        let end = (t + dur).min(stoptime);
        sessions.push((start, end));

        t += dur + pause;
    }

    sessions
}
#[derive(Clone, Debug)]
pub struct SimParams {
    pub stoptime: f64,
    pub mean_length_bg: f64,
    pub k_queue: usize,
    pub distance: f64,
    pub initial_bitrate: f64,   // Mbps
    pub pl_prob: f64,
    pub n_xr: usize,
    pub n_bg: usize,
    pub rate_bps_bg_in: f64,
    pub is_ul_bg_traffic: usize,
    pub test_type: String,      // "BW" | "JI" | "PL" | "STD" | "RANDOM"
    pub video_filename: String,
    pub fps: f32,
    pub n_close: usize,
    pub distance_close: f64,
    pub seed: u64,
    pub gop_size: usize,
    pub intra_refresh: usize,   // 0/1
    pub abr: usize,             // 0 CBR | 1 Nest-VR | 2 Everest | 3 RL | 4 GCC | 5 NADA | 6 FovOptix
    pub nest_vr_choice: usize,  // 0 Speedy | 1 Balanced | 2 Anxious
    pub test_distances_everest: usize, // 0/1
    pub sim_id: usize,
}

pub fn parse_cli_to_params(args: &[String]) -> SimParams {
    assert!(args.len() == 23, "unexpected number of args");
    SimParams {
        stoptime:               args[1].parse().unwrap(),
        mean_length_bg:         args[2].parse().unwrap(),
        k_queue:                args[3].parse().unwrap(),
        distance:               args[4].parse().unwrap(),
        initial_bitrate:        args[5].parse().unwrap(),
        pl_prob:                args[6].parse().unwrap(),
        n_xr:                   args[7].parse().unwrap(),
        n_bg:                   args[8].parse().unwrap(),
        rate_bps_bg_in:         args[9].parse().unwrap(),
        is_ul_bg_traffic:       args[10].parse().unwrap(),
        test_type:              args[11].clone(),
        video_filename:         args[12].clone(),
        fps:                    args[13].parse().unwrap(),
        n_close:                args[14].parse().unwrap(),
        distance_close:         args[15].parse().unwrap(),
        seed:                   args[16].parse().unwrap(),
        gop_size:               args[17].parse().unwrap(),
        intra_refresh:          args[18].parse().unwrap(),
        abr:                    args[19].parse().unwrap(),
        nest_vr_choice:         args[20].parse().unwrap(),
        test_distances_everest: args[21].parse().unwrap(),
        sim_id:                 args[22].parse().unwrap(),
    }
}

// ---- Pull the content of your current `main()` here ----
// Return Ok(()) on success; bubble up errors with anyhow.
pub fn run_sim(params: SimParams) -> Result<()> {

    env::set_var("RUST_BACKTRACE", "1");

    // println!("CUDA_VISIBLE_DEVICES={:?}", std::env::var("CUDA_VISIBLE_DEVICES"));

    // 1) Unpack everything (keeps names identical to your CLI version)
    let SimParams {
        stoptime,
        mean_length_bg,
        k_queue,
        distance,
        initial_bitrate,
        pl_prob,
        n_xr,
        n_bg,
        rate_bps_bg_in,
        is_ul_bg_traffic,
        test_type,
        video_filename,
        fps,
        n_close,
        distance_close,
        seed,
        gop_size,
        intra_refresh,
        abr,
        nest_vr_choice,
        test_distances_everest,
        sim_id,
    } = params;

    // 2) Handy deriveds (exactly like your main)
    let sim_unique_string = format!("Simu_{}", sim_id);
    let test_distances_everest_bool = test_distances_everest != 0;



    let sim_unique_string = format!("Simu_{}", sim_id);  
    
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
        crate::print_red!("Emulated patterns: \n{:#?}", emu_effects); 
    }

   


    for i in 0..n_close{ // to set up variable distance scenarios across users

        let first_vr_pair_distance: VRPair= VRPair::new(
            
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
            stoptime, 
            &sim_unique_string, 
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
            stoptime, 
            &sim_unique_string, 

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


        vr.sta_client.outport_coords_xrclient.connect(XRClient::input_coordinates_STA, &vr.mbox_xr_client.address()); 
        
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
        
        let mut sessions = if test_distances_everest_bool 
            {
                generate_session_timeline(&mut rng, init, stoptime)
            }
            else{
                generate_session_timeline_basic(init, stoptime)
        }; 

        // if i == 0 && abr == 3 {
        if i == 0 { // Make STA0 always be active, since it is used as the one for plots for a fair comparison.  
            sessions = generate_session_timeline_basic(init, stoptime); // Force the ReinforcementLearner to be active all across the simulation. 
        }
        // let sessions: Vec<(f64, f64)> = generate_session_timeline(&mut rng, init, stoptime); // Each VR Session gets its own scheduling in the simulation

        crate::print_magenta!("ALL SESSIONS FOR CLIENT {} : {:#?}", i ,sessions); 
        let mut sessions = sessions;
        sessions.sort_by(|a,b| a.0.partial_cmp(&b.0).unwrap());

        let mut has_scheduled_vsync: bool = false; 

        for (idx, (start, end)) in sessions.iter().copied().enumerate() {
            // ---- Schedule client start ----
            scheduler.schedule_event(
                Duration::from_secs_f64(start),
                XRClient::configure_streams,
                packet_size,
                addr_client,
            ).unwrap();

            // print_red!("Scheduling VSYNC at {}", start);

            if !has_scheduled_vsync{
                scheduler.schedule_event(  // ALREADY SCHEDULED BY session_reboot at start/end, do not schedule twice!! 
                    Duration::from_secs_f64(start),
                    XRClient::vsync,
                    (),
                    addr_client,
                ).unwrap();
                has_scheduled_vsync = true; // SCHEDULE VSYNC ONCE AND ONLY ONCE PER CLIENT.
            }
         

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
    // Use saved addresses for movement, only in Client STAs! Will also share coords messages with connected XRClient
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

    // Schedule background STA events
    for address in &bg_sta_addresses {
        let epsilon = Duration::from_secs_f64(exponential(0.1, &mut rng));
        scheduler
            .schedule_event(epsilon, STA_extended::send_packet_BG, (), address)
            .unwrap();
    }
    // Run simulation
    simu.step_by(Duration::from_secs_f64(stoptime));

    // let step_interval = Duration::from_secs_f64(BITRATE_UPDATE_INTERVAL); 
    // let mut t = Duration::ZERO; 
    // while t < Duration::from_secs_f64(stoptime)
    // {
        

    //     simu.step_by(step_interval);
        
    //     for pair in vr_pairs{
    //         if let BitrateMode::ReinforcementLearner { connector, ..} = pair.xr_server.bitrate_manager.bitrate_mode{
    //             let obs = pair.xr_server.bitrate_manager.build_rl_observation(simu.time()); 
    //             let reward = pair.xr_server.bitrate_manager.rl_reward_function(&obs);

    //             let done = t >= Duration::from_secs_f64(stoptime); 

    //             connector.lock().unwrap().select_action(&obs); 
    //         }
    //     }
        
        
    //     t += step_interval; 
    // }

    if let Ok(stats) = queue_stats.lock() {
        stats.print_nicely();
    };
    Ok(())
}

