use crate::lib::alvr_stream_socket::{ALVR_ORIGINAL_SOCKETRX_BEHAVIOR, VideoCodec};
// asynchronix/examples/xr_entry.rs
use crate::lib::models_mm1k::{
    EmulatedLink, NetworkPattern, QueueModule, MAX_EMULATED_QUEUE_PACKETS, VizEvent, 
};

use crate::lib::models_mm1k::{LinkSelectionStrategy, StaCapabilities};
use crate::lib::{
    exponential,
    models_mm1k::{AP_X, AP_Y, ROOM_H, ROOM_W},
    //   MpduPacket, SlidingWindowAverage,
    // MAX_AMPDU_SIZE,
    // P_TX,
    // frametransmission_delay,
    // AmpduPacket,
    Coords,
    DebugColor,
    PREFIX_ID_DOWNLINK,
    PREFIX_ID_UPLINK,
};
use anyhow::Result;
#[allow(unused)]
use asynchronix::simulation::{Mailbox, Scheduler, SimInit};
use asynchronix::time::MonotonicTime;
// use rand::rngs::StdRng;
use crate::lib::models_XR::{NestVrProfile, ObservationConfig, STA_extended, XRClient, XRServer};
use rand::seq::SliceRandom;
use rand::thread_rng; //  SeedableRng};
use rand::Rng;
use std::env;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use std::{fs, u64};

pub const SIM_START_TIME: u64 = 1;
pub const PACKET_SIZE_SOCKETS_BYTES: usize = 1400;
pub const NUM_INPUT_ARGS_SIM: usize = 36;
pub const BANDWIDTH_EMU_LINK: u64 = 100E7 as u64; // 1 Gbps link


fn random_room_coords<R: Rng>(rng: &mut R) -> Coords {
    // Determine the total spread area (shorter by 1.5)
    let spread_w = ROOM_W / 1.5; 
    let spread_h = ROOM_H / 1.5; 
    
    // Calculate the min and max bounds centered on the AP
    let min_x = AP_X - (spread_w / 2.0);
    let max_x = AP_X + (spread_w / 2.0);
    
    let min_y = AP_Y - (spread_h / 2.0);
    let max_y = AP_Y + (spread_h / 2.0);

    // Generate random coordinates within those bounds
    let x = rng.gen_range(min_x..max_x);
    let y = rng.gen_range(min_y..max_y);
    
    Coords::with_coords(x, y, 0.0)
}

struct VRPair {
    xr_server: XRServer,
    xr_client: XRClient,
    sta_server: STA_extended,
    sta_client: STA_extended,
    emu_link: EmulatedLink,
    mbox_xr_server: Mailbox<XRServer>,
    mbox_xr_client: Mailbox<XRClient>,
    mbox_sta_server: Mailbox<STA_extended>,
    mbox_sta_client: Mailbox<STA_extended>,
    mbox_emu_link: Mailbox<EmulatedLink>,
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
        use_foveation: bool, 
        vbv_perframe: bool, 
        abr_enabled: usize,
        nest_vr_profile: &NestVrProfile,
        netem_values_tests: Option<(bool, bool, bool, bool)>,
        test_distances_everest_bool: bool,
        t_end_simu: f64,
        simu_unique_str: &str,
        obs_config: ObservationConfig,
        reward_mode: usize,
        t_update_abr: f32,
        ap_coords: Coords,
        edca_be_mode: bool,
        codec_selection: VideoCodec,
        results_path_name: &str, 
        random_seed: u64, 
        no_uplink_tracking_bool: bool, 
        deterministic_frame_sizes_bool: bool, 
    ) -> Self {
        let initial_bitrate = initial_bitrate_orig;

        println!(
            "[VR session {}] with abr_choice: {}",
            pair_index, abr_enabled
        );

        let bm_string = match abr_enabled {
            0 => "CBR",
            1 => "Nest-VR",
            2 => "EveRest",
            4 => "GCC Port",
            5 => "NADA Port",
            6 => "FovOptix Port",
            _ => "???",
        };

        let server_id = PREFIX_ID_DOWNLINK + pair_index as i32;
        let client_id = PREFIX_ID_UPLINK + pair_index as i32;
        let server_ip = IpAddr::V4(Ipv4Addr::new(127, 0, pair_index as u8, 1));
        let client_ip: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, pair_index as u8, 2));

        let server_coords: Coords = ap_coords.clone();

        let mut client_coords = Coords::with_coords(distance + AP_X, AP_Y, 0.0);
        
        if test_distances_everest_bool {
            let mut rng = thread_rng();
            client_coords = random_room_coords(&mut rng);
        } else {
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
            use_foveation, 
            vbv_perframe, 
            deterministic_frame_sizes_bool, 

            abr_enabled,
            nest_vr_profile,
            t_end_simu,
            simu_unique_str, // for identifying each simulation on the Connector
            obs_config,
            reward_mode,
            t_update_abr,
            PACKET_SIZE_SOCKETS_BYTES,
            edca_be_mode,
            codec_selection,
            results_path_name, 
        );

        let mut xr_client = XRClient::new(
            client_ip,
            fps,
            t0,
            name_folder,
            test,
            abr_enabled,
            simu_unique_str,
            bm_string,
            t_update_abr,
            PACKET_SIZE_SOCKETS_BYTES,
            edca_be_mode,
            codec_selection,
            results_path_name, 
            random_seed, 
            no_uplink_tracking_bool, 
        );

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
            0,
            ap_coords,
            random_seed, 
            test_distances_everest_bool
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
            0,
            ap_coords,
            random_seed, 
            test_distances_everest_bool

        );

        let mut emu_link = EmulatedLink::new_with_bandwidth(
            MAX_EMULATED_QUEUE_PACKETS,
            t0,
            netem_values_tests,
            server_ip,
            BANDWIDTH_EMU_LINK,
        );

        let mbox_xr_server = Mailbox::new();
        let mbox_xr_client = Mailbox::new();
        let mbox_sta_server = Mailbox::new();
        let mbox_sta_client = Mailbox::new();

        let mbox_emu_link = Mailbox::new();

        // xr_server
        //     .outport_videoapp_network
        //     .connect(STA_extended::input_XR_app, &mbox_sta_server);

        xr_server
            .outport_videoapp_network
            .connect(EmulatedLink::input, &mbox_emu_link); // Add the netem module in the middle.
        emu_link
            .output
            .connect(STA_extended::input_XR_app, &mbox_sta_server);

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
        let z = ea - eb;
        // E[X | a<=X<=b] for Exp(λ) truncated to [a,b]
        let ex_trunc = (1.0 / lambda) + (a * ea - b * eb) / z;
        lambda *= ex_trunc / mean;
    }
    // Inverse CDF for truncated exp
    let u: f64 = rng.gen();
    let ea = (-lambda * a).exp();
    let eb = (-lambda * b).exp();
    let x = -((u * (eb - ea) + ea).ln()) / lambda;
    x.clamp(a, b)
}

fn generate_session_timeline_basic(sim_init_time: f64, stoptime: f64) -> Vec<(f64, f64)> {
    let mut _rng = thread_rng();
    let mut sessions = Vec::new();

    // exponentially distributed start offset between 1 and 5 seconds
    
    let start_offset = truncated_exponential_seconds(&mut _rng, 2.5, 1.0, 5.0);
    // let start_offset = 0.0; 

    let start: f64 = sim_init_time + start_offset;
    let end = stoptime;

    sessions.push((start, end));
    sessions
}

const MIN_SESSION_DUR: f64 = 4.0;  // Reduced from 8.0
const MAX_SESSION_DUR: f64 = 12.0; // Reduced from 20.0
// Pause Timing Constants (Truncated Exponential)
const PAUSE_MEAN: f64 = 12.0;      // Increased from 10.0 to space them out
const PAUSE_MIN: f64 = 4.0;
const PAUSE_MAX: f64 = 25.0;

fn generate_session_timeline<R: Rng>(
    rng: &mut R,
    stoptime: f64,
) -> Vec<(f64, f64)> {
    let start_time_pause = truncated_exponential_seconds(rng, PAUSE_MIN, 3.0, 8.0);

    let mut t = start_time_pause;
    let mut sessions = Vec::new();

    while t < stoptime {
        // Using the new constants here
        let dur = rng.gen_range(MIN_SESSION_DUR..=MAX_SESSION_DUR);

        let pause = truncated_exponential_seconds(
            rng, 
            PAUSE_MEAN, 
            PAUSE_MIN, 
            PAUSE_MAX
        );

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
    pub initial_bitrate: f64, // Mbps
    pub pl_prob: f64,
    pub n_xr: usize,
    pub n_bg: usize,
    pub rate_bps_bg_in: f64,
    pub is_ul_bg_traffic: usize,
    pub test_type: String, // "BW" | "JI" | "PL" | "STD" | "RANDOM"
    pub video_filename: String,
    pub fps_arg: f32,
    pub n_close: usize,
    pub distance_close: f64,
    pub seed: u64,
    pub gop_size: usize,
    pub intra_refresh: usize,          // 0/1
    pub use_foveation: usize,          // 0/1
    pub vbv_per_frame: usize,          // 0/1, per-second if set to 0. 
    pub abr: usize, // 0 CBR | 1 Nest-VR | 2 Everest | 3 ?? | 4 GCC | 5 NADA | 6 FovOptix
    pub nest_vr_choice: usize, // 0 Speedy | 1 Balanced | 2 Anxious
    pub test_distances_everest: usize, // 0/1
    pub sim_id: usize,
    pub observation_type: usize,
    pub reward_mode: usize, // 0-> naive , 1-> normalized, 2-> ??? todo shaping.
    pub t_update_abr: f32,
    // pub eval_string: String, // to store name of eval run, used for benchmarking in parallel.
    pub mlo_channel_config: String,
    pub edca_be: usize,
    pub mlo_link_sel_policy: usize,
    pub packs_per_ampdu: usize,
    pub codec_input_arg: String,
    pub name_results_path: String, 
    pub no_uplink_tracking: usize, 
    pub deterministic_frame_sizes: usize, 
}

pub fn parse_cli_to_params(args: &[String]) -> SimParams {
    assert!(
        args.len() == NUM_INPUT_ARGS_SIM,
        "unexpected number of args"
    );
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
        fps_arg:                args[13].parse().unwrap(),
        n_close:                args[14].parse().unwrap(),
        distance_close:         args[15].parse().unwrap(),
        seed:                   args[16].parse().unwrap(),
        gop_size:               args[17].parse().unwrap(),
        intra_refresh:          args[18].parse().unwrap(),
        use_foveation:          args[19].parse().unwrap(), 
        vbv_per_frame:          args[20].parse().unwrap(), 
        abr:                    args[21].parse().unwrap(),
        nest_vr_choice:         args[22].parse().unwrap(),
        test_distances_everest: args[23].parse().unwrap(),
        sim_id:                 args[24].parse().unwrap(),
        observation_type:       args[25].parse().unwrap(),
        reward_mode:            args[26].parse().unwrap(),
        t_update_abr:           args[27].parse().unwrap(),
        mlo_channel_config:     args[28].parse().unwrap(),
        edca_be:                args[29].parse().unwrap(),
        mlo_link_sel_policy:    args[30].parse().unwrap(),
        packs_per_ampdu:        args[31].parse().unwrap(),
        codec_input_arg:        args[32].parse().unwrap(),
        name_results_path:      args[33].clone(),
        no_uplink_tracking:     args[34].parse().unwrap(), 
        deterministic_frame_sizes: args[35].parse().unwrap(), 
    }
}

// ---- Pull the content of your current `main()` here ----
// Return Ok(()) on success; bubble up errors with anyhow.
pub fn run_sim(params: SimParams) -> Result<()> {
    env::set_var("RUST_BACKTRACE", "1");

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
        fps_arg,
        n_close,
        distance_close,
        seed,
        gop_size,
        intra_refresh,
        use_foveation, 
        vbv_per_frame, 
        abr,
        nest_vr_choice,
        test_distances_everest,
        sim_id,
        observation_type,
        reward_mode,
        t_update_abr,
        // eval_string,
        mlo_channel_config,
        edca_be,
        mlo_link_sel_policy,
        packs_per_ampdu,
        codec_input_arg,
        name_results_path, 
        no_uplink_tracking, 
        deterministic_frame_sizes, 
    } = params;

    let start_sim_benchmark = std::time::Instant::now();
    let sim_unique_string = format!("Simu_{} | {codec_input_arg}", sim_id);
    let test_distances_everest_bool = test_distances_everest != 0;
    let edca_be_bool = edca_be != 0;
    let no_uplink_tracking_bool = no_uplink_tracking!=0; 
    let deterministic_frame_sizes_bool = deterministic_frame_sizes != 0; 

    // Set test constants based on test_type parameter
    let (test_bandwidth, test_jitter, test_pl, test_random) = match test_type.as_str() {
        "BW" => (true, false, false, false),
        "JI" => (false, true, false, false),
        "PL" => (false, false, true, false),
        "RANDOM" => (false, false, false, true),
        _ => (false, false, false, false), // Default/STD case
    };

    // Use the test type from parameter as suffix directly
    let suffix = if ["BW", "JI", "PL", "STD", "RANDOM"].contains(&test_type.as_str()) {
        test_type.as_str()
    } else {
        "STD" // Default suffix if invalid test type provided
    };
    // let mut rng: StdRng = StdRng::seed_from_u64(seed);
    // let abr_bool = abr > 0;

    let nest_vr_profile = match nest_vr_choice {
        0 => NestVrProfile::Speedy,
        1 => NestVrProfile::Balanced,
        2 => NestVrProfile::Anxious,
        _ => NestVrProfile::Balanced, //default to balanced
    };

    // pub const MLO_LINK_SELECTION_STRATEGY: LinkSelectionStrategy = LinkSelectionStrategy::LyapunovBackpressure;
    let mlo_policy = match mlo_link_sel_policy {
        0 => LinkSelectionStrategy::PrimaryFirst,
        1 => LinkSelectionStrategy::Opportunistic,
        2 => LinkSelectionStrategy::LyapunovBackpressure,
        _ => LinkSelectionStrategy::Opportunistic,
    };

    // Create output directory
   let name_folder = format!(
        "sim_T{:.0}_D{:.1}_Br{:.1}Mbps_FPS{:.0}_Codec{codec_input_arg}_GoP{:.0}_IR{:.0}_Foveate{:.0}_VBVframe{:.0}_macPL{:.1}_aggAMPDU={:.0}_NXR{:.0}_NBG{:.0}_BGLambda{:.0}_UL{:.0}_{suffix}_{video_filename}_Nclose{:.0}_dclose{:.1}_S{:.0}_ABR{:.0}_{mlo_channel_config}_EDCAbe{:.0}_{}_noTrack{:.0}_fibonacciVid{:.0}",
        stoptime, distance, initial_bitrate, fps_arg, gop_size, intra_refresh, use_foveation, vbv_per_frame, pl_prob, packs_per_ampdu, n_xr, n_bg, rate_bps_bg_in ,is_ul_bg_traffic,  n_close, distance_close, seed,abr, edca_be, mlo_policy.to_string(), no_uplink_tracking as usize, deterministic_frame_sizes, );
        
    let output_path = format!("{}/{}", name_results_path ,name_folder);
    fs::create_dir_all(&output_path).expect("Failed to create directory");

    let t0 = MonotonicTime::EPOCH;
    let ap_coords: Coords = Coords::with_coords(AP_X, AP_Y, 0.0);

    let mut vr_pairs: Vec<VRPair> = Vec::new();
    let mut xr_client_addresses = Vec::new();
    let mut xr_server_addresses = Vec::new();
    let mut bg_sta_models = Vec::new();
    let mut bg_sta_mailboxes = Vec::new();
    let mut bg_sta_addresses = Vec::new();

    let mut emu_addresses = Vec::new();

    let mut all_sta_ids = Vec::new();

    assert!(
        n_close <= 49 && n_xr <= 49 && n_bg <= 49,
        "CAN'T USE MORE THAN 50 STAs PER CATEGORY (or things break)"
    );
    for i in 0..n_close {
        all_sta_ids.push(PREFIX_ID_DOWNLINK + i as i32);
        all_sta_ids.push(PREFIX_ID_UPLINK + i as i32);
    }
    for j in n_close..n_xr {
        all_sta_ids.push(PREFIX_ID_DOWNLINK + j as i32);
        all_sta_ids.push(PREFIX_ID_UPLINK + j as i32);
    }
    // 2) Gather all of the BG STA IDs
    for i in 0..n_bg {
        all_sta_ids.push(PREFIX_ID_DOWNLINK + 50 + i as i32); // assume maximum of 50 BG STAs, hard limit of simulator.
        all_sta_ids.push(PREFIX_ID_UPLINK + 50 + i as i32); // assume maximum of 50 BG STAs, hard limit of simulator.
    }

    let link_configs = crate::lib::models_mm1k::create_mlo_config(&mlo_channel_config);

    let available_links: Vec<_> = link_configs.iter().map(|lc| lc.link_id).collect();

    let (viz_tx, viz_rx) = crossbeam::channel::unbounded::<VizEvent>();


    // Create and configure queue
    let mut queue = QueueModule::new(
        all_sta_ids.len(),
        k_queue.saturating_sub(1),
        pl_prob,
        all_sta_ids.clone(),
        name_folder.clone(),
        Some((test_bandwidth, test_jitter, test_pl, test_random)),
        link_configs,
        mlo_policy,
        packs_per_ampdu,
        &name_results_path, 
        Some(viz_tx.clone()), 
    );

    for sta_id in &all_sta_ids {
        // This ensures that in MLO0, everyone gets [0],
        // in MLO1, everyone gets [0, 1], etc. Todo: tri-band case.

        let capabilities = if *sta_id >= PREFIX_ID_UPLINK && *sta_id < PREFIX_ID_DOWNLINK {
            // Uplink STAs (clients)
            StaCapabilities {
                _is_str_capable: true,
                links: available_links.clone(), // <--- Dynamic
            }
        } else if *sta_id >= PREFIX_ID_DOWNLINK {
            // Downlink STAs (servers)
            StaCapabilities {
                _is_str_capable: true,
                links: available_links.clone(), // <--- Dynamic
            }
        } else {
            // Background traffic
            // NOTE: If BG traffic should ALWAYS be single link (even in MLO),
            // use vec![available_links[0]] instead.
            // Otherwise, use available_links.clone() to let them use whatever is open.
            StaCapabilities {
                _is_str_capable: true,
                links: available_links.clone(),
            }
        };

        queue.sta_capabilities.insert(*sta_id, capabilities);
    }

    let mbox_queue = Mailbox::new();
    let _queue_address = mbox_queue.address();

    // let csv_data: Arc<Mutex<lib::CsvData>> = queue.csv_metrics.get_data_handle();
    let queue_stats = queue.get_queue_stats_handle();
    // let sta_stats = queue.get_stas_stats_handle();
    // print_red!("EMU EFFECTS HERE", );

    assert!(
        n_close <= n_xr,
        "n_close_users must not exceed total XR users"
    );

    let localhost_v4 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let scratch_link = EmulatedLink::new_with_bandwidth(
        MAX_EMULATED_QUEUE_PACKETS,
        t0,
        Some((test_bandwidth, test_jitter, test_pl, test_random)),
        localhost_v4,
        BANDWIDTH_EMU_LINK,
    );

    let codec_selection = match codec_input_arg.as_str() {
        "AV1" => VideoCodec::AV1,
        "HEVC" => VideoCodec::HEVC,
        _ => {
            crate::print_red!("Unspecified codec WARNING! Default: HEVC",);
            VideoCodec::HEVC
        }
    };

    let emu_effects: Vec<NetworkPattern> = scratch_link.get_network_patterns().to_vec();

    let obs_config = match observation_type {
        0 => ObservationConfig::Raw,
        1 => ObservationConfig::ManualScaledV1,
        2 => ObservationConfig::RunningAvg,
        _ => ObservationConfig::ManualScaledV1,
    };

    let mut rng = rand::thread_rng();

    for i in 0..n_xr {
        // Determine the distance based on the index
        let current_distance = if i < n_close {
            distance_close
        } else {
            distance
        };

        let current_fps = fps_arg; 
       


        let current_abr_mode = abr;
        let mut bitrate_choice = initial_bitrate;
    

        let mut current_t_update_abr = t_update_abr; 
        if current_abr_mode == 2 {
                current_t_update_abr = 1.0 / fps_arg; // Make Everest have an update per each frame. 
            }
        
        println!("[VR session {}] Final: {}, ABR_duration: {:.3}" , i, current_abr_mode, current_t_update_abr);

        let vr = VRPair::new(
            i,
            t0,
            bitrate_choice,
            current_distance, // Use the conditional distance here
            &name_folder,
            suffix,
            &emu_effects,
            &video_filename,
            current_fps,
            gop_size,
            intra_refresh != 0,
            use_foveation!= 0, 
            vbv_per_frame != 0, 
            current_abr_mode,
            &nest_vr_profile,
            Some((test_bandwidth, test_jitter, test_pl, test_random)),
            test_distances_everest_bool,
            stoptime,
            &sim_unique_string,
            obs_config,
            reward_mode,
            current_t_update_abr,
            ap_coords,
            edca_be_bool,
            codec_selection,
            &name_results_path, 
            seed, 
            no_uplink_tracking_bool, 
            deterministic_frame_sizes_bool, 

        );

        // Common pushes for all users
        xr_client_addresses.push(vr.mbox_xr_client.address());
        xr_server_addresses.push(vr.mbox_xr_server.address());

        // Only push to emu_addresses for the "close" users
        if i < n_close {
            emu_addresses.push(vr.mbox_emu_link.address());
        }

        vr_pairs.push(vr);
    }
    let mut sta_client_addrs = Vec::new();
    for vr in &vr_pairs {
        sta_client_addrs.push(vr.mbox_sta_client.address());
    }

    // Create background STAs   TODO: SEPARATE UL/DL TRAFFIC for BG STAs!!!!
    for i in 0..n_bg {
        let coords = Coords {
            x: distance + AP_X, // relative to AP location
            y: 0.0 + AP_Y,      // also relative to AP location
            z: 0.0,             // unused
        };
        let sta_id_dl = PREFIX_ID_DOWNLINK + 50 + i as i32;
        let sta_id_ul = PREFIX_ID_UPLINK + 50 + i as i32;

        // let sta_id = 300 + i as i32;

        let bg_sta = STA_extended::new(
            // rate_bps_in,
            mean_length_bg,
            sta_id_dl,
            sta_id_ul, // Default destination (AP)
            coords,
            true,
            t0,
            true,
            rate_bps_bg_in, // Background traffic rate
            is_ul_bg_traffic,
            ap_coords,
            seed, 
            test_distances_everest_bool, 
        );

        let mbox_bg_sta = Mailbox::new();
        bg_sta_addresses.push(mbox_bg_sta.address());
        bg_sta_mailboxes.push(mbox_bg_sta);

        // TODO: REVISIT
        queue.STA_coords_map.insert(sta_id_dl as usize, ap_coords);

        queue
            .STA_coords_map
            .insert(sta_id_ul as usize, bg_sta.sta_coordinates.clone());

        bg_sta_models.push(bg_sta);
        all_sta_ids.push(sta_id_dl);
        all_sta_ids.push(sta_id_ul);
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

        vr.xr_server.output_perfect_information_bitrate.connect(
            XRClient::input_perfect_information_bitrate,
            &vr.mbox_xr_client.address(),
        );

        vr.sta_client.outport_coords_xrclient.connect(
            XRClient::input_coordinates_STA,
            &vr.mbox_xr_client.address(),
        );

        for (link_id, output) in queue.link_outputs.iter_mut() {
            // println!("connecting link id {} to corresponding sta", link_id);
            output.connect(STA_extended::input_wireless, &vr.mbox_sta_server);
            output.connect(STA_extended::input_wireless, &vr.mbox_sta_client);

            for mbox_bg_sta in &bg_sta_mailboxes {
                output.connect(STA_extended::input_wireless, mbox_bg_sta);
                // println!("Connecting queue output of L_id to BG STA",  )
            }
        }
    }

    // Connect background STAs to queue
    println!("Connecting background STAs...");
    for (i, bg_sta) in bg_sta_models.iter_mut().enumerate() {
        if bg_sta.is_ul_bg == 1 {
            // Connect to UL port
            println!(
                "  [BG STA {}] Connecting to QueueModule::input_UL (Uplink)",
                bg_sta.sta_id
            );
            bg_sta
                .output_network_port
                .connect(QueueModule::input_UL, &mbox_queue);
        } else if bg_sta.is_ul_bg == 0 {
            // Connect to DL port
            println!(
                "  [BG STA {}] Connecting to QueueModule::input (Downlink)",
                bg_sta.sta_id
            );
            bg_sta
                .output_network_port
                .connect(QueueModule::input, &mbox_queue);
        } else if bg_sta.is_ul_bg == 2 {
            println!(
                "  [BG STA {}] Connecting to QueueModule::input (Downlink AND Uplink)",
                bg_sta.sta_id
            );
            bg_sta
                .output_network_port
                .connect(QueueModule::input, &mbox_queue);
            bg_sta
                .output_network_port
                .connect(QueueModule::input_UL, &mbox_queue);

            // because each DL or UL BG packet will go to both input_UL and input in this mode, we guard via is_ul each input,
            // so STA_IDs are VERY importantin DL/UL, but we have enough free IDs using the consts that it is OK with up to 50 STAs per category.
        } else {
            crate::print_red!("WRONG OPTION!! BG STA{} is UL: {}", i, bg_sta.is_ul_bg,);
            continue;
        }
        queue
            .STA_coords_map
            .insert(bg_sta.sta_id as usize, bg_sta.sta_coordinates.clone());
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

    let packet_size = PACKET_SIZE_SOCKETS_BYTES;

    // Schedule XR events
    for addr in &emu_addresses {
        scheduler
            .schedule_event(
                Duration::from_secs(SIM_START_TIME),
                EmulatedLink::flush_queue,
                (),
                addr,
            )
            .unwrap();
    }

    for (i, (addr_client, addr_server)) in xr_client_addresses
        .iter()
        .zip(&xr_server_addresses)
        .enumerate()
    {
        let init: f64 = SIM_START_TIME as f64;

        let mut sessions = if test_distances_everest_bool {
            generate_session_timeline(&mut rng, stoptime)
        } else {
            generate_session_timeline_basic(init, stoptime)
        };

        // if i == 0 && abr == 3 {
        if i == 0 {
            // Make STA0 always be active, since it is used as the one for plots for a fair comparison.
            sessions = generate_session_timeline_basic(init, stoptime); // Force the ReinforcementLearner/VR STA to be active all across the simulation.
        }
        // let sessions: Vec<(f64, f64)> = generate_session_timeline(&mut rng, init, stoptime); // Each VR Session gets its own scheduling in the simulation

        crate::print_magenta!("ALL SESSIONS FOR CLIENT {} : {:#?}", i, sessions);
        let mut sessions = sessions;
        sessions.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let mut has_scheduled_vsync: bool = false;

        for (idx, (start, end)) in sessions.iter().copied().enumerate() {
            // ---- Schedule client start ----
            scheduler
                .schedule_event(
                    Duration::from_secs_f64(start),
                    XRClient::configure_streams,
                    packet_size,
                    addr_client,
                )
                .unwrap();

            if !has_scheduled_vsync {
                crate::print_red!(
                    "SIM START - Scheduling VSYNC at {} (ends at {}) ",
                    start,
                    end
                );

                scheduler
                    .schedule_event(
                        // ALREADY SCHEDULED BY session_reboot at start/end, do not schedule twice!!
                        Duration::from_secs_f64(start),
                        XRClient::vsync,
                        (),
                        addr_client,
                    )
                    .unwrap();
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
            scheduler
                .schedule_event(
                    Duration::from_secs_f64(end),
                    XRClient::session_end,
                    pause_after,
                    addr_client,
                )
                .unwrap();

            // ---- Server: start and end mirroring client ----
            let dest_ip = IpAddr::V4(Ipv4Addr::new(127, 0, idx as u8, 2));
            scheduler
                .schedule_event(
                    Duration::from_secs_f64(start),
                    XRServer::connection_pipeline,
                    dest_ip,
                    addr_server,
                )
                .unwrap();

            scheduler
                .schedule_event(
                    Duration::from_secs_f64(end),
                    XRServer::session_end,
                    pause_after,
                    addr_server,
                )
                .unwrap();
        }
    }
    // Use saved addresses for movement, only in Client STAs! Will also share coords messages with connected XRClient
    for sta_client_addr in sta_client_addrs.iter() {
        scheduler
            .schedule_event(
                Duration::from_secs(SIM_START_TIME),
                STA_extended::move_coordinates_everest, // will only actually move if rwalk is set to true
                (),
                sta_client_addr,
            )
            .unwrap();
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

    if let Ok(stats) = queue_stats.lock() {
        stats.print_nicely();
    };
    let elapsed = start_sim_benchmark.elapsed();
    // ===== Drain viz events =====
    let mut events: Vec<VizEvent> = Vec::with_capacity(1024);
    while let Ok(ev) = viz_rx.try_recv() {
        events.push(ev);
    }

    
    println!(
        "[VIZ] Collected {} events in in {:.4} s spanning from [{:.6}, {:.6}] s of simulated time ",
        events.len(),
        elapsed.as_secs_f32(), 
        events.first().map(event_t).unwrap_or(0.0),
        events.last().map(event_end).unwrap_or(0.0),
    );

    // Optional: pickle for later replay (uncomment if VizEvent + MacKey + EdcaAc derive Serialize/Deserialize)
    // let viz_path = format!("{}/viz_events.bin", output_path);
    // if let Ok(file) = std::fs::File::create(&viz_path) {
    //     let _ = bincode::serialize_into(std::io::BufWriter::new(file), &events);
    //     println!("[VIZ] Wrote {} events to {}", events.len(), viz_path);
    // }

    // ===== Open the viewer (blocks until the user closes the window) =====
    if !events.is_empty() {
        let idx = VizIndex::build(events);
        run_viewer(idx);
    } else {
        println!("[VIZ] No events to visualize (was viz_tx wired up?).");
    }


    Ok(())
}


use std::collections::{HashMap, HashSet};
use crate::lib::render_text;
use crate::lib::MacKey; 
pub struct VizIndex {
    pub all: Vec<VizEvent>,                                  // owned, sorted by t
    pub txops_by_link: HashMap<u8, Vec<usize>>,              // indices into `all`
    pub collisions_by_link: HashMap<u8, Vec<usize>>,
    pub backoff_by_key: HashMap<MacKey, Vec<usize>>,
    pub qdepth_by_key: HashMap<MacKey, Vec<usize>>,
    pub mac_keys_sorted: Vec<MacKey>,                        // for the row layout
    pub t_min: f64,
    pub t_max: f64,
}

impl VizIndex {
    pub fn build(mut events: Vec<VizEvent>) -> Self {
        events.sort_by(|a, b| event_t(a).partial_cmp(&event_t(b)).unwrap());

        let mut idx = VizIndex {
            t_min: events.first().map(event_t).unwrap_or(0.0),
            t_max: events.last().map(|e| event_end(e)).unwrap_or(1.0),
            all: events,
            txops_by_link: HashMap::new(),
            collisions_by_link: HashMap::new(),
            backoff_by_key: HashMap::new(),
            qdepth_by_key: HashMap::new(),
            mac_keys_sorted: Vec::new(),
        };

        let mut keys = HashSet::new();
        for (i, ev) in idx.all.iter().enumerate() {
            match ev {
                VizEvent::TxopStart { link_id, owner, .. } => {
                    idx.txops_by_link.entry(*link_id).or_default().push(i);
                    keys.insert(*owner);
                }
                VizEvent::Collision { link_id, contenders, .. } => {
                    idx.collisions_by_link.entry(*link_id).or_default().push(i);
                    for k in contenders { keys.insert(*k); }
                }
                VizEvent::BackoffSnap { mac_key, .. } => {
                    idx.backoff_by_key.entry(*mac_key).or_default().push(i);
                    keys.insert(*mac_key);
                }
                VizEvent::QueueDepth { mac_key, .. } => {
                    idx.qdepth_by_key.entry(*mac_key).or_default().push(i);
                    keys.insert(*mac_key);
                }
            }
        }
        idx.mac_keys_sorted = keys.into_iter().collect();
        // sort by (link_id, sta_id, ac_priority) so rows group by link, AP first, then VO/VI/BE/BG
        idx.mac_keys_sorted.sort_by_key(|k| (k.2, k.0 != -1, k.0, ac_prio(k.1)));
        idx
    }
}

fn latest_at<'a>(indices: &'a [usize], all: &'a [VizEvent], t_cursor: f64)
    -> Option<&'a VizEvent>
{
    // binary search by t
    let pos = indices.partition_point(|&i| event_t(&all[i]) <= t_cursor);
    if pos == 0 { None } else { Some(&all[indices[pos - 1]]) }
}

use minifb::{Key, MouseButton, MouseMode, Window, WindowOptions};
use crate::lib::EdcaAc;

pub struct ViewState {
    pub center_t: f64,
    pub span_t: f64,
    pub cursor_t: f64, 
    pub paused: bool,
    pub selected_link: Option<u8>,
    pub row_scroll: i32,
    pub mouse_drag: Option<(f32, f64)>, // (mouse_x_at_drag_start, center_t_at_drag_start)
}

pub fn run_viewer(idx: VizIndex) {
    const W: usize = 1500;
    const H: usize = 900;
    let mut window = Window::new("WLAN Sim Playback", W, H, WindowOptions::default()).unwrap();

    let mut buf = vec![0u32; W * H];
    let mut view = ViewState {
        center_t: (idx.t_min + idx.t_max) * 0.5,
        span_t: ((idx.t_max - idx.t_min) * 0.05).max(0.001),
        cursor_t: idx.t_min, // Initialize cursor at the start
        paused: false,
        selected_link: None,
        row_scroll: 0,
        mouse_drag: None,
    };

    let panel_x = 220; // Increased from 100 to give the text more breathing room
    let panel_w = W - panel_x - 20;

    while window.is_open() && !window.is_key_down(Key::Escape) {
        // Pass panel_w to handle_input so mouse tracking scales correctly
        handle_input(&window, &mut view, &idx, panel_w);
        clamp_view(&mut view, &idx);

        buf.fill(0x0d0d12);
        render_hud(&mut buf, W, &view, &idx);

        let mut lane_y = 60usize;
        let lane_h = 50usize;
        for &link_id in &[0u8, 1u8] {
            // Use variables instead of hardcoded 100
            render_link_lane(&mut buf, W, lane_y, lane_h, panel_x, panel_w, &view, &idx, link_id);
            lane_y += lane_h + 5;
        }

        let rows_top = lane_y + 10;
        let rows_bottom = H - 200;
        // Use variables here too
        render_mackey_rows(&mut buf, W, rows_top, rows_bottom, panel_x, panel_w, &view, &idx);

        // And here
        render_qdepth_panel(&mut buf, W, H - 180, 160, panel_x, panel_w, &view, &idx);
        
        let cursor_x = x_of(view.cursor_t, &view, panel_x, panel_w);
        if cursor_x >= panel_x as i32 && cursor_x <= (panel_x + panel_w) as i32 {
            draw_vline(&mut buf, W, cursor_x, 50, (H - 20) as i32, 0xffff66);
        }

        window.update_with_buffer(&buf, W, H).unwrap();
    }
}


fn handle_input(window: &Window, view: &mut ViewState, idx: &VizIndex, panel_w: usize) {
    // Pan
    if window.is_key_down(Key::Left) || window.is_key_down(Key::A) {
        view.center_t -= view.span_t * 0.02;
    }
    if window.is_key_down(Key::Right) || window.is_key_down(Key::D) {
        view.center_t += view.span_t * 0.02;
    }
    // Zoom
    if window.is_key_pressed(Key::Equal, minifb::KeyRepeat::Yes)
        || window.is_key_pressed(Key::NumPadPlus, minifb::KeyRepeat::Yes) {
        view.span_t *= 0.8;
    }
    if window.is_key_pressed(Key::Minus, minifb::KeyRepeat::Yes)
        || window.is_key_pressed(Key::NumPadMinus, minifb::KeyRepeat::Yes) {
        view.span_t *= 1.25;
    }
    // Jump to start/end
    if window.is_key_pressed(Key::Home, minifb::KeyRepeat::No) {
        view.center_t = idx.t_min + view.span_t * 0.5;
    }
    if window.is_key_pressed(Key::End, minifb::KeyRepeat::No) {
        view.center_t = idx.t_max - view.span_t * 0.5;
    }
    // Pause
    if window.is_key_pressed(Key::Space, minifb::KeyRepeat::No) {
        view.paused = !view.paused;
    }
    // Cycle selected link filter
    if window.is_key_pressed(Key::Tab, minifb::KeyRepeat::No) {
        view.selected_link = match view.selected_link {
            None => Some(0), Some(0) => Some(1), Some(1) => None, _ => None,
        };
    }
    // Mouse drag pan
    let (mx, _my) = window.get_mouse_pos(MouseMode::Discard).unwrap_or((0.0, 0.0));
    if window.get_mouse_down(MouseButton::Left) {
        if let Some((mx0, ct0)) = view.mouse_drag {
            let dx_px = (mx - mx0) as f64;
            let dt_per_px = view.span_t / panel_w as f64;
            view.center_t = ct0 - dx_px * dt_per_px;
        } else {
            view.mouse_drag = Some((mx, view.center_t));
        }
    } else {
        view.mouse_drag = None;
    }
    // Right-click to place the playhead cursor (NEW)
    if window.get_mouse_down(MouseButton::Right) {
        let panel_x0 = 100.0;
        let drawing_width = (panel_w - 120) as f64;
        let n = ((mx as f64) - panel_x0) / drawing_width;
        let clicked_t = view.center_t - view.span_t * 0.5 + view.span_t * n;
        view.cursor_t = clicked_t.clamp(idx.t_min, idx.t_max);
    }


    // Scroll wheel zoom around mouse
    if let Some((_, scroll_y)) = window.get_scroll_wheel() {
        if scroll_y.abs() > 0.0 {
            let factor = if scroll_y > 0.0 { 0.85 } else { 1.18 };
            // pivot zoom on cursor
            let panel_x0 = 100.0;
            let n = ((mx as f64) - panel_x0) / panel_w as f64;
            let n = n.clamp(0.0, 1.0);
            let t_at_cursor = view.center_t - view.span_t * 0.5 + view.span_t * n;
            view.span_t *= factor;
            view.center_t = t_at_cursor - view.span_t * (n - 0.5);
        }
    }
    // Row scroll
    if window.is_key_down(Key::PageDown) { view.row_scroll += 4; }
    if window.is_key_down(Key::PageUp)   { view.row_scroll -= 4; }
}

fn clamp_view(view: &mut ViewState, idx: &VizIndex) {
    let total = idx.t_max - idx.t_min;
    view.span_t = view.span_t.clamp(1e-6, total.max(1e-3));
    view.center_t = view.center_t.clamp(idx.t_min + view.span_t * 0.5,
                                        idx.t_max - view.span_t * 0.5);
    view.row_scroll = view.row_scroll.max(0);
}

fn x_of(t: f64, view: &ViewState, panel_x: usize, panel_w: usize) -> i32 {
    let t0 = view.center_t - view.span_t * 0.5;
    let n = (t - t0) / view.span_t;
    panel_x as i32 + (n * panel_w as f64) as i32
}

fn fill_rect(buf: &mut [u32], stride: usize, x: usize, y: usize, w: usize, h: usize, c: u32) {
    let h_buf = buf.len() / stride;
    for yy in y..(y + h).min(h_buf) {
        let row = yy * stride;
        for xx in x..(x + w).min(stride) {
            buf[row + xx] = c;
        }
    }
}

fn ac_color(ac: EdcaAc) -> u32 {
    match ac {
        EdcaAc::Voice      => 0xff66cc,  // magenta-ish
        EdcaAc::Video      => 0x66ccff,  // cyan
        EdcaAc::BestEffort => 0x88dd88,  // green
        EdcaAc::Background => 0xaaaaaa,  // grey
    }
}

fn render_link_lane(
    buf: &mut [u32], stride: usize, lane_y: usize, lane_h: usize,
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex, link_id: u8,
) {
    fill_rect(buf, stride, panel_x, lane_y, panel_w, lane_h, 0x14141c);

    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;

    // TXOPs
    if let Some(indices) = idx.txops_by_link.get(&link_id) {
        let start = indices.partition_point(|&i| event_end(&idx.all[i]) < t_lo);
        for &ii in &indices[start..] {
            let ev = &idx.all[ii];
            
            // We match by reference to avoid moving the Vec out of the VizIndex
            let (t, end, owner, dest_id, ampdu_packets, mcs, stream_id, frame_id) = match ev {
                VizEvent::TxopStart { t, end, owner, dest_id, ampdu_packets, mcs, stream_id, frame_ids, .. } =>
                    (*t, *end, *owner, *dest_id, *ampdu_packets, *mcs, *stream_id, frame_ids), // Removed '*' from frame_ids
                _ => continue,
            };
            
            if t > t_hi { break; }

            let x0 = x_of(t,   view, panel_x, panel_w).max(panel_x as i32);
            let x1 = x_of(end, view, panel_x, panel_w).min((panel_x + panel_w) as i32);
            if x1 <= x0 { continue; }

            let color = ac_color(owner.1);
            fill_rect(buf, stride, x0 as usize, lane_y + 4,
                      (x1 - x0) as usize, lane_h - 8, color);

            let width = x1 - x0;
            if width > 80 {
                // Row 1: MAC info
                let label1 = format!("{:?} STA{}->{} MCS{}", owner.1, owner.0, dest_id, mcs);
                render_text(buf, &label1, x0 as usize + 4, lane_y + 8, stride, 0x000000, 1);
                
                // Row 2: ALVR info
                if width > 100 {
                    // format! works perfectly with a &Vec<u32> using the {:?} debug formatter
                    let label2 = format!("{}|frameIDs: {:?} ({} MPDUs)", crate::lib::alvr_stream_socket::get_stream_name(stream_id), frame_id, ampdu_packets);
                    render_text(buf, &label2, x0 as usize + 4, lane_y + 20, stride, 0x000000, 1);
                }
            }
        }
    }

    // Collisions overlay
    if let Some(indices) = idx.collisions_by_link.get(&link_id) {
        let start = indices.partition_point(|&i| event_end(&idx.all[i]) < t_lo);
        for &ii in &indices[start..] {
            let ev = &idx.all[ii];
            let (t, end) = match ev {
                VizEvent::Collision { t, end, .. } => (*t, *end),
                _ => continue,
            };
            if t > t_hi { break; }
            let x0 = x_of(t,   view, panel_x, panel_w).max(panel_x as i32);
            let x1 = x_of(end, view, panel_x, panel_w).min((panel_x + panel_w) as i32);
            if x1 <= x0 { continue; }
            for yy in (lane_y + 6)..(lane_y + lane_h - 6) {
                let c = if (yy & 2) == 0 { 0xff3344 } else { 0x661010 };
                let row = yy * stride;
                for xx in (x0 as usize)..(x1 as usize).min(stride) {
                    buf[row + xx] = c;
                }
            }
        }
    }

    render_text(buf, &format!("LINK {}", link_id), 8, lane_y + lane_h / 2 - 8, stride, 0xeeeeee, 2);
}

fn render_mackey_rows(
    buf: &mut [u32], stride: usize, top: usize, bottom: usize,
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
) {
    let row_h = 36;
    let rows_visible = (bottom - top) / row_h;
    
    // Evaluate states at the user's cursor, not the center of the screen
    let cursor = view.cursor_t; 

    // Draw a dark sidebar background for contrast so timeline bars don't bleed under text
    fill_rect(buf, stride, 0, top, panel_x, bottom - top, 0x14141c);

    let start_row = view.row_scroll as usize;
    for (vrow, key) in idx.mac_keys_sorted.iter()
        .skip(start_row)
        .take(rows_visible)
        .enumerate()
    {
        if let Some(filter) = view.selected_link {
            if key.2 != filter { continue; }
        }
        let y = top + vrow * row_h;

        // Row label (Bumped to pure white for better visibility)
        let lbl = format!("sta{:>4} {:?} L{}",
            if key.0 == -1 { -1 } else { key.0 }, key.1, key.2);

        render_text(buf, &lbl, 8, y + 8, stride, 0xffffff, 2);
        // State at cursor
        let bo = idx.backoff_by_key.get(key)
            .and_then(|v| latest_at(v, &idx.all, cursor));
        let qd = idx.qdepth_by_key.get(key)
            .and_then(|v| latest_at(v, &idx.all, cursor));

        if let Some(VizEvent::BackoffSnap { counter, cw, frozen, medium_free_since, .. }) = bo {
            // AIFS box: lit if cursor < medium_free_since + aifs_secs(ac)
            let aifs_s = aifs_secs_for_ac(key.1);
            let aifs_active = cursor < medium_free_since + aifs_s;
            let aifs_color = if aifs_active { 0xffaa00 } else { 0x333333 };
            fill_rect(buf, stride, panel_x, y + 4, 14, row_h - 8, aifs_color);

            // Backoff bar: cw cells, counter remaining
            let bar_x = panel_x + 24;
            let bar_w = 200;
            let cells = (*cw as usize).min(64);
            let cell_w = (bar_w / cells.max(1)).max(2);
            for c in 0..cells {
                let x = bar_x + c * cell_w;
                let color = if c < (*counter as usize) {
                    if *frozen { 0x664488 } else { 0x44aaff }
                } else { 0x222230 };
                fill_rect(buf, stride, x, y + 6, cell_w - 1, row_h - 12, color);
            }
            render_text(buf, &format!("CW={} BO={}{}", cw, counter,
                if *frozen { " ❄" } else { "" }),
                bar_x + bar_w + 12, y + 4, stride, 0xcccccc, 2);
        }

        if let Some(VizEvent::QueueDepth { depth, .. }) = qd {
            // Q bar at right side
            let qx = panel_x + panel_w - 200;
            let qw_max = 180;
            let normalized = (*depth as f64 / 64.0).min(1.0);  // pick a sensible cap
            let qw = (normalized * qw_max as f64) as usize;
            fill_rect(buf, stride, qx, y + 6, qw_max, row_h - 12, 0x222230);
            fill_rect(buf, stride, qx, y + 6, qw, row_h - 12, 0xee8844);
            render_text(buf, &format!("Q={}", depth), qx + qw_max + 8, y + 4, stride, 0xcccccc, 1);
        }
    }
}

#[derive(Clone, Copy, Debug, Hash)]
pub struct EdcaParam {
    pub cw_min: u32,
    pub cw_max: u32,
    pub aifsn: u8,          // slots added to DIFS
    pub txop_limit_us: u16, // 0 = no TXOP
}
const SIFS_US: u64 = 16; // 16 µs
const SLOT_TIME_US: u64 = 9; // 9 µs for OFDM

fn aifs(p: EdcaParam) -> Duration {
    Duration::from_micros(SIFS_US + p.aifsn as u64 * SLOT_TIME_US)
}
fn event_t(ev: &VizEvent) -> f64 {
    match ev {
        VizEvent::TxopStart   { t, .. } => *t,
        VizEvent::Collision   { t, .. } => *t,
        VizEvent::BackoffSnap { t, .. } => *t,
        VizEvent::QueueDepth  { t, .. } => *t,
    }
}

fn event_end(ev: &VizEvent) -> f64 {
    match ev {
        VizEvent::TxopStart { end, .. } => *end,
        VizEvent::Collision { end, .. } => *end,
        // instantaneous events: end == t
        VizEvent::BackoffSnap { t, .. } => *t,
        VizEvent::QueueDepth  { t, .. } => *t,
    }
}

// Free function — the QueueModule has its own &mut self version,
// but this one is for the viewer where we don't have a QueueModule.
fn ac_prio(ac: EdcaAc) -> u8 {
    match ac {
        EdcaAc::Voice      => 0,
        EdcaAc::Video      => 1,
        EdcaAc::BestEffort => 2,
        EdcaAc::Background => 3,
    }
}

// Drop the `#[derive(Hash, Clone, Debug)]` line — that's only valid on types,
// not on functions. The function itself is fine:
fn aifs_secs_for_ac(ac: EdcaAc) -> f64 {
    match ac {
        EdcaAc::Voice      => 25e-6,
        EdcaAc::Video      => 25e-6,
        EdcaAc::BestEffort => 43e-6,
        EdcaAc::Background => 79e-6,
    }
}


fn render_hud(buf: &mut [u32], stride: usize, view: &ViewState, idx: &VizIndex) {
    fill_rect(buf, stride, 0, 0, stride, 50, 0x1a1a24);
    let span_ms = view.span_t * 1000.0;
    let label = format!(
        "t = {:.6}s   span = {:.3}ms   range = [{:.3}, {:.3}]s   {} events   {}",
        view.center_t, span_ms, idx.t_min, idx.t_max, idx.all.len(),
        if view.paused { "PAUSED" } else { "" },
    );
    render_text(buf, &label, 12, 16, stride, 0xeeeeee, 2);

    let filter = match view.selected_link {
        Some(l) => format!("[link filter: L{}]", l),
        None    => "[link filter: ALL]".to_string(),
    };
    render_text(buf, &filter, stride.saturating_sub(260), 16, stride, 0xaaccff, 1);
    
    // Controls instructions
    render_text(buf, "L-Click drag pan   R-Click set cursor   Scroll zoom   Tab filter-link   Space pause",
                12, 36, stride, 0x888899, 1);

    // Color Legend (NEW)
    let mut leg_x = stride.saturating_sub(450);
    let leg_y = 36;
    render_text(buf, "LEGEND:", leg_x, leg_y, stride, 0x888899, 1);
    leg_x += 60;
    for (ac, name, col) in [
        (EdcaAc::Voice, "VO", 0xff66cc),
        (EdcaAc::Video, "VI", 0x66ccff),
        (EdcaAc::BestEffort, "BE", 0x88dd88),
        (EdcaAc::Background, "BK", 0xaaaaaa),
    ] {
        fill_rect(buf, stride, leg_x, leg_y, 10, 10, col);
        render_text(buf, name, leg_x + 14, leg_y - 2, stride, 0xeeeeee, 1);
        leg_x += 40;
    }
}

// Helper to safely dim a 32-bit RGB hex color without channel bleeding
fn dim_color(color: u32, factor: u32) -> u32 {
    let r = ((color >> 16) & 0xFF) / factor;
    let g = ((color >> 8) & 0xFF) / factor;
    let b = (color & 0xFF) / factor;
    (r << 16) | (g << 8) | b
}
fn render_qdepth_panel(
    buf: &mut [u32], stride: usize,
    panel_y: usize, panel_h: usize,
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
) {
    // Backgrounds
    fill_rect(buf, stride, panel_x, panel_y, panel_w, panel_h, 0x10101a);
    fill_rect(buf, stride, 0, panel_y, panel_x, panel_h, 0x14141c);

    render_text(buf, "QUEUES BY DESTINATION:", 8, panel_y + 6, stride, 0xcccccc, 2);

    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;

    // --- 1. AGGREGATION LOGIC ---
    let mut agg_map: HashMap<(i32, EdcaAc, i32), Vec<usize>> = HashMap::new();
    
    for (key, indices) in &idx.qdepth_by_key {
        for &ii in indices {
            if let VizEvent::QueueDepth { sta_src, .. } = &idx.all[ii] {
                let agg_key = (key.0, key.1, *sta_src);
                agg_map.entry(agg_key).or_default().push(ii);
            }
        }
    }

    for indices in agg_map.values_mut() {
        indices.sort_by(|&a, &b| event_t(&idx.all[a]).partial_cmp(&event_t(&idx.all[b])).unwrap());
    }

    // --- 2. MAX DEPTH CALCULATION (FIXED FOR ZOOM) ---
    let mut max_depth = 1usize;
    for indices in agg_map.values() {
        let s = indices.partition_point(|&i| event_t(&idx.all[i]) < t_lo);
        
        // CRITICAL FIX: Include the active depth just before the visible window
        if s > 0 {
            if let VizEvent::QueueDepth { depth, .. } = &idx.all[indices[s - 1]] {
                if *depth > max_depth { max_depth = *depth; }
            }
        }

        for &ii in &indices[s..] {
            let t = event_t(&idx.all[ii]);
            if t > t_hi { break; }
            if let VizEvent::QueueDepth { depth, .. } = &idx.all[ii] {
                if *depth > max_depth { max_depth = *depth; }
            }
        }
    }

    let mut keys_to_draw: Vec<_> = agg_map.keys().collect();
    keys_to_draw.sort_by_key(|k| (ac_prio(k.1), k.0, k.2));
    keys_to_draw.reverse();

    let mut legend_y = panel_y + 24;
    let base_y = panel_y as i32 + panel_h as i32 - 2;

    // Helper closure to safely calculate and clamp Y coordinates
    let calc_y = |depth: usize, max: usize| -> i32 {
        let raw_y = base_y - (((depth as f64 / max as f64) * (panel_h as f64 - 12.0)) as i32);
        raw_y.clamp(panel_y as i32, base_y) // Strict boundary enforcement
    };

    for &&(sta_id, ac, sta_src) in &keys_to_draw {
        let indices = &agg_map[&(sta_id, ac, sta_src)];
        
        // --- VISUAL DISTINCTION LOGIC ---
        let is_target_range = (100..=150).contains(&sta_src);
        
        // Pattern: 0 (Solid) for target range, 2 (Dotted) for others
        let pattern_type = if is_target_range { 0 } else { 2 }; 
        
        // Hue: Warm colors for target range, Cool colors for others
        let outline_color = if sta_src == -1 {
            ac_color(ac) 
        } else if is_target_range {
            let warm_palette = [0xFF4444, 0xFF8822, 0xFFCC33, 0xFF55AA];
            warm_palette[(sta_src.abs() as usize) % warm_palette.len()]
        } else {
            let cool_palette = [0x44AAFF, 0x44FF88, 0x8844FF, 0x44FFEE];
            cool_palette[(sta_src.abs() as usize) % cool_palette.len()]
        };

        let fill_color = dim_color(outline_color, 6); 

        let s = indices.partition_point(|&i| event_t(&idx.all[i]) < t_lo);
        let is_active = (s > 0 && s <= indices.len()) || 
                        (s < indices.len() && event_t(&idx.all[indices[s]]) <= t_hi);

        // --- 3. LEGEND WITH PATTERN PREVIEW ---
        if is_active && legend_y + 12 < panel_y + panel_h {
            let lbl = if sta_src == -1 {
                format!("STA{:<3} {:?} (All)", sta_src, ac)
            } else {
                format!("STA{:<3} {:?}", sta_src, ac)
            };
            
            render_text(buf, &lbl, 35, legend_y, stride, 0xdddddd, 2);
            
            for px in 0..22 {
                if should_draw_pixel(px as i32, pattern_type) {
                    buf[(legend_y + 6) * stride + (8 + px)] = outline_color;
                }
            }
            legend_y += 14;
        }

        let mut prev: Option<(i32, i32)> = None;

        if s > 0 {
            if let VizEvent::QueueDepth { depth, .. } = &idx.all[indices[s - 1]] {
                let x = x_of(t_lo, view, panel_x, panel_w);
                let y = calc_y(*depth, max_depth); // Use clamping helper
                prev = Some((x, y));
            }
        }

        // --- 4. RENDERING WITH STIPPLE PATTERN ---
        for &ii in &indices[s..] {
            let (t, depth) = match &idx.all[ii] {
                VizEvent::QueueDepth { t, depth, .. } => (*t, *depth),
                _ => continue,
            };
            if t > t_hi { break; }
            
            let x = x_of(t, view, panel_x, panel_w);
            let y = calc_y(depth, max_depth); // Use clamping helper
            
           if let Some((px, py)) = prev {
                for fill_x in px..x {
                    if fill_x >= panel_x as i32 && fill_x < (panel_x + panel_w) as i32 {
                        let ux = fill_x as usize;
                        if should_draw_pixel(fill_x, pattern_type) {
                            let outline_idx = (py as usize) * stride + ux;
                            if outline_idx < buf.len() {
                                buf[outline_idx] = outline_color;
                            }
                        }
                        draw_vline(buf, stride, fill_x, py + 1, base_y, fill_color);
                    }
                }
                if x >= panel_x as i32 && x < (panel_x + panel_w) as i32 {
                    draw_vline(buf, stride, x, py, y, outline_color);
                }
            }
            prev = Some((x, y));
        }

        if let Some((px, py)) = prev {
            let end_x = (panel_x + panel_w) as i32;
            for fill_x in px..end_x {
                if fill_x >= panel_x as i32 && fill_x < end_x {
                    let ux = fill_x as usize;
                    if should_draw_pixel(fill_x, pattern_type) {
                        let outline_idx = (py as usize) * stride + ux;
                        if outline_idx < buf.len() {
                            buf[outline_idx] = outline_color;
                        }
                    }
                    draw_vline(buf, stride, fill_x, py + 1, base_y, fill_color);
                }
            }
        }
    }
    render_text(buf, &format!("max={}", max_depth), panel_x + 6, panel_y + 6, stride, 0x888899, 1);
}


/// Helper to determine if a pixel should be drawn for a specific pattern
fn should_draw_pixel(x: i32, pattern_type: usize) -> bool {
    match pattern_type {
        1 => (x / 6) % 2 == 0,  // Dashed
        2 => (x / 2) % 2 == 0,  // Dotted
        _ => true,              // Solid (STA 0 or default)
    }
}
fn draw_hline(buf: &mut [u32], stride: usize, x0: i32, x1: i32, y: i32, c: u32) {
    let h = (buf.len() / stride) as i32;
    if y < 0 || y >= h { return; }
    let (a, b) = if x0 <= x1 { (x0, x1) } else { (x1, x0) };
    let a = a.max(0) as usize;
    let b = (b.min(stride as i32 - 1)) as usize;
    let row = (y as usize) * stride;
    for x in a..=b { buf[row + x] = c; }
}

fn draw_vline(buf: &mut [u32], stride: usize, x: i32, y0: i32, y1: i32, c: u32) {
    let h = (buf.len() / stride) as i32;
    if x < 0 || x >= stride as i32 { return; }
    let (a, b) = if y0 <= y1 { (y0, y1) } else { (y1, y0) };
    let a = a.max(0) as usize;
    let b = b.min(h - 1) as usize;
    for y in a..=b { buf[y * stride + x as usize] = c; }
}