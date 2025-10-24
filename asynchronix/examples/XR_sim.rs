#[allow(unused_imports)]
#[allow(dead_code)]
#[allow(unused)]
////////////////////////////////////// XR SIMULATOR ////////////////////////////
///
///     Mixing up connection.rs and bitratemanager to simplify the process of generating frames.
///     
// use crate::::generate_session_timeline; 
use asynchronix::simulation::{Mailbox, Scheduler, SimInit};
use asynchronix::time::MonotonicTime;
use lib::models_mm1k::NetworkPattern;
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

mod lib; // for calling m own local library
// mod lib;        // <- this exposes examples/lib/* as `crate::lib`
mod xr_entry;   // <- brings in examples/xr_entry.rs as submodule

use xr_entry::{SimParams, parse_cli_to_params, run_sim};


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
        let framerate; 
    
        // UNCOMMENT (when not evaluating RL in same scenario) . 
        // if matches!(abr_enabled, 3){  // ABR==3 -> ReinforcementLearner mode, First VR pair is RL, rest is random between CBR, Nest-VR and Everest. 

            if pair_index == 0{ 
                abr_choice = abr_enabled; 
                framerate = fps; 
                // do nothing, it's correct
            }
            else{
                // abr_choice = rng.gen_range(0..=2);  // generates 0, 1, or 2 (or 4 for GCC)
                let choices = [0, 1, 2, 4];

                let fps_choices = [90.0, 120.0, 60.0]; // Random FPS -> More entropy, less bias
                let mut rng = thread_rng();
                abr_choice = *choices.choose(&mut rng).unwrap();

                framerate = *fps_choices.choose(&mut rng).unwrap();


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
            framerate,
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

        let mut xr_client = XRClient::new(client_ip, framerate, t0, name_folder, test, abr_choice, simu_unique_str, bm_string);

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





fn main() {
    env::set_var("RUST_BACKTRACE", "1");
    let args: Vec<String> = env::args().collect();
    if args.len() != 23 {
        eprintln!("Usage: {} <stoptime> <mean_length_BG> <k_queue> \
                <distance> <bitrate> <pl_prob> <n_xr> <n_bg> <rate_bps_BG> <IS_UL> <test_type> \
                <video_filename> <FPS> <N_close_users> <distance_close_users> <seed> <GoP_size> \
                <Intra-refresh enabled> <ABR enabled> <nest-vr_profile> <Coords_everest_movement_test> <sim_id>", args[0]);
        std::process::exit(2);
    }
    let params = parse_cli_to_params(&args);
    if let Err(e) = run_sim(params) {
        eprintln!("Simulation failed: {e:?}");
        std::process::exit(1);
    }
}

