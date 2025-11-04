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

use crate::lib::models_XR::{NestVrProfile, ObservationConfig, STA_extended, XRClient, XRServer};
use crate::lib::UPLINK_QUEUE_SIZE;

pub const SIM_START_TIME: u64 = 1;


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




fn main() {
    env::set_var("RUST_BACKTRACE", "1");
    let args: Vec<String> = env::args().collect();
    if args.len() != 28 {
        eprintln!("Usage: {} <stoptime> <mean_length_BG> <k_queue> \
                <distance> <bitrate> <pl_prob> <n_xr> <n_bg> <rate_bps_BG> <IS_UL> <test_type> \
                <video_filename> <FPS> <N_close_users> <distance_close_users> <seed> <GoP_size> \
                <Intra-refresh enabled> <ABR enabled> <nest-vr_profile> <Coords_everest_movement_test> <sim_id> <observation_config> 
                <reward_mode> <T_ABR> <eval_string> <MLO_config (MLO0-3)>", args[0]);
        std::process::exit(2);
    }
    let params = parse_cli_to_params(&args);
    if let Err(e) = run_sim(params) {
        eprintln!("Simulation failed: {e:?}");
        std::process::exit(1);
    }
}

