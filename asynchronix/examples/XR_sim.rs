#[allow(unused_imports)]
#[allow(dead_code)]
#[allow(unused)]
use asynchronix::simulation::{Mailbox, Scheduler, SimInit};

// use rand::seq::SliceRandom;
// use futures_util::Stream;
// use lib::alvr_stream_socket::{Buffer, StreamReceiver};
// use tai_time::TaiTime;
// use rand::{thread_rng, SeedableRng};
// use rand::rngs::StdRng;
// use asynchronix::time::MonotonicTime;
// use lib::models_mm1k::NetworkPattern;
// use tai_time::TaiTime;
// use xkbcommon::xkb::Table;
// use crate::lib::models_XR::BitrateMode;

mod lib; // for calling m own local library
         // mod lib;        // <- this exposes examples/lib/* as `crate::lib`
mod xr_entry; // <- brings in examples/xr_entry.rs as submodule

use std::env;
use xr_entry::{parse_cli_to_params, run_sim};

fn main() {
    env::set_var("RUST_BACKTRACE", "1");
    let args: Vec<String> = env::args().collect();
    if args.len() != crate::xr_entry::NUM_INPUT_ARGS_SIM {
        println!("Debug: Expected {}, got {}", crate::xr_entry::NUM_INPUT_ARGS_SIM, args.len());
       
        eprintln!(
            "Usage: {} <stoptime> <mean_length_BG> <k_queue> <distance> <initial_bitrate> \
            <pl_prob> <n_xr> <n_bg> <rate_bps_BG> <IS_UL> <test_type> <video_filename> <FPS> \
            <N_close_users> <distance_close_users> <seed> <GoP_size> <Intra-refresh> <Use_Foveation> \
            <VBV_per_frame> <ABR_mode> <nest-vr_profile> <Coords_everest_test> <sim_id> \
            <observation_type> <reward_mode> <T_update_abr> <MLO_config> <Edca_be> \
            <mlo_link_selection> <packs_per_ampdu> <codec_input> <results_path>",
            args[0]
        );

        std::process::exit(2);
    }
    let params = parse_cli_to_params(&args);
    if let Err(e) = run_sim(params) {
        eprintln!("Simulation failed: {e:?}");
        std::process::exit(1);
    }
}
