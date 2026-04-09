use csv::Writer;
use futures::future::join_all;
use std::sync::Arc;
use std::{path::Path, time::Duration};
use tai_time::TaiTime;
use tokio::{sync::Semaphore, time::sleep};
mod lib;
use crate::lib::alvr_stream_socket::{ChunkedAv1Encoder, ChunkedEncoder, VideoCodec};
// bring your types into scope (adjust these paths to your project)
use crate::lib::models_XR::{HEIGHT_ENCODER, WIDTH_ENCODER};
use lib::alvr_stream_socket::{ChunkedHevcEncoder, ChunkedSoftwareHevcEncoder};
use std::env;
use std::path::PathBuf;

pub const CSV_FOLDER_STR: &str = "aaa_csv_framesizes";
pub const VIDEO_NAME: &str = "snow_short";

const CHUNK_DURATION: f64 = 5.0;
const NUM_SEMAPHORES: usize = 3; // NUMBER OF PARALLEL TASKS.

const FRAMERATE_VALUES: [u32; 3] = [60, 90, 120];
const CODECS_TO_RUN: [VideoCodec; 2] = [VideoCodec::HEVC, VideoCodec::AV1];


// Instead of consts, we use a small helper
fn get_paths() -> (PathBuf, PathBuf) {
    let user = env::var("USER").unwrap_or_else(|_| "unknown".to_string());
    match user.as_str() {
        // HPC user
        "fmaura" => (
            PathBuf::from("/home/fmaura/simulator_asynchronix/asynchronix/video_samples_vmaf"),
            PathBuf::from(&format!(
                "/home/fmaura/simulator_asynchronix/asynchronix/{}",
                CSV_FOLDER_STR
            )),
        ),
        // Local user
        "boris" => (
            PathBuf::from("/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf"),
            PathBuf::from(&format!(
                "/home/boris/Desktop/Rust_MG1/asynchronix/{}",
                CSV_FOLDER_STR
            )),
        ),
        // Fallback (optional)
        _ => (
            PathBuf::from("./video_samples_vmaf"),
            PathBuf::from(&format!("./{}", CSV_FOLDER_STR)),
        ),
    }
}

async fn encode_one_video(
    video_dir: PathBuf,
    csv_dir: PathBuf,
    width: usize,
    height: usize,
    gop_size: usize,
    intra_refresh: bool,
    framerate: u32,
    bitrate_mbps: f32,
    codec: VideoCodec,
    use_foveation: bool, 
    vbv_perframe: bool, 
) -> anyhow::Result<()> {
    let video_name = format!("{}_{}fps.mp4", VIDEO_NAME, framerate);
    let video_path = video_dir.join(&video_name);

    if !video_path.exists() {
        eprintln!("WARN: video not found: {}", video_path.display());
        return Ok(());
    }
    let codec_str = match codec {
        VideoCodec::AV1 => "AV1",
        VideoCodec::HEVC => "HEVC",
        // Add _ => "Unknown" if you expect other variants,
        // but typically these are the two main ones here.
    };

    let fov_val = if use_foveation { 1 } else { 0 };
    let ir_val = if intra_refresh { 1 } else { 0 };
    let vbv_val = if vbv_perframe { 1 } else { 0 };

    // NEW: Bake the variables into the intermediate CSV names
    let csv_path = csv_dir.join(format!(
        "{}_{}_{}Mbps_vbv{}_IR{}_foveated{}_framesizes.csv",
        codec_str,
        video_path.file_stem().unwrap().to_str().unwrap(),
        bitrate_mbps,
        vbv_val,
        ir_val,
        fov_val
    ));

    let mut enc = match codec {
        VideoCodec::AV1 => ChunkedEncoder::Av1(ChunkedAv1Encoder::new(
            video_path.to_str().unwrap(),
            width as u32,
            height as u32,
            &format!("{:.2}M", bitrate_mbps),
            CHUNK_DURATION, // chunk_seconds
            format!("[AV1 ENCODER [{}fps-{:.1}Mbps]", framerate, bitrate_mbps),
            0.0,
            framerate as f32,
            gop_size,
            intra_refresh,
            use_foveation, 
            vbv_perframe, 
        )),
        VideoCodec::HEVC => ChunkedEncoder::Hevc(ChunkedHevcEncoder::new(
            video_path.to_str().unwrap(),
            width as u32,
            height as u32,
            &format!("{:.2}M", bitrate_mbps),
            CHUNK_DURATION, // chunk_seconds
            format!("HEVC ENCODER [{}fps-{:.1}Mbps]", framerate, bitrate_mbps),
            0.0,
            framerate as f32,
            gop_size,
            intra_refresh,
            use_foveation, 
            vbv_perframe,             
        )),
        _ => ChunkedEncoder::HevcSoftware(ChunkedSoftwareHevcEncoder::new(
            video_path.to_str().unwrap(),
            width as u32,
            height as u32,
            &format!("{:.2}M", bitrate_mbps),
            CHUNK_DURATION, // chunk_seconds
            format!("[{}fps-{:.1}Mbps]", framerate, bitrate_mbps),
            0.0,
            framerate as f32,
            gop_size,
            intra_refresh,
            use_foveation, 
            vbv_perframe, 
        )),
    };

    let eye_gaze_model =  crate::lib::models_XR::EyeGazeModel {  // Random model of saccadic eye movement, same as used in simulation
                seed_offset: 0 as i64,         
                framerate: framerate as f32, 
                ..Default::default()
            }; 

    let mut wtr = Writer::from_path(&csv_path)?;
    wtr.write_record(&["frame_index", "bytes"])?;

    let mut global_idx: u64 = 0;
    let mut now = TaiTime::EPOCH;

    let mut current_gaze_time = Duration::ZERO;
    let frames_per_chunk = (framerate as f64 * CHUNK_DURATION).round() as usize;
    loop {
        let mut eye_samples = Vec::with_capacity(frames_per_chunk);

        for f in 0..frames_per_chunk {
            // Calculate the exact time of this specific frame within the stream
            let frame_time = current_gaze_time + Duration::from_secs_f64(f as f64 / framerate as f64);
            
            // Get [Option<Pose>; 2]
            let poses = eye_gaze_model.generate(frame_time);
            
            // Extract orientation to get [Option<Quat>; 2]
            let quats = poses.map(|opt_pose| opt_pose.map(|p| p.orientation));
            
            eye_samples.push(quats);
        }
        current_gaze_time += Duration::from_secs_f64(CHUNK_DURATION);
        enc.start_chunking(bitrate_mbps, now, eye_samples ).await;
        now = now + Duration::from_secs_f64(CHUNK_DURATION);

        let mut frames_this_chunk = 0usize;
        let mut idle_streak = 0usize;

        loop {
            if let Some(frame) = enc.next_frame().await {
                let sz = frame.len();
                wtr.write_record(&[global_idx.to_string(), sz.to_string()])?;
                global_idx += 1;
                frames_this_chunk += 1;
                idle_streak = 0;
            } else {
                idle_streak += 1;
                if idle_streak > 200 {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        }
        if global_idx % 256 == 0 {
            wtr.flush()?;
        }
        wtr.flush()?; // Ensure the last chunk of data hits the disk!

        if frames_this_chunk == 0 {
            println!(
                "[DONE] {} -> {}fps {:.1}Mbps → {} frames → {}",
                codec_str,
                framerate,
                bitrate_mbps,
                global_idx,
                csv_path.display()
            );
            break;
        }
    }

    Ok(())
}

use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;

#[derive(Debug, PartialEq, Eq, Hash)]
struct VideoGroup {
    name: String,
    fps: u32,
}
fn merge_csvs_into_one(
    codec_prefix: &str, 
    csv_dir: &Path, 
    use_foveation: bool, 
    use_intrarefresh: bool, 
    gop_size: usize, 
    vbv_perframe: bool 
) -> Result<(), Box<dyn Error>> {
    
    let fov_val = if use_foveation { 1 } else { 0 };
    let ir_val = if use_intrarefresh { 1 } else { 0 };
    let vbv_val = if vbv_perframe { 1 } else { 0 };

    let re_str = format!(
        r"{}_(.*)_(\d+)fps_([\d\.]+)Mbps_vbv{}_IR{}_foveated{}_framesizes\.csv", 
        codec_prefix, vbv_val, ir_val, fov_val
    );
    let re = Regex::new(&re_str)?;

    let mut groups: HashMap<VideoGroup, Vec<(f32, String)>> = HashMap::new();

    for entry in std::fs::read_dir(csv_dir)? {
        let path = entry?.path();
        let filename = path.file_name().unwrap().to_string_lossy();

        if let Some(cap) = re.captures(&filename) {
            let video_name = cap[1].to_string();
            let fps = cap[2].parse::<u32>()?;
            let mbps = cap[3].parse::<f32>()?; 

            let group = VideoGroup { name: video_name, fps };
            groups.entry(group).or_default().push((mbps, path.to_string_lossy().into_owned()));
        }
    }

    for (group, mut files) in groups {
        println!("[MERGING] Processing {} at {}fps for codec {}...", group.name, group.fps, codec_prefix);

        files.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let (first_mbps, first_path) = &files[0];
        let file = File::open(first_path)?;
        let mut combined_df = CsvReader::new(file).finish()?;
        
        let first_col_name = format!("{}Mbps", first_mbps);
        combined_df.rename("bytes", first_col_name.into())?;

        for (mbps, path) in files.iter().skip(1) {
            let next_file = File::open(path)?;
            let mut next_df = CsvReader::new(next_file)
                .finish()?
                .select(["frame_index", "bytes"])?;

            let next_col_name = format!("{}Mbps", mbps);
            next_df.rename("bytes", next_col_name.into())?;

            combined_df = combined_df.left_join(&next_df, ["frame_index"], ["frame_index"])?;
        }

        // NEW: Fixed string formatter to cleanly use 0/1 integers instead of precision args on bools
        let output_name = format!(
            "merged_{}_{}_{}fps_vbv{}_IR{}_foveated{}.csv", 
            codec_prefix, group.name, group.fps, vbv_val, ir_val, fov_val
        );

        let output_path = csv_dir.join(output_name);
        let mut out_file = File::create(&output_path)?;
        CsvWriter::new(&mut out_file).finish(&mut combined_df)?;

        println!("[DONE] Saved merged CSV to {}", output_path.display());
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let width = WIDTH_ENCODER;
    let height = HEIGHT_ENCODER;
    let gop_size: usize = 300;
    let intra_refresh = false;
    let use_foveation = true; 
    let vbv_perframe = false; 
    // let video_codec = VideoCodec::AV1;

    let (video_dir, csv_dir) = get_paths();

    if !csv_dir.exists() {
        println!("[INFO] Creating directory: {}", csv_dir.display());
        std::fs::create_dir_all(&csv_dir)?;
    }
    // ----------------------
    println!(
        "[INFO] Running as {} → VIDEO_DIR={}, CSV_DIR={}",
        std::env::var("USER").unwrap_or_default(),
        video_dir.display(),
        csv_dir.display()
    );

    // let br_values: Vec<f32> = (5..=100).step_by(5).map(|x| x as f32).collect();
    let br_values = vec![100.0]; 
    // let framerate_values = [60, 90, 120];
    // let codecs_to_run: [VideoCodec; 2] = [VideoCodec::AV1, VideoCodec::HEVC];

    let sem = Arc::new(Semaphore::new(NUM_SEMAPHORES)); // allow 5 encoders at a time
    let mut tasks = Vec::new();

    for video_codec in CODECS_TO_RUN {
        for framerate in FRAMERATE_VALUES {
            for &bitrate_mbps in &br_values {
                let video_dir = video_dir.clone();
                let csv_dir = csv_dir.clone();
                let permit = sem.clone().acquire_owned().await?;
                let task = tokio::spawn(async move {
                    let _permit = permit; // keep until task done
                    if let Err(e) = encode_one_video(
                        video_dir,
                        csv_dir,
                        width,
                        height,
                        gop_size,
                        intra_refresh,
                        framerate,
                        bitrate_mbps,
                        video_codec,
                        use_foveation, 
                        vbv_perframe ,
                    )
                    .await
                    {
                        eprintln!("[ERR] {}fps {:.1}Mbps → {e}", framerate, bitrate_mbps);
                    }
                });
                tasks.push(task);
            }
        }
    }

    // Wait for all tasks
    join_all(tasks).await;

    println!("[INFO] All encoding tasks completed. Starting CSV merge...");

    // Merge the resulting CSVs
    for video_codec in CODECS_TO_RUN {
        // Match string names identically to how you saved them in `encode_one_video`
        let codec_str = match video_codec {
            VideoCodec::AV1 => "AV1",
            VideoCodec::HEVC => "HEVC",
            // Add other variants if needed
        };
        // Pass the csv_dir reference so it knows where to look and save
        if let Err(e) = merge_csvs_into_one(codec_str, &csv_dir, use_foveation, intra_refresh, gop_size, vbv_perframe ) {
            eprintln!("[ERR] Failed to merge CSVs for {}: {}", codec_str, e);
        }
    }
    
    Ok(())
}
