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
pub const VIDEO_NAME: &str = "snow";

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

const CHUNK_DURATION: f64 = 5.0;
const NUM_SEMAPHORES: usize = 5; // NUMBER OF PARALLEL TASKS.

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

    let csv_path = csv_dir.join(format!(
        "{}_{}_{}Mbps_framesizes.csv",
        codec_str,
        video_path.file_stem().unwrap().to_str().unwrap(),
        bitrate_mbps
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
        )),

        VideoCodec::HEVC => ChunkedEncoder::Hevc(ChunkedHevcEncoder::new(
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
        )),
    };

    let mut wtr = Writer::from_path(&csv_path)?;
    wtr.write_record(&["frame_index", "bytes"])?;

    let mut global_idx: u64 = 0;
    let mut now = TaiTime::EPOCH;
    loop {
        enc.start_chunking(bitrate_mbps, now).await;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let width = WIDTH_ENCODER;
    let height = HEIGHT_ENCODER;
    let gop_size: usize = 90;
    let intra_refresh = true;

    // let video_codec = VideoCodec::AV1;
    let codecs_to_run = [VideoCodec::AV1];

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

    let br_values: Vec<f32> = (5..=100).step_by(5).map(|x| x as f32).collect();
    let framerate_values = [60, 90, 120];

    let sem = Arc::new(Semaphore::new(NUM_SEMAPHORES)); // allow 5 encoders at a time
    let mut tasks = Vec::new();

    for video_codec in codecs_to_run {
        for framerate in framerate_values {
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

    for video_codec in codecs_to_run {
        let codec_str = format!("{}", video_codec);
        let _ = merge_csvs_into_one(&codec_str);
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

fn merge_csvs_into_one(codec_prefix: &str) -> Result<(), Box<dyn Error>> {
    // let codec_prefix = "HEVC";
    // Regex to capture: 1: Name, 2: FPS, 3: Mbps
    let re = Regex::new(&format!(
        r"{codec_prefix}_(.*)_(\d+)fps_(\d+)Mbps_framesizes\.csv",
    ))?;

    // Map to group files: Key -> Vec<(Mbps, Path)>c
    let mut groups: HashMap<VideoGroup, Vec<(u32, String)>> = HashMap::new();

    // 1. Scan directory and group files
    for entry in std::fs::read_dir("./bcopy")? {
        let path = entry?.path();
        let filename = path.file_name().unwrap().to_string_lossy();

        if let Some(cap) = re.captures(&filename) {
            let video_name = cap[1].to_string();
            let fps = cap[2].parse::<u32>()?;
            let mbps = cap[3].parse::<u32>()?;

            let group = VideoGroup {
                name: video_name,
                fps,
            };
            groups
                .entry(group)
                .or_default()
                .push((mbps, path.to_string_lossy().into_owned()));
        }
    }

    // 2. Process each group into its own CSV
    for (group, mut files) in groups {
        println!("Processing {} at {}fps...", group.name, group.fps);

        // Sort files by bitrate (Mbps)
        files.sort_by_key(|f| f.0);

        // Initialize DataFrame with the first file in the sorted group
        // ... inside your loop
        let (first_mbps, first_path) = &files[0];

        // FIX: Open file first, then pass to CsvReader::new()
        let file = File::open(first_path)?;
        let mut combined_df = CsvReader::new(file).finish()?;
        combined_df.rename("bytes", format!("{}Mbps", first_mbps).into())?;

        // Join subsequent files
        for (mbps, path) in files.iter().skip(1) {
            // FIX: Same here
            let next_file = File::open(path)?;
            let next_df = CsvReader::new(next_file)
                .finish()?
                .select(["frame_index", "bytes"])?;

            // Note: rename is usually done on the DataFrame after finish()
            let mut next_df = next_df;
            next_df.rename("bytes", format!("{}Mbps", mbps).into())?;

            combined_df = combined_df.left_join(&next_df, ["frame_index"], ["frame_index"])?;
        }

        // 3. Save the specific group file
        let output_name = format!("merged_{}_{}fps.csv", group.name, group.fps);
        let mut out_file = File::create(&output_name)?;
        CsvWriter::new(&mut out_file).finish(&mut combined_df)?;

        println!("Saved to {}", output_name);
    }

    Ok(())
}
