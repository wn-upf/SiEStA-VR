use csv::Writer;
use futures::future::join_all;
use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;
use std::{env, path::Path, path::PathBuf, time::Duration};
use tai_time::TaiTime;
use tokio::{sync::Semaphore, time::sleep};

mod lib;
use crate::lib::alvr_stream_socket::{ChunkedAv1Encoder, ChunkedEncoder, VideoCodec};
// bring your types into scope (adjust these paths to your project)
use crate::lib::models_XR::{HEIGHT_ENCODER, WIDTH_ENCODER};
use lib::alvr_stream_socket::{ChunkedHevcEncoder, ChunkedSoftwareHevcEncoder};

pub const CSV_FOLDER_STR: &str = "aaa_csv_framesizes";
pub const VIDEO_NAME: &str = "snow_short";

const CHUNK_DURATION: f64 = 5.0;
const NUM_SEMAPHORES: usize = 3; // NUMBER OF PARALLEL TASKS.

const FRAMERATE_VALUES: [u32; 3] = [60, 90, 120];
const CODECS_TO_RUN: [VideoCodec; 1] = [VideoCodec::HEVC];

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
        // Add _ => "Unknown" if you expect other variants
    };

    let base_name = video_path.file_stem().unwrap().to_str().unwrap();

    // Include intra and GOP in the filename to avoid overwrites
    let csv_path = csv_dir.join(format!(
        "{}_{}_intra{}_gop{}_{}Mbps_framesizes.csv",
        codec_str,
        base_name, // Typically "snow_short_60fps"
        intra_refresh,
        gop_size,
        bitrate_mbps
    ));

    let enc_log_tag = format!(
        "[{}fps-{:.1}Mbps-intra{}-gop{}]",
        framerate, bitrate_mbps, intra_refresh, gop_size
    );

    let mut enc = match codec {
        VideoCodec::AV1 => ChunkedEncoder::Av1(ChunkedAv1Encoder::new(
            video_path.to_str().unwrap(),
            width as u32,
            height as u32,
            &format!("{:.2}M", bitrate_mbps),
            CHUNK_DURATION,
            format!("AV1 ENCODER {}", enc_log_tag),
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
            CHUNK_DURATION,
            format!("HEVC ENCODER {}", enc_log_tag),
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
            CHUNK_DURATION,
            format!("HEVC SW {}", enc_log_tag),
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
                "[DONE] {} -> {} → {} frames → {}",
                codec_str,
                enc_log_tag,
                global_idx,
                csv_path.display()
            );
            break;
        }
    }

    Ok(())
}

// Expanded VideoGroup to group uniquely by intra and gop combinations
#[derive(Debug, PartialEq, Eq, Hash)]
struct VideoGroup {
    name: String,
    fps: u32,
    intra: bool,
    gop: usize,
}

fn merge_csvs_into_one(codec_prefix: &str, csv_dir: &Path) -> Result<(), Box<dyn Error>> {
    // Regex updated to capture intra(true|false) and gop sizes natively
    let re_str = format!(
        r"{}_(.*)_(\d+)fps_intra(true|false)_gop(\d+)_([\d\.]+)Mbps_framesizes\.csv",
        codec_prefix
    );
    let re = Regex::new(&re_str)?;

    let mut groups: HashMap<VideoGroup, Vec<(f32, String)>> = HashMap::new();

    for entry in std::fs::read_dir(csv_dir)? {
        let path = entry?.path();
        let filename = path.file_name().unwrap().to_string_lossy();

        if let Some(cap) = re.captures(&filename) {
            let video_name = cap[1].to_string();
            let fps = cap[2].parse::<u32>()?;
            let intra = cap[3].parse::<bool>()?;
            let gop = cap[4].parse::<usize>()?;
            let mbps = cap[5].parse::<f32>()?; 

            let group = VideoGroup {
                name: video_name,
                fps,
                intra,
                gop,
            };
            groups
                .entry(group)
                .or_default()
                .push((mbps, path.to_string_lossy().into_owned()));
        }
    }

    for (group, mut files) in groups {
        println!(
            "[MERGING] Processing {} at {}fps | Intra: {} | GoP: {} for codec {}...",
            group.name, group.fps, group.intra, group.gop, codec_prefix
        );

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

        // Updated filename layout to separate merged configurations
        let output_name = format!(
            "merged_{}_{}_{}fps_intra{}_gop{}.csv",
            codec_prefix, group.name, group.fps, group.intra, group.gop
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

    let (video_dir, csv_dir) = get_paths();

    if !csv_dir.exists() {
        println!("[INFO] Creating directory: {}", csv_dir.display());
        std::fs::create_dir_all(&csv_dir)?;
    }

    println!(
        "[INFO] Running as {} → VIDEO_DIR={}, CSV_DIR={}",
        std::env::var("USER").unwrap_or_default(),
        video_dir.display(),
        csv_dir.display()
    );

    let br_values: Vec<f32> = (5..=100).step_by(5).map(|x| x as f32).collect();

    // Define the new configurations to loop through
    let intra_options = [true, false];

    let sem = Arc::new(Semaphore::new(NUM_SEMAPHORES));
    let mut tasks = Vec::new();

    for video_codec in CODECS_TO_RUN {
        for framerate in FRAMERATE_VALUES {
            
            // Calculate Max Per-Chunk GoP
            // Frames per chunk = fps * chunk_duration (e.g. 60 * 5.0 = 300)
            let max_chunk_gop = (framerate as f64 * CHUNK_DURATION) as usize;

            for intra_refresh in intra_options {
                // Prevent unnecessary iterations: if intra_refresh is on, GoP size is ignored.
                // We only need to run one pass per bitrate, so we use max_chunk_gop as a placeholder.
                let current_gop_options = if intra_refresh {
                    vec![max_chunk_gop] 
                } else {
                    vec![60, 120, max_chunk_gop]
                };

                for &gop_size in &current_gop_options {
                    for &bitrate_mbps in &br_values {
                        let video_dir = video_dir.clone();
                        let csv_dir = csv_dir.clone();
                        let permit = sem.clone().acquire_owned().await?;
                        
                        let task = tokio::spawn(async move {
                            let _permit = permit;
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
                                eprintln!("[ERR] {}fps {:.1}Mbps Intra:{} GoP:{} → {e}", 
                                          framerate, bitrate_mbps, intra_refresh, gop_size);
                            }
                        });
                        tasks.push(task);
                    }
                }
            }
        }
    }
    
    join_all(tasks).await;
    println!("[INFO] All encoding tasks completed. Starting CSV merge...");

    for video_codec in CODECS_TO_RUN {
        let codec_str = match video_codec {
            VideoCodec::AV1 => "AV1",
            VideoCodec::HEVC => "HEVC",
        };
        
        if let Err(e) = merge_csvs_into_one(codec_str, &csv_dir) {
            eprintln!("[ERR] Failed to merge CSVs for {}: {}", codec_str, e);
        }
    }
    
    Ok(())
}