use std::{path::Path, time::Duration};
use csv::Writer;
use tokio::{sync::Semaphore, time::sleep};
use tai_time::TaiTime;
use futures::future::join_all;
use std::sync::Arc; 
mod lib; 
// bring your types into scope (adjust these paths to your project)
use lib::alvr_stream_socket::ChunkedHevcEncoder;
use std::path::{PathBuf};
use crate::lib::models_XR::{HEIGHT_ENCODER, WIDTH_ENCODER};
use std::env; 


// Instead of consts, we use a small helper
fn get_paths() -> (PathBuf, PathBuf) {
    let user = env::var("USER").unwrap_or_else(|_| "unknown".to_string());
    match user.as_str() {
        // HPC user
        "fmaura" => (
            PathBuf::from("/home/fmaura/simulator_asynchronix/asynchronix/video_samples_vmaf"),
            PathBuf::from("/home/fmaura/simulator_asynchronix/asynchronix/csv_framesizes"),
        ),
        // Local user
        "boris" => (
            PathBuf::from("/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf"),
            PathBuf::from("/home/boris/Desktop/Rust_MG1/asynchronix/csv_framesizes"),
        ),
        // Fallback (optional)
        _ => (
            PathBuf::from("./video_samples_vmaf"),
            PathBuf::from("./csv_framesizes"),
        ),
    }
}


const CHUNK_DURATION : f64 = 5.0; 
const NUM_SEMAPHORES: usize = 5; // NUMBER OF PARALLEL TASKS. 

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let width = WIDTH_ENCODER;
    let height = HEIGHT_ENCODER;
    let gop_size: usize = 90;
    let intra_refresh = true;

    let (video_dir, csv_dir) = get_paths();
    println!(
        "[INFO] Running as {} → VIDEO_DIR={}, CSV_DIR={}",
        std::env::var("USER").unwrap_or_default(),
        video_dir.display(),
        csv_dir.display()
    );



    let br_values: Vec<f32> = (5..=100).step_by(5).map(|x| x as f32).collect();
    let sem = Arc::new(Semaphore::new(NUM_SEMAPHORES)); // allow 5 encoders at a time
    let mut tasks = Vec::new();

    for framerate in [60, 90, 120] {
        for &bitrate_mbps in &br_values {
            let video_dir = video_dir.clone();
            let csv_dir = csv_dir.clone();
            let permit = sem.clone().acquire_owned().await?;
            let task = tokio::spawn(async move {
                let _permit = permit; // keep until task done
                if let Err(e) = encode_one_video(video_dir, csv_dir ,width, height, gop_size, intra_refresh, framerate, bitrate_mbps).await {
                    eprintln!("[ERR] {}fps {:.1}Mbps → {e}", framerate, bitrate_mbps);
                }
            });
            tasks.push(task);
        }
    }

    // Wait for all tasks
    join_all(tasks).await;
    Ok(())
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
) -> anyhow::Result<()> {
    let video_name = format!("snow_{}fps.mp4", framerate);
    let video_path = video_dir.join(&video_name);

    if !video_path.exists() {
        eprintln!("WARN: video not found: {}", video_path.display());
        return Ok(());
    }

    let csv_path = csv_dir.join(format!(
        "{}_{}Mbps_framesizes.csv",
        video_path.file_stem().unwrap().to_str().unwrap(),
        bitrate_mbps
    ));

    let mut enc = ChunkedHevcEncoder::new(
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
    );

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

        if frames_this_chunk == 0 {
            println!(
                "[DONE] {}fps {:.1}Mbps → {} frames → {}",
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
