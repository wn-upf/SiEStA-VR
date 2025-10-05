use std::{path::Path, time::Duration};
use csv::Writer;
use tokio::time::sleep;
use tai_time::TaiTime;

mod lib; 
// bring your types into scope (adjust these paths to your project)
use lib::alvr_stream_socket::ChunkedHevcEncoder;
use std::path::{PathBuf};
use crate::lib::models_XR::{HEIGHT_ENCODER, WIDTH_ENCODER};

const VIDEO_DIR: &str = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf";
const CSV_DIR:   &str = "/home/boris/Desktop/Rust_MG1/asynchronix/csv_framesizes";

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    // ---- Init encoder (tweak params to your defaults) ----
    // width/height here are examples; pick the size you want to encode to
    let width = WIDTH_ENCODER;
    let height = HEIGHT_ENCODER;
    // let framerate: f32 = 60.0;       // your source/output FPS
    let gop_size: usize = 90;        // one GOP per second (example)
    let intra_refresh = true;       // or true if you want PIR mode


    let br_values : Vec<f32> = (5..=100)
    .step_by(5)
    .map(|x| x as f32)
    .collect();
 


    for framerate in [60,90,120] {
        for bitrate_mbps in br_values.iter(){

            
            let chunk_seconds = 1.0;         // encode/read in 5s chunks


            let input = "/home/boris/Desktop/Rust_MG1/asynchronix/csv_framesizes/";
            let video_name = format!("snow_{}fps.mp4", framerate);
            let video_path: PathBuf = Path::new(VIDEO_DIR).join(&video_name);

            // Optional: check the file exists
            if !video_path.exists() {
                eprintln!("WARN: video not found: {}", video_path.display());
                continue;
            }

            // ---- Derive CSV path from the *video* stem, placed under csv_framesizes/
            let stem = video_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("video");
            let csv_path = Path::new(CSV_DIR).join(format!("{}_{}Mbps_framesizes.csv", stem, *bitrate_mbps));


            let mut enc = ChunkedHevcEncoder::new(
                &video_path.to_str().unwrap(), 
                width as u32,
                height as u32,
                &format!("{:.2}M", bitrate_mbps),
                chunk_seconds,
                "[FRAME-LOGGER]".to_string(),
                5.0,                // start at t=0s
                framerate as f32,
                gop_size,
                intra_refresh,
            );

            // ---- CSV writer with header ----
            let mut wtr: Writer<std::fs::File> = Writer::from_path(&csv_path)?;
            wtr.write_record(&["frame_index", "bytes"])?;

            // ---- loop: start a chunk, drain frames, repeat until no more ----
            let mut global_idx: u64 = 0;
            let mut empty_chunks_in_a_row = 0usize;

            // A "now" for logging (doesn't matter for encoding; use zero)
            let mut now = TaiTime::EPOCH;

            // --- loop until ffmpeg ends naturally ---
            loop {
                // Start a new chunk at the encoder's internal offset (it increments each time)
                enc.start_chunking(*bitrate_mbps, now).await;

                print!(" Global IDX = {}", global_idx); 

                now =  now + Duration::from_secs_f64(chunk_seconds); 


                let mut frames_this_chunk = 0usize;
                let mut idle_streak = 0usize;

                loop {
                    if let Some(frame) = enc.next_frame().await {
                        let sz = frame.len();
                        // println!("Got frame of size {}. Global: {}, this chunk: {}", sz, global_idx, frames_this_chunk); 
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
                if global_idx % 256 == 0{
                    wtr.flush()?;
                }


                if frames_this_chunk == 0 {
                    // no frames this chunk → video likely ended
                    println!("NO FRAMEEEEES"); 
                    break;
                }
            }

            println!("Wrote {} frames to {}", global_idx, csv_path.display());
        }
    }
    Ok(())

}
