use anyhow::Result;
use ffmpeg_sidecar::command::FfmpegCommand;
use minifb::{Key, Scale, Window, WindowOptions};
use rand::Rng;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::ChildStdin;
use tokio::sync::mpsc::{
    bounded, error::TryRecvError, unbounded_channel, Receiver, Sender, UnboundedReceiver,
};
use tokio::time;

pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;
pub const INITIAL_BITRATE: &str = "2M";

fn convert_rgb_to_u32(rgb_data: &[u8], width: usize, height: usize) -> Vec<u32> {
    if rgb_data.len() != width * height * 3 {
        eprintln!(
            "Unexpected RGB data length. Expected {}, got {}",
            width * height * 3,
            rgb_data.len()
        );
        return Vec::new();
    }
    rgb_data
        .chunks_exact(3)
        .map(|chunk| {
            let r = chunk[0] as u32;
            let g = chunk[1] as u32;
            let b = chunk[2] as u32;
            (r << 16) | (g << 8) | b
        })
        .collect()
}

fn scale_pixels(
    buffer: &[u32],
    orig_width: usize,
    orig_height: usize,
    new_width: usize,
    new_height: usize,
) -> Vec<u32> {
    let mut scaled = vec![0u32; new_width * new_height];
    for y in 0..new_height {
        let orig_y = y * orig_height / new_height;
        for x in 0..new_width {
            let orig_x = x * orig_width / new_width;
            scaled[y * new_width + x] = buffer[orig_y * orig_width + orig_x];
        }
    }
    scaled
}

pub struct HevcEncoder {
    packet_rx: UnboundedReceiver<Vec<u8>>,
    _child: ffmpeg_sidecar::child::FfmpegChild,
}

impl HevcEncoder {
    pub async fn new(input: &str, width: u32, height: u32, bitrate: &str) -> Result<Self> {
        let mut child = FfmpegCommand::new()
            .hwaccel("cuvid")
            .args(&["-re"])
            .args(&["-stream_loop", "-1"])
            .input(input)
            .args(&[
                "-vf",
                &format!(
                    "scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p",
                    width, height
                ),
            ])
            .args(&["-c:v", "hevc_nvenc"])
            .args(&["-preset", "fast"])
            .args(&["-rc", "cbr"])
            .args(&["-b:v", bitrate, "-maxrate", bitrate])
            .args(&["-rc-lookahead", "0"])
            .args(&["-g", "60"])
            .args(&["-movflags", "+frag_keyframe+empty_moov"])
            .args(&["-flush_packets", "1"])
            .args(&["-bsf:v", "hevc_mp4toannexb"])
            .args(&["-an"])
            .args(&["-f", "mp4", "-"])
            .spawn()?;

        let stdout = child.take_stdout().unwrap();
        let stderr = child.take_stderr().unwrap();

        let (packet_tx, packet_rx) = unbounded_channel();

        tokio::spawn(async move {
            let mut async_stdout = tokio::process::ChildStdout::from_std(stdout).unwrap();
            let mut reader = BufReader::new(async_stdout);
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if packet_tx.send(buf[..n].to_vec()).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Encoder read error: {}", e);
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            let mut async_stderr = tokio::process::ChildStderr::from_std(stderr).unwrap();
            let mut reader = BufReader::new(async_stderr);
            let mut buf = String::new();
            loop {
                match reader.read_to_string(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => eprint!("{}", buf),
                    Err(e) => {
                        eprintln!("Encoder stderr read error: {}", e);
                        break;
                    }
                }
                buf.clear();
            }
        });

        Ok(Self {
            packet_rx,
            _child: child,
        })
    }

    pub async fn try_next_packet(&mut self) -> Result<Option<Vec<u8>>> {
        match self.packet_rx.try_recv() {
            Ok(packet) => Ok(Some(packet)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(anyhow::anyhow!("Encoder channel disconnected")),
        }
    }
}

pub struct HevcDecoder {
    frame_rx: UnboundedReceiver<Vec<u8>>,
    packet_tx: Sender<Vec<u8>>,
    width: u32,
    height: u32,
}

impl HevcDecoder {
    pub async fn new(framerate: u32, width: u32, height: u32) -> Result<Self> {
        let frame_size = (width as usize) * (height as usize) * 3;
        let mut child = FfmpegCommand::new()
            .hwaccel("auto")
            .args(&["-f", "mp4", "-i", "-"])
            .args(&["-vf", &format!("fps={}", framerate)])
            .args(&["-pix_fmt", "rgb24"])
            .args(&["-f", "rawvideo", "-"])
            .spawn()?;

        let stdout = child.take_stdout().unwrap();
        let stdin = child.take_stdin().unwrap();
        let stderr = child.take_stderr().unwrap();

        let (frame_tx, frame_rx) = unbounded_channel();
        let (packet_tx, mut packet_rx) = bounded(100);

        tokio::spawn({
            let frame_size = frame_size;
            async move {
                let mut async_stdout = tokio::process::ChildStdout::from_std(stdout).unwrap();
                let mut reader = BufReader::new(async_stdout);
                let mut buffer = Vec::with_capacity(frame_size * 2);
                let mut chunk = vec![0u8; 4096];
                loop {
                    match reader.read(&mut chunk).await {
                        Ok(0) => break,
                        Ok(n) => {
                            buffer.extend_from_slice(&chunk[..n]);
                            while buffer.len() >= frame_size {
                                let frame = buffer.drain(..frame_size).collect();
                                if frame_tx.send(frame).is_err() {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Decoder read error: {}", e);
                            break;
                        }
                    }
                }
            }
        });

        tokio::spawn(async move {
            let mut async_stdin = tokio::process::ChildStdin::from_std(stdin).unwrap();
            while let Some(packet) = packet_rx.recv().await {
                if let Err(e) = async_stdin.write_all(&packet).await {
                    eprintln!("Decoder write error: {}", e);
                    break;
                }
            }
        });

        tokio::spawn(async move {
            let mut async_stderr = tokio::process::ChildStderr::from_std(stderr).unwrap();
            let mut reader = BufReader::new(async_stderr);
            let mut buf = String::new();
            loop {
                match reader.read_to_string(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => eprint!("{}", buf),
                    Err(e) => {
                        eprintln!("Decoder stderr read error: {}", e);
                        break;
                    }
                }
                buf.clear();
            }
        });

        Ok(Self {
            frame_rx,
            packet_tx,
            width,
            height,
        })
    }

    pub async fn try_next_frame(&mut self) -> Result<Option<Vec<u8>>> {
        match self.frame_rx.try_recv() {
            Ok(frame) => Ok(Some(frame)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                Err(anyhow::anyhow!("Decoder frame channel disconnected"))
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    ffmpeg_sidecar::download::auto_download()?;
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/cut_video.mp4";

    let mut encoder = HevcEncoder::new(
        input_path,
        WIDTH_ENCODER as u32,
        HEIGHT_ENCODER as u32,
        INITIAL_BITRATE,
    )
    .await?;
    let mut decoder = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32).await?;

    let drop_probability = 0.000;
    let mut rng = rand::thread_rng();

    let scale_factor = 0.8;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;

    let mut window = Window::new(
        "Decoded HEVC with Dropped Frames",
        scaled_width,
        scaled_height,
        WindowOptions::default(),
    )?;

    let frame_duration = Duration::from_secs_f64(1.0 / 60.0);
    let mut next_frame_time = time::Instant::now();
    let mut t = Instant::now();
    let mut number_fps: usize = 0;

    while window.is_open() && !window.is_key_down(Key::Escape) {
        while let Ok(Some(packet)) = encoder.try_next_packet().await {
            if rng.gen::<f64>() >= drop_probability {
                if let Err(e) = decoder.packet_tx.send(packet).await {
                    eprintln!("Failed to send packet to decoder: {}", e);
                }
            }
        }

        if let Ok(Some(frame)) = decoder.try_next_frame().await {
            let pixels = convert_rgb_to_u32(&frame, WIDTH_ENCODER, HEIGHT_ENCODER);
            let scaled = scale_pixels(
                &pixels,
                WIDTH_ENCODER,
                HEIGHT_ENCODER,
                scaled_width,
                scaled_height,
            );
            window.update_with_buffer(&scaled, scaled_width, scaled_height)?;
            number_fps += 1;
        }

        if t.elapsed() >= Duration::from_secs(2) {
            println!("FPS COUNTER AVG 2secs: {}", number_fps / 2);
            number_fps = 0;
            t = Instant::now();
        }

        let now = time::Instant::now();
        if now < next_frame_time {
            time::sleep(next_frame_time - now).await;
        }
        next_frame_time += frame_duration;
    }

    Ok(())
}
