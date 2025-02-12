use anyhow::Result;
use std::io::{BufReader, Read, Write};
use std::process::{ChildStdin, ChildStdout};
use ffmpeg_sidecar::command::FfmpegCommand;
use minifb::{Key, Scale, Window, WindowOptions};
use rand::Rng;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use std::time::Duration;
use std::time::Instant;
// Define the expected (encoder) dimensions.
pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;

pub const INITIAL_BITRATE : &str= "2M"; 

/// Converts raw RGB byte data (3 bytes per pixel) into a Vec<u32> pixel buffer
/// where each pixel is represented as 0xRRGGBB.
fn convert_rgb_to_u32(rgb_data: &[u8], width: usize, height: usize) -> Vec<u32> {
    let expected_len = width * height * 3;
    if rgb_data.len() != expected_len {
        eprintln!(
            "Unexpected RGB data length. Expected {}, got {}",
            expected_len,
            rgb_data.len()
        );
        return Vec::new();
    }
    
    let mut pixels = Vec::with_capacity(width * height);
    for chunk in rgb_data.chunks_exact(3) {
        let pixel = ((chunk[0] as u32) << 16) | 
                   ((chunk[1] as u32) << 8) | 
                   (chunk[2] as u32);
        pixels.push(pixel);
    }

    if !pixels.is_empty() {
        println!("First 5 pixels: {:x} {:x} {:x} {:x} {:x}", 
            pixels[0], pixels[1], pixels[2], pixels[3], pixels[4]);
    }
    pixels
}


fn scale_pixels(buffer: &[u32], orig_width: usize, orig_height: usize, new_width: usize, new_height: usize) -> Vec<u32> {
    let mut scaled = vec![0u32; new_width * new_height];
    let x_ratio = (orig_width << 16) / new_width;
    let y_ratio = (orig_height << 16) / new_height;

    for y in 0..new_height {
        let y2 = ((y * y_ratio) >> 16) * orig_width;
        for x in 0..new_width {
            let x2 = (x * x_ratio) >> 16;
            scaled[y * new_width + x] = buffer[y2 + x2];
        }
    }
    scaled
}

pub struct HevcEncoder {
    packet_rx: Receiver<Vec<u8>>,
    _child: ffmpeg_sidecar::child::FfmpegChild,
    _stderr_handle: std::thread::JoinHandle<()>,
}
impl HevcEncoder {
    pub fn new(input: &str, width: u32, height: u32, bitrate: &str) -> Result<Self> {
        let mut child = FfmpegCommand::new()
            .hwaccel("cuvid")
            .args(&["-re"]) // Read input at real-time speed
            .args(&["-stream_loop", "-1"]) // Loop input indefinitely
            .input(input)
            .args(&["-vf", &format!("scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p", width, height)])
            .args(&["-c:v", "hevc_nvenc"])
            .args(&["-preset", "fast"])
            .args(&["-rc", "cbr"])
            .args(&["-b:v", bitrate, "-maxrate", bitrate])
            // .args(&["-r", "60"]) // Specify the output frame rate (60 FPS)
            .args(&["-rc-lookahead", "0"])
            .args(&["-g", "60"])
            // .args(&["-threads", "5"])
            .args(&["-movflags", "+frag_keyframe+empty_moov"])
            .args(&["-flush_packets", "1"])
            .args(&["-bsf:v", "hevc_mp4toannexb"])
            .args(&["-an"])
            .args(&["-f", "mp4", "-"])
            .spawn()?;

        let stdout = child.take_stdout().unwrap();
        let stderr = child.take_stderr().unwrap();

        let (packet_tx, packet_rx) = unbounded();

        // Start stdout reader thread
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        packet_tx.send(buf[..n].to_vec()).unwrap();
                    }
                    Err(e) => {
                        eprintln!("Encoder read error: {}", e);
                        break;
                    }
                }
            }
        });

        // Start stderr monitor thread
        let stderr_handle = std::thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut buf = String::new();
            loop {
                buf.clear();
                match reader.read_to_string(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => eprint!("{}", buf),
                    Err(e) => {
                        eprintln!("Encoder stderr read error: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(Self {
            packet_rx,
            _child: child,
            _stderr_handle: stderr_handle,
        })
    }

    pub fn try_next_packet(&self) -> Result<Option<Vec<u8>>> {
        match self.packet_rx.try_recv() {
            Ok(packet) => Ok(Some(packet)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(anyhow::anyhow!("Encoder channel disconnected")),
        }
    }
}

pub struct HevcDecoder {
    frame_rx: Receiver<Vec<u8>>,
    // packet_tx: Sender<Vec<u8>>,
    packet_tx: Sender<Vec<u8>>,  // Changed from [u8] to Vec<u8>
    _stdin_handle: std::thread::JoinHandle<()>,
    _stderr_handle: std::thread::JoinHandle<()>,
    width: u32,
    height: u32,
}

impl HevcDecoder {
    pub fn new(framerate: u32, width: u32, height: u32) -> Result<Self> {
        let frame_size = (width as usize) * (height as usize) * 3;
        let mut child = FfmpegCommand::new()
            .hwaccel("auto")
            .args(&["-f", "mp4", "-i", "-"])
            // .args(&["-c:v", "hevc"])
            .args(&["-vf", &format!("fps={}", framerate)])
            .args(&["-pix_fmt", "rgb24"])
            .args(&["-f", "rawvideo", "-"])
            .spawn()?;

        let stdout = child.take_stdout().unwrap();
        let stdin = child.take_stdin().unwrap();
        let stderr = child.take_stderr().unwrap();

    // Changed: Explicitly specify Vec<u8> type for the channel
    let (frame_tx, frame_rx) = unbounded::<Vec<u8>>();
    let (packet_tx, packet_rx) = bounded::<Vec<u8>>(100);  // Added type parameter
        // Start stdout reader thread
        std::thread::spawn({
            let frame_size = frame_size;
            move || {
                let mut reader = BufReader::new(stdout);
                let mut buffer = Vec::with_capacity(frame_size * 2);
                let mut chunk = vec![0u8; 4096];
                loop {
                    match reader.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => {
                            buffer.extend_from_slice(&chunk[..n]);
                            while buffer.len() >= frame_size {
                                let frame = buffer.drain(..frame_size).collect();
                                frame_tx.send(frame).unwrap();
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

        let stdin_handle = std::thread::spawn(move || {
            let mut writer = stdin;
            for packet in packet_rx {
                if let Err(e) = writer.write_all(&packet) {  // packet is Vec<u8> here
                    eprintln!("Decoder write error: {}", e);
                    break;
                }
            }
        });

        // Start stderr monitor thread
        let stderr_handle = std::thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut buf = String::new();
            loop {
                buf.clear();
                match reader.read_to_string(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => eprint!("{}", buf),
                    Err(e) => {
                        eprintln!("Decoder stderr read error: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(Self {
            frame_rx,
            packet_tx,
            _stdin_handle: stdin_handle,
            _stderr_handle: stderr_handle,
            width,
            height,
        })
    }

    pub fn try_next_frame(&self) -> Result<Option<Vec<u8>>> {
        match self.frame_rx.try_recv() {
            Ok(frame) => Ok(Some(frame)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(anyhow::anyhow!("Decoder frame channel disconnected")),
        }
    }
}

fn main() -> Result<()> {
    ffmpeg_sidecar::download::auto_download()?;
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/cut_video.mp4";
    println!("HIHI!!!"); 



    let mut encoder = HevcEncoder::new(input_path, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, &INITIAL_BITRATE)?;
    let decoder = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32)?;
    println!("SET UP ENCODE DECODE"); 
    let drop_probability = 0.000;
    let mut rng = rand::thread_rng();

    let scale_factor = 0.8;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;

    let mut window = Window::new(
        "Video Stream",
        scaled_width,
        scaled_height,
        WindowOptions::default(),
    )?;
    println!("WINDOW OPEN"); 
    // Target 60 FPS (16.67ms per frame)
    let frame_duration = std::time::Duration::from_secs_f64(1.0 / 60.0);
    let mut next_frame_time = std::time::Instant::now();
    let mut t = Instant::now(); 
    let mut number_fps: usize = 0; // var to keep track somehow of fps


    while window.is_open() && !window.is_key_down(Key::Escape) {
        // Process encoder packets
        // print!("RUN!"); 
        while let Ok(Some(packet)) = encoder.try_next_packet() {
            // if rng.gen::<f64>() >= drop_probability {
                // std::thread::sleep(Duration::from_millis(100)); // Adjust this value based on desired speed
                decoder.packet_tx.send(packet)?;
            // }
        }
        // Process decoder frames
        if Instant::now().duration_since(t) >= Duration::from_secs(2){
            t = Instant::now();
            println!("FPS COUNTER AVG 2secs: {}", number_fps / 2); 
            number_fps = 0; 
        }
        // println!("{}", number_fps); 
        if let Ok(Some(frame)) = decoder.try_next_frame() {
            let pixels = convert_rgb_to_u32(&frame, WIDTH_ENCODER, HEIGHT_ENCODER);
            let scaled = scale_pixels(&pixels, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
            window.update_with_buffer(&scaled, scaled_width, scaled_height)?;
            number_fps += 1; 

        }


        // // Maintain frame rate
        // let now = std::time::Instant::now();
        // if now < next_frame_time {
        //     std::thread::sleep(next_frame_time - now);
        // }
        next_frame_time += frame_duration;
    }

    Ok(())
}