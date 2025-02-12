use anyhow::Result;
use std::io::{BufReader, Read, Write};
use std::process::{ChildStdin, ChildStdout};
use ffmpeg_sidecar::command::FfmpegCommand;
use minifb::{Key, Scale, Window, WindowOptions};
use std::time::Duration;
use sdl2::libc;

pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;

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

fn scale_pixels(
    buffer: &[u32],
    orig_width: usize,
    orig_height: usize,
    new_width: usize,
    new_height: usize,
) -> Vec<u32> {
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
    reader: BufReader<ChildStdout>,
    _child: ffmpeg_sidecar::child::FfmpegChild,
    stderr: std::process::ChildStderr,
}

impl HevcEncoder {
    pub fn new(input: &str, width: u32, height: u32, bitrate: &str) -> Result<Self> {
        if !std::path::Path::new(input).exists() {
            return Err(anyhow::anyhow!("Input file does not exist: {}", input));
        }

        let mut ffmpeg_cmd_builder = FfmpegCommand::new(); 
        let mut ffmpeg_cmd = ffmpeg_cmd_builder
            .args(&["-stream_loop", "-1"])
            .args(&["-loglevel", "warning"])
            .input(input)
            .args(&["-vf", &format!("scale={}:{}", width, height)])
            .args(&["-c:v", "libx265"])
            .args(&["-preset", "ultrafast"])
            .args(&["-tune", "zerolatency"])
            .args(&["-x265-params", "log-level=error:keyint=30:min-keyint=30:scenecut=0"])
            .args(&["-pix_fmt", "yuv420p"])
            .args(&["-b:v", bitrate])
            .args(&["-maxrate", bitrate])
            .args(&["-minrate", bitrate])
            .args(&["-bufsize", "1M"])  // Reduced buffer size for lower latency
            .args(&["-rc", "cbr"])
            .args(&["-an"])
            .args(&["-f", "mpegts", "pipe:1"]);

        println!("Encoder command: {:?}", ffmpeg_cmd);
        let mut child = ffmpeg_cmd.spawn()?;
        
        let stdout = child.take_stdout().ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;
        let stderr = child.take_stderr().ok_or_else(|| anyhow::anyhow!("Failed to capture stderr"))?;

        Ok(HevcEncoder {
            reader: BufReader::new(stdout),
            _child: child,
            stderr,
        })
    }

    pub fn next_packet(&mut self) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; 131072];  // Increased buffer size for TS packets
        match self.reader.read(&mut buf) {
            Ok(0) => Ok(None),
            Ok(n) => {
                buf.truncate(n);
                println!("Read encoded packet of size: {} bytes", n);
                Ok(Some(buf))
            }
            Err(e) => Err(e.into())
        }
    }

    pub fn check_errors(&mut self) -> Result<()> {
        let mut buf = [0u8; 4096];
        if let Ok(n) = self.stderr.read(&mut buf) {
            if n > 0 {
                let stderr_output = String::from_utf8_lossy(&buf[..n]);
                if !stderr_output.contains("ffmpeg version") {
                    eprintln!("Encoder stderr: {}", stderr_output);
                }
            }
        }
        Ok(())
    }
}

pub struct HevcDecoder {
    reader: BufReader<ChildStdout>,
    writer: ChildStdin,
    frame_size: usize,
    width: u32,
    height: u32,
    stderr: std::process::ChildStderr,
}

impl HevcDecoder {
    pub fn new(framerate: u32, width: u32, height: u32) -> Result<Self> {
        let frame_size: usize = (width as usize) * (height as usize) * 3;
        let mut ffmpeg_cmd_builder = FfmpegCommand::new(); 
          let frame_size = (width as usize) * (height as usize) * 3;
        
        let ffmpeg_cmd = ffmpeg_cmd_builder
            .args(&["-hwaccel", "cuda"])
            .args(&["-c:v", "hevc_cuvid"])
            .args(&["-i", "pipe:0"])
            .args(&["-fps_mode", "cfr"])
            .args(&["-fflags", "+nobuffer"])
            .args(&["-flags", "low_delay"])
            .args(&["-strict", "experimental"])
            .args(&["-vf", &format!(
                "hwdownload,format=nv12,fps={},scale_cuda={}:{}:format=yuv420p,hwdownload,format=rgb24",
                framerate, width, height
            )])
            .args(&["-f", "rawvideo"])
            .args(&["-pix_fmt", "rgb24"])
            .args(&["pipe:1"]);

        println!("Decoder command: {:?}", ffmpeg_cmd);
        let mut child = ffmpeg_cmd.spawn()?;
        
        let stdout = child.take_stdout().ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;
        let stdin = child.take_stdin().ok_or_else(|| anyhow::anyhow!("Failed to capture stdin"))?;
        let stderr = child.take_stderr().ok_or_else(|| anyhow::anyhow!("Failed to capture stderr"))?;

        Ok(HevcDecoder {
            reader: BufReader::new(stdout),
            writer: stdin,
            frame_size,
            width,
            height,
            stderr,
        })
    }

    pub fn feed_packet(&mut self, packet: &[u8]) -> Result<()> {
        self.writer.write_all(packet)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>> {
        let mut frame = vec![0u8; self.frame_size];
        
        use std::os::unix::io::AsRawFd;
        let fd = self.reader.get_ref().as_raw_fd();
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFL);
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        match self.reader.read_exact(&mut frame) {
            Ok(()) => Ok(Some(frame)),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn check_errors(&mut self) -> Result<()> {
        let mut buf = [0u8; 4096];
        if let Ok(n) = self.stderr.read(&mut buf) {
            if n > 0 {
                eprintln!("Decoder stderr: {}", String::from_utf8_lossy(&buf[..n]));
            }
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    ffmpeg_sidecar::download::auto_download()?;

    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_sunflower_2160p_60fps_stereo_abl.mp4";
    let width = 1920;
    let height = 1080;
    
    let mut encoder = HevcEncoder::new(input_path, width, height, "2M")?;
    let mut decoder = HevcDecoder::new(60, width, height)?;

    let scale_factor = 0.4;
    let scaled_width = (width as f64 * scale_factor) as usize;
    let scaled_height = (height as f64 * scale_factor) as usize;

    let mut window = Window::new(
        "Video Stream",
        scaled_width,
        scaled_height,
        WindowOptions {
            scale: Scale::X1,
            ..WindowOptions::default()
        },
    )?;

    let mut frames_processed = 0;
    let start_time = std::time::Instant::now();
    let mut last_frame_time = start_time;
    while window.is_open() && !window.is_key_down(Key::Escape) {
        // Process encoder packets
        if let Some(packet) = encoder.next_packet()? {
            println!("Sending packet size: {}", packet.len());
            if let Err(e) = decoder.feed_packet(&packet) {
                eprintln!("Packet feed error: {}", e);
            }
        }

        // Process decoder frames with timeout
        let start_read = std::time::Instant::now();
        while start_read.elapsed() < Duration::from_millis(33) {
            match decoder.next_frame() {
                Ok(Some(frame_bytes)) => {
                    println!("Received frame: {} bytes", frame_bytes.len());
                    
                    // Validate frame size
                    if frame_bytes.len() != (width * height * 3) as usize {
                        eprintln!("Invalid frame size: {} (expected {})", 
                            frame_bytes.len(), width * height * 3);
                        continue;
                    }

                    // Convert and display
                    let pixels = convert_rgb_to_u32(&frame_bytes, width as usize, height as usize);
                    if !pixels.is_empty() {
                        let scaled_pixels = scale_pixels(
                            &pixels,
                            width as usize,
                            height as usize,
                            scaled_width,
                            scaled_height
                        );
                        
                        window.update_with_buffer(&scaled_pixels, scaled_width, scaled_height)?;
                        frames_processed += 1;
                    }
                }
                Ok(None) => break, // No more frames available
                Err(e) => eprintln!("Frame read error: {}", e),
            }
        }

        encoder.check_errors()?;
        decoder.check_errors()?;
        std::thread::sleep(Duration::from_millis(1));
    }

    Ok(())
}