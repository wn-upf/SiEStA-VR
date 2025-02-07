extern crate ffmpeg_next as ffmpeg;
extern crate minifb;

use ffmpeg::format::{input, Pixel};
use ffmpeg::media::Type;
use ffmpeg::software::scaling::{context::Context, flag::Flags};
use ffmpeg::util::frame::video::Video;
use minifb::{Window, WindowOptions};
use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;

fn convert_rgb_to_u32(rgb: &[u8], width: usize, height: usize) -> Vec<u32> {
    let mut buffer = Vec::with_capacity(width * height);

    for y in 0..height {
        for x in 0..width {
            let r = rgb[(y * width + x) * 3] as u32;
            let g = rgb[(y * width + x) * 3 + 1] as u32;
            let b = rgb[(y * width + x) * 3 + 2] as u32;
            let a = 255u32; // Assume full alpha for RGB to RGBA conversion
            buffer.push((a << 24) | (r << 16) | (g << 8) | b); // ARGB format
        }
    }

    buffer
}

fn main() -> Result<(), ffmpeg::Error> {
    ffmpeg::init().unwrap();
    if let Ok(mut ictx) = input(&PathBuf::from(
        "/home/boris/Desktop/Rust_MG1/asynchronix/BigBuckBunny.mp4",
    )) {
        let input = ictx
            .streams()
            .best(Type::Video)
            .ok_or(ffmpeg::Error::StreamNotFound)?;
        let video_stream_index = input.index();

        let context_decoder = ffmpeg::codec::context::Context::from_parameters(input.parameters())?;
        let mut decoder = context_decoder.decoder().video()?;

        let mut scaler = Context::get(
            decoder.format(),
            decoder.width(),
            decoder.height(),
            Pixel::RGB24,
            decoder.width(),
            decoder.height(),
            Flags::BILINEAR,
        )?;

        let mut frame_index = 0;

        let mut input_window = match Window::new(
            "Input Video",
            decoder.width() as usize,
            decoder.height() as usize,
            WindowOptions::default(),
        ) {
            Ok(window) => window,
            Err(e) => {
                eprintln!("Error creating input window: {}", e);
                return Err(ffmpeg::Error::DecoderNotFound); // Or another appropriate error
            }
        };

        let mut output_window = match Window::new(
            "Decoded Video",
            decoder.width() as usize,
            decoder.height() as usize,
            WindowOptions::default(),
        ) {
            Ok(window) => window,
            Err(e) => {
                eprintln!("Error creating output window: {}", e);
                return Err(ffmpeg::Error::DecoderNotFound); // Or another appropriate error
            }
        };
        // Store frames as RGB data for both windows
        let mut input_frame_rgb = vec![0u8; (decoder.width() * decoder.height() * 3) as usize];
        let mut decoded_frame_rgb = vec![0u8; (decoder.width() * decoder.height() * 3) as usize];

        // Convert RGB frame to u32 format for minifb
        let input_frame_u32 = convert_rgb_to_u32(
            &input_frame_rgb,
            decoder.width() as usize,
            decoder.height() as usize,
        );

        let decoded_frame_u32 = convert_rgb_to_u32(
            &decoded_frame_rgb,
            decoder.width() as usize,
            decoder.height() as usize,
        );
        output_window
            .update_with_buffer(
                &decoded_frame_u32,
                decoder.width() as usize,
                decoder.height() as usize,
            )
            .unwrap();

        let mut receive_and_process_decoded_frames =
            |decoder: &mut ffmpeg::decoder::Video| -> Result<(), ffmpeg::Error> {
                let mut decoded = Video::empty();
                while decoder.receive_frame(&mut decoded).is_ok() {
                    // Scale decoded frame to RGB
                    let mut rgb_frame = Video::empty();
                    scaler.run(&decoded, &mut rgb_frame)?;

                    // Copy input frame to the input window buffer
                    input_frame_rgb.copy_from_slice(rgb_frame.data(0));

                    // Convert RGB frame to u32 format for minifb
                    let input_frame_u32 = convert_rgb_to_u32(
                        &input_frame_rgb,
                        decoder.width() as usize,
                        decoder.height() as usize,
                    );

                    // Display the input video in the window
                    input_window
                        .update_with_buffer(
                            &input_frame_u32,
                            decoder.width() as usize,
                            decoder.height() as usize,
                        )
                        .unwrap();

                    // Copy decoded frame to the output window buffer
                    decoded_frame_rgb.copy_from_slice(rgb_frame.data(0));

                    let decoded_frame_u32 = convert_rgb_to_u32(
                        &decoded_frame_rgb,
                        decoder.width() as usize,
                        decoder.height() as usize,
                    );

                    // Display the decoded video in the second window
                    output_window
                        .update_with_buffer(
                            &decoded_frame_u32,
                            decoder.width() as usize,
                            decoder.height() as usize,
                        )
                        .unwrap();
                    frame_index += 1;
                }
                Ok(())
            };

        for (stream, packet) in ictx.packets() {
            if stream.index() == video_stream_index {
                decoder.send_packet(&packet)?;
                receive_and_process_decoded_frames(&mut decoder)?;
            }
        }
        decoder.send_eof()?;
        receive_and_process_decoded_frames(&mut decoder)?;
    }

    Ok(())
}

fn save_file(frame: &Video, index: usize) -> std::result::Result<(), std::io::Error> {
    let mut file = File::create(format!(
        "/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/frame{}.ppm",
        index
    ))?;
    file.write_all(format!("P6\n{} {}\n255\n", frame.width(), frame.height()).as_bytes())?;
    file.write_all(frame.data(0))?;
    Ok(())
}
