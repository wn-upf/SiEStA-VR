#encode at variable bitrates:
# ------------------- first 500k, then 1mb, then 2 mb
#  5 - 3 - 4 pattern, ignoring names of segments


cd bitrates_comparison


# Function to clean up and exit
cleanup() {
    echo "Exiting and cleaning up..."
    rm -f first_10sec.mkv second_10sec.mkv third_10sec.mkv fourth_10sec.mkv final_variable_bitrate.mkv
    exit 1
}

# Trap SIGINT (Ctrl+C) and call the cleanup function
trap cleanup SIGINT

## make separate copies for better I/O parallelization
cp ../bbb_1080p60fps.mp4 bbb1.mp4  
cp ../bbb_1080p60fps.mp4 bbb2.mp4
cp ../bbb_1080p60fps.mp4 bbb3.mp4
cp ../bbb_1080p60fps.mp4 bbb4.mp4

ffmpeg -hwaccel cuda -i bbb1.mp4 -t  10       -c:v hevc_nvenc -b:v 10M  first_10sec.mkv &
ffmpeg -hwaccel cuda -i bbb2.mp4 -ss 10 -t 10 -c:v hevc_nvenc -b:v 500K second_10sec.mkv &
ffmpeg -hwaccel cuda -i bbb3.mp4 -ss 20 -t 10 -c:v hevc_nvenc -b:v 10M  third_10sec.mkv &
ffmpeg -hwaccel cuda -i bbb4.mp4 -ss 30 -t 10 -c:v hevc_nvenc -b:v 5M   fourth_10sec.mkv &
wait
rm bbb1.mp4 bbb2.mp4 bbb3.mp4 bbb4.mp4 final_variable_bitrate.mkv fixed_output.mkv
ffmpeg -hwaccel cuda -i first_10sec.mkv -i second_10sec.mkv -i third_10sec.mkv -i fourth_10sec.mkv -filter_complex "[0:v][1:v][2:v][3:v]concat=n=4:v=1:a=0" -c:v hevc_nvenc -async 1 final_variable_bitrate.mkv
rm first_10sec.mkv second_10sec.mkv third_10sec.mkv fourth_10sec.mkv
ffmpeg -hwaccel cuda -i final_variable_bitrate.mkv -acodec copy -vcodec copy fixed_output.mkv


vlc fixed_output.mkv


# ffmpeg -hwaccel cuda -i 10mbps_sample_40s.mkv -i fixed_output.mkv \
#     -filter_complex "[0:v][1:v]libvmaf=log_fmt=json:log_path=output.json" -f null -

# PSNR + VMAF + SSIM : 
# ffmpeg -hwaccel cuda -i 10mbps_sample_40s.mkv -i fixed_output.mkv \
#     -filter_complex "[0:v][1:v]libvmaf=log_fmt=json:log_path=output.json;[0:v][1:v]ssim=stats_file=ssim.log;[0:v][1:v]psnr=stats_file=psnr.log" -f null -

ffmpeg -hwaccel cuda -i output_nvenc.mkv -i fixed_output.mkv \
    -filter_complex "[0:v][1:v]libvmaf=log_fmt=json:log_path=vmaf.json" \
    -filter_complex "[0:v][1:v]psnr=stats_file=psnr.log" \
    -filter_complex "[0:v][1:v]ssim=stats_file=ssim.log" \
    -f null -
# cd ..



