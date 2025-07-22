






## Quick guide: 

Simulator for setting up a number of N_XR VR sessions, with configurable distance to an AP.

To run, execute the bash script ```xrun.sh``` in order to iterate over all the combinations of selected scenarios. Via modifying the values assigned to ```SERIAL_EXECUTION``` and ```NUMBER_OF_JOBS``` the degree of parallelism for execution can be controlled.

Other input args: 

```TEST_TYPE```:  Can be "BW", "JI", "PL", "RANDOM", or "STD" for different emulated tests (or none in case of STD). 'RANDOM' distributes randomly scheduled emulated effects over the simulation of the 3 previous types.  

```video_samples```: Denotes name of used video (without '.mp4' extension), located in ```video_samples_vmaf``` folder. The simulation assumes 60 FPS videos in 4K resolution. A sample can be obtained, for example in: http://bbb3d.renderfarming.net/download.html


Dependencies: FFMPEG v7.1 (https://ffmpeg.org/download.html)
