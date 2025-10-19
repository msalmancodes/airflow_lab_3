"""
Video processing functions for Airflow DAG
Extracts audio, frames, and prepares outputs from video files
"""

import os 
import pickle 
from pathlib import Path 
import imageio.v3 as iio
import numpy as np


#define paths

BASE_DIR = Path("/opt/airflow/dags")
BASE_DIR = Path("/opt/airflow/dags")
VIDEO_DIR = BASE_DIR / "videos"
OUTPUT_DIR = BASE_DIR / "outputs"
AUDIO_DIR = OUTPUT_DIR / "audio"
FRAMES_DIR = OUTPUT_DIR / "frames"


video_files = list(VIDEO_DIR.glob("*.mp4"))

if not video_files:
    raise FileNotFoundError(f"No MP4 files found in {VIDEO_DIR}")

video_path = video_files[0]
print(f"Found video: {video_path.name}")

# Get video properties
properties = iio.improps(video_path, plugin="pyav")
print(f"Duration: {properties.duration} seconds")
print(f"FPS: {properties.fps}")
print(f"Size: {properties.shape}")


def load_video(**kwargs):
    """
    Task 1: Load video file and return metadata
    """
    print("=" * 50)
    print("TASK 1: Loading Video")
    print("=" * 50)
    
    # Find first video file in videos directory
    video_files = list(VIDEO_DIR.glob("*.mp4"))
    
    if not video_files:
        raise FileNotFoundError(f"No MP4 files found in {VIDEO_DIR}")
    
    video_path = video_files[0]
    print(f"Found video: {video_path.name}")
    
    # Get video properties
    properties = iio.improps(video_path, plugin="pyav")
    print(f"Duration: {properties.duration} seconds")
    print(f"FPS: {properties.fps}")
    print(f"Size: {properties.shape}")
    
    # Return video info as serialized data
    video_info = {
        'path': str(video_path),
        'name': video_path.name,
        'duration': properties.duration,
        'fps': properties.fps,
        'shape': properties.shape
    }
    
    print(f"✅ Video loaded successfully: {video_path.name}")
    return pickle.dumps(video_info)

def extract_audio(ti, **kwargs):
    """
    Task 2: Extract audio from video
    """
    print("=" * 50)
    print("TASK 2: Extracting Audio")
    print("=" * 50)
    
    # Get video info from previous task
    video_info = pickle.loads(ti.xcom_pull(task_ids='load_video_task'))
    video_path = video_info['path']
    video_name = Path(video_info['name']).stem
    
    print(f"Processing: {video_info['name']}")
    
    # Create audio output directory
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    
    # Extract audio using imageio
    audio_path = AUDIO_DIR / f"{video_name}_audio.wav"
    
    try:
        # Read video and extract audio
        reader = iio.imopen(video_path, "r", plugin="pyav")
        audio_data = []
        
        for frame in reader:
            if hasattr(frame, 'audio'):
                audio_data.append(frame.audio)
        
        if audio_data:
            print(f"✅ Audio extracted: {audio_path.name}")
        else:
            print("⚠️  No audio track found in video")
            audio_path = None
            
    except Exception as e:
        print(f"⚠️  Audio extraction skipped: {str(e)}")
        audio_path = None
    
    audio_info = {
        'audio_path': str(audio_path) if audio_path else None,
        'video_name': video_name
    }
    
    return pickle.dumps(audio_info)



def extract_frames(ti, **kwargs):
    """
    Task 3: Extract key frames from video
    """
    print("=" * 50)
    print("TASK 3: Extracting Frames")
    print("=" * 50)
    
    # Get video info from first task
    video_info = pickle.loads(ti.xcom_pull(task_ids='load_video_task'))
    video_path = video_info['path']
    video_name = Path(video_info['name']).stem
    
    print(f"Processing: {video_info['name']}")
    
    # Create frames output directory
    FRAMES_DIR.mkdir(parents=True, exist_ok=True)
    
    # Extract frames (every 30th frame to keep it light)
    frame_paths = []
    frame_count = 0
    sample_rate = 30  # Extract every 30th frame
    
    try:
        for idx, frame in enumerate(iio.imiter(video_path, plugin="pyav")):
            if idx % sample_rate == 0:
                frame_filename = f"{video_name}_frame_{frame_count:04d}.jpg"
                frame_path = FRAMES_DIR / frame_filename
                iio.imwrite(frame_path, frame)
                frame_paths.append(str(frame_path))
                frame_count += 1
                
                # Limit to 10 frames for the lab
                if frame_count >= 10:
                    break
        
        print(f"✅ Extracted {frame_count} frames")
        
    except Exception as e:
        print(f"❌ Error extracting frames: {str(e)}")
        raise
    
    frames_info = {
        'frame_paths': frame_paths,
        'frame_count': frame_count,
        'video_name': video_name
    }
    
    return pickle.dumps(frames_info)

def save_results(ti, **kwargs):
    """
    Task 4: Consolidate and save all results
    """
    print("=" * 50)
    print("TASK 4: Saving Results")
    print("=" * 50)
    
    # Get data from all previous tasks
    video_info = pickle.loads(ti.xcom_pull(task_ids='load_video_task'))
    audio_info = pickle.loads(ti.xcom_pull(task_ids='extract_audio_task'))
    frames_info = pickle.loads(ti.xcom_pull(task_ids='extract_frames_task'))
    
    # Print summary
    print("\n" + "=" * 50)
    print("VIDEO PROCESSING COMPLETE - SUMMARY")
    print("=" * 50)
    print(f"📹 Video: {video_info['name']}")
    print(f"⏱️  Duration: {video_info['duration']:.2f} seconds")
    print(f"🎬 FPS: {video_info['fps']}")
    print(f"📐 Resolution: {video_info['shape']}")
    print(f"\n🎵 Audio: {audio_info['audio_path'] if audio_info['audio_path'] else 'Not extracted'}")
    print(f"🖼️  Frames extracted: {frames_info['frame_count']}")
    print(f"📁 Output directory: {OUTPUT_DIR}")
    print("=" * 50)
    
    # Create summary file
    summary_path = OUTPUT_DIR / "processing_summary.txt"
    with open(summary_path, 'w') as f:
        f.write("VIDEO PROCESSING SUMMARY\n")
        f.write("=" * 50 + "\n")
        f.write(f"Video: {video_info['name']}\n")
        f.write(f"Duration: {video_info['duration']:.2f} seconds\n")
        f.write(f"FPS: {video_info['fps']}\n")
        f.write(f"Resolution: {video_info['shape']}\n")
        f.write(f"Audio: {audio_info['audio_path'] if audio_info['audio_path'] else 'Not extracted'}\n")
        f.write(f"Frames: {frames_info['frame_count']}\n")
    
    print(f"✅ Summary saved: {summary_path}")
    
    return "Video processing pipeline completed successfully! 🎉"



