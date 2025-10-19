"""
Airflow DAG for Video Processing Pipeline
Automates video breakdown into audio, frames, and text
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from src.video_processing import load_video, extract_audio, extract_frames, save_results
from airflow import configuration as conf

# Enable pickle support for XCom (passing data between tasks)
conf.set('core', 'enable_xcom_pickling', 'True')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow_video_pipeline',
    'start_date': datetime(2024, 10, 19),
    'retries': 1,  # Retry once if task fails
    'retry_delay': timedelta(minutes=2),
}

# Create the DAG
dag = DAG(
    'Video_Processing_Pipeline',
    default_args=default_args,
    description='Automated video breakdown: audio + frames extraction',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['video', 'mlops', 'processing']
)

# Task 1: Load video file
load_video_task = PythonOperator(
    task_id='load_video_task',
    python_callable=load_video,
    provide_context=True,
    dag=dag,
)

# Task 2: Extract audio from video
extract_audio_task = PythonOperator(
    task_id='extract_audio_task',
    python_callable=extract_audio,
    provide_context=True,
    dag=dag,
)

# Task 3: Extract frames from video
extract_frames_task = PythonOperator(
    task_id='extract_frames_task',
    python_callable=extract_frames,
    provide_context=True,
    dag=dag,
)

# Task 4: Save and summarize results
save_results_task = PythonOperator(
    task_id='save_results_task',
    python_callable=save_results,
    provide_context=True,
    dag=dag,
)

# Define task dependencies (pipeline flow)
# Load video first, then extract audio and frames in parallel, then save results
load_video_task >> [extract_audio_task, extract_frames_task] >> save_results_task

# Command-line interaction
if __name__ == "__main__":
    dag.cli()
```

**Notice the pipeline structure:**
```
load_video_task
      ↓
  ┌───┴───┐
  ↓       ↓
extract_audio  extract_frames  (run in parallel!)
  └───┬───┘
      ↓
save_results_task