# Airflow Video Processing Pipeline

MLOps Lab: Automated video breakdown pipeline using Apache Airflow

## Project Overview
This project demonstrates workflow orchestration using Airflow to automate video processing:
- Extract audio from video
- Extract key frames
- Extract text (OCR)

## Structure
- `dags/` - Airflow DAGs and processing scripts
- `dags/videos/` - Input video files
- `dags/outputs/` - Extracted components

## Setup
Follow instructions in the lab guide for Docker setup.

## Future Work
Integrate ML models for speech delivery quality analysis.