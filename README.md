🧠 Proactive AI-Based Anomaly Detection for ETL Pipelines
📖 Overview

This project implements a GPU-accelerated, autoencoder-based anomaly detection system that is fully integrated within a Dagster ETL pipeline.
It serves as the practical implementation for my MSc Thesis — “Proactive AI-Based Anomaly Detection for ETL Pipelines: A Predictive QA Framework for Data Engineering Workflows.”

The system proactively validates data during every ETL run by embedding deep learning–based anomaly detection directly into the pipeline.
When data loads occur, the autoencoder automatically scans the clean source data, learns its patterns, and then validates the target data in real time to detect deviations.

⚙️ System Workflow

Integrated Autoencoder Learning

During each ETL job, the autoencoder scans the clean source dataset to learn its internal data patterns and correlations.

These learned representations serve as the benchmark for comparison during target data validation.

Target Data Validation

When the data is loaded into the target system, the same autoencoder evaluates the new data against the learned clean data patterns.

Any record showing unusual reconstruction error or deviation is immediately flagged as an anomaly.

Automated Logging and Reporting

All detected anomalies are logged into an AWS S3 bucket.

A detailed summary report containing anomaly statistics and GPU status is automatically sent via email to the QA or data engineering team.

End-to-End GPU Acceleration (CUDA)

The entire validation process is powered by CUDA-enabled GPUs, ensuring superfast matrix operations and parallel computation during autoencoder inference.

This enables large-scale anomaly validation in a fraction of the time compared to CPU-based processing.

⚡ CUDA Acceleration Highlights

Massive Parallelism: CUDA executes thousands of threads simultaneously, allowing rapid validation across millions of records.

Optimized Computation: PyTorch operations such as matrix multiplications and tensor transformations are offloaded to GPU cores for lightning-fast processing.

Low Latency: Real-time GPU inference ensures that data validation occurs almost instantly after each load.

Scalability: The CUDA-based design is easily extendable to multi-GPU environments for enterprise-scale data pipelines.

🧩 Implementation Highlights

Frameworks: Dagster 1.x, PyTorch 2.x (CUDA-enabled), PostgreSQL (Neon), AWS S3

Integration: Autoencoder model runs inside the Dagster job — no separate step required.

Key Components:

jobs.py – Defines the ETL workflow and triggers validation.

repository.py – Registers Dagster jobs and assets.

banking_anomaly_detection_to_s3.py – Performs GPU-based validation and uploads logs to S3.

requirements.txt – Contains dependency versions for reproducibility.

.gitignore – Excludes heavy or environment-specific files like .venv and logs.

🌟 Significance

End-to-End Automation: The AI validation layer runs within the ETL pipeline automatically — no manual intervention needed.

Proactive QA: Detects data-quality issues as soon as they occur, preventing corrupt or inconsistent data from reaching production systems.

High Performance: CUDA acceleration enables ultra-fast validation, even for large-scale datasets.

Business Impact: Minimizes defect leakage, improves data trust, and reduces manual testing time.

🧰 Setup Instructions

Clone the Repository

git clone https://github.com/kranthi09/Thesis.git
cd Thesis


Create and Activate Virtual Environment

python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt


Configure Environment Variables (.env)

SOURCE_DB=postgresql+psycopg2://.../source
TARGET_DB=postgresql+psycopg2://.../target
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=etl-logs-neon
EMAIL_SENDER=...
EMAIL_RECEIVER=...
SMTP_PASSWORD=...


Launch Dagster

dagster dev -f repository.py


Then open http://localhost:3000
 to run and monitor your ETL job.

🚀 Future Enhancements

Integrate drift detection and dynamic re-training of autoencoders.

Enable real-time data stream validation using Kafka or Spark.

Extend to multi-GPU support using NVIDIA RAPIDS or Dask.

Build a visual dashboard for anomaly insights and validation history.

👤 Author

Kranthi Kumar
MSc Data Analytics, National College of Ireland
📧 kranthi.vys86@gmail.com
