
________________________________________
🧠 Proactive AI-Based Anomaly Detection for ETL Pipelines
📖 Overview
This project implements a GPU-accelerated, autoencoder-based anomaly-detection layer integrated within a Dagster-orchestrated ETL pipeline. It’s the practical build for my MSc thesis: “Proactive AI-Based Anomaly Detection for ETL Pipelines: A Predictive QA Framework for Data Engineering Workflows.”
The system proactively validates data during source→target movement and flags anomalies, missing records, duplicates, and mismatches before data reaches production—reducing manual QA effort and increasing trust in downstream analytics.
________________________________________
⚙️ System Architecture
1.	Data Flow Simulation
o	Synthetic datasets (Banking, Insurance, Sales) loaded to NeonDB/Supabase as source/target views.
o	Controlled anomaly injection (nulls, duplicates, outliers, missing records) in target tables to emulate real defects.
2.	Orchestration (Dagster)
o	Dagster assets/jobs run the ETL and automatically trigger the validation stage after each load.
3.	AI Validation Layer (Autoencoder)
o	PyTorch autoencoder learns “normal” patterns on clean data.
o	During validation, high reconstruction error ⇒ anomaly.
o	CUDA (GPU) acceleration for fast inference.
4.	Outputs
o	Logs of anomalies/duplicates/nulls/missing records written as CSVs to AWS S3.
o	Email notification with summary + GPU status sent to the QA inbox.
________________________________________
🧩 Implementation Highlights
•	Core tech: Dagster 1.x, PyTorch 2.x, PostgreSQL (Neon), AWS S3.
•	Key files:
o	jobs.py — ETL job and post-load validation trigger.
o	repository.py — registers Dagster jobs/assets.
o	banking_anomaly_detection_to_s3.py — GPU-based validator + S3 uploads.
o	requirements.txt — reproducible environment.
o	.gitignore — excludes venv/logs/data/artifacts.

Dataset	Num of Records	Features	Injected Anomalies	AE Detection Rate	GPU Time (secs)	CPU Time (secs)
Banking	600k	20	~1%	~99%	10-12 secs	22-25 secs
Insurance	1 million	10	~0.5%	~98%	6-9 secs	18-20 secs
Sales	200k	8	~0.001%	100%	5-7 secs	12-15 secs

________________________________________
🧰 Setup
1.	Clone
        git clone https://github.com/kranthi09/Thesis.git
                cd Thesis
2.	Environment
        python -m venv .venv
                .venv\Scripts\activate   # Windows
                 pip install -r requirements.txt
3.	Configuration (.env, not committed)
SOURCE_DB=postgresql+psycopg2://.../source
TARGET_DB=postgresql+psycopg2://.../target
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=etl-logs-neon
EMAIL_SENDER=...
EMAIL_RECEIVER=...
SMTP_PASSWORD=...
4.	Run Dagster
dagster dev -f repository.py
               Open http://localhost:3000 and launch the job.
________________________________________
📈 Roadmap
•	Add drift detection + auto-retraining.
•	Streaming validation with Dagster.
•	Containerized AE service; multi-GPU/distributed runs.
•	Broader baseline comparisons (IF/LOF/OC-SVM) & PR curves.
________________________________________
👤 Author
Kranthi Kumar
MSc Data Analytics, National College of Ireland
📧 kranthi.vys86@gmail.com

