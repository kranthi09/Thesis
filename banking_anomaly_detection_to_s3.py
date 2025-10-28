import os, json, time, torch
import numpy as np
import pandas as pd
import torch.nn as nn
from email.message import EmailMessage
from sqlalchemy import create_engine
from sklearn.preprocessing import MinMaxScaler
import smtplib, boto3

# ===== Config =====
SOURCE_DB = "postgresql+psycopg2://neondb_owner:npg_Dpqsjt16nZJr@ep-lingering-meadow-ade2pgq9-pooler.c-2.us-east-1.aws.neon.tech/source"
TARGET_DB = "postgresql+psycopg2://neondb_owner:npg_Dpqsjt16nZJr@ep-lingering-meadow-ade2pgq9-pooler.c-2.us-east-1.aws.neon.tech/target"
SOURCE_TABLE = "fact_banking"
TARGET_TABLE = "fact_banking_target"
PRIMARY_KEY = "transaction_id"
EPOCHS = 100
OUTPUT_DIR = "validation_logs"

AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''
BUCKET_NAME = 'etl-logs-neon'

EMAIL_SENDER = 'kranthi.vys86@gmail.com'
EMAIL_RECEIVER = 'kranthi.vys86@gmail.com'
SMTP_PASSWORD = 'hyfa sxwp wvft iyzz'

os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Email ===
def send_email(subject, body, files):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg.set_content(body)
    for path in files:
        with open(path, 'rb') as f:
            msg.add_attachment(f.read(), maintype='application', subtype='octet-stream', filename=os.path.basename(path))
    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.starttls()
        smtp.login(EMAIL_SENDER, SMTP_PASSWORD)
        smtp.send_message(msg)

# === S3 Upload ===
def upload_to_s3(filepath):
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    s3.upload_file(filepath, BUCKET_NAME, os.path.basename(filepath))

# === Autoencoder ===
class Autoencoder(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.encoder = nn.Sequential(nn.Linear(input_dim, 64), nn.ReLU(), nn.Linear(64, 32))
        self.decoder = nn.Sequential(nn.Linear(32, 64), nn.ReLU(), nn.Linear(64, input_dim))
    def forward(self, x):
        return self.decoder(self.encoder(x))

# === Main Function ===
def detect_anomalies():
    engine_src = create_engine(SOURCE_DB)
    engine_tgt = create_engine(TARGET_DB)
    df_src = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE}", engine_src)
    df_tgt = pd.read_sql(f"SELECT * FROM {TARGET_TABLE}", engine_tgt)

    start = time.time()

    # Nulls, Duplicates, Missing
    df_src[df_src.isnull().any(axis=1)].to_csv(f"{OUTPUT_DIR}/nulls_source.csv", index=False)
    df_tgt[df_tgt.isnull().any(axis=1)].to_csv(f"{OUTPUT_DIR}/nulls_target.csv", index=False)
    df_src[df_src.duplicated(subset=[PRIMARY_KEY])].to_csv(f"{OUTPUT_DIR}/duplicates_source.csv", index=False)
    df_tgt[df_tgt.duplicated(subset=[PRIMARY_KEY])].to_csv(f"{OUTPUT_DIR}/duplicates_target.csv", index=False)
    df_src[~df_src[PRIMARY_KEY].isin(df_tgt[PRIMARY_KEY])].to_csv(f"{OUTPUT_DIR}/missing_records.csv", index=False)

    # Autoencoder input
    X_train = df_src.drop(columns=[PRIMARY_KEY]).select_dtypes(include=[np.number]).dropna()
    X_test = df_tgt.drop(columns=[PRIMARY_KEY]).select_dtypes(include=[np.number]).dropna()

    scaler = MinMaxScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = Autoencoder(X_train_scaled.shape[1]).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    criterion = nn.MSELoss()

    X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32).to(device)

    for epoch in range(EPOCHS):
        model.train()
        optimizer.zero_grad()
        output = model(X_train_tensor)
        loss = criterion(output, X_train_tensor)
        loss.backward()
        optimizer.step()
        if epoch % 20 == 0 or epoch == EPOCHS - 1:
            print(f"Epoch {epoch} Loss: {loss.item():.6f}")

    # Predict
    model.eval()
    X_test_tensor = torch.tensor(X_test_scaled, dtype=torch.float32).to(device)
    with torch.no_grad():
        recon = model(X_test_tensor).cpu().numpy()

    recon_error = np.mean((X_test_scaled - recon) ** 2, axis=1)
    threshold = np.percentile(recon_error, 99)
    anomalies = recon_error > threshold
    df_anomalies = df_tgt.iloc[X_test.index][anomalies].copy()
    df_anomalies["reconstruction_error"] = recon_error[anomalies]
    df_anomalies.to_csv(f"{OUTPUT_DIR}/autoencoder_anomalies.csv", index=False)

    # Metrics
    end = time.time()
    metrics = {
        "device": str(device),
        "gpu_available": torch.cuda.is_available(),
        "gpu_name": torch.cuda.get_device_name(0) if torch.cuda.is_available() else "None",
        "epochs": EPOCHS,
        "total_records": len(X_test),
        "detected_anomalies": int(anomalies.sum()),
        "threshold": float(threshold),
        "reconstruction_error": {
            "mean": float(np.mean(recon_error)),
            "std": float(np.std(recon_error)),
            "min": float(np.min(recon_error)),
            "max": float(np.max(recon_error))
        },
        "training_loss_last_epoch": float(loss.item()),
        "validation_time_sec": round(end - start, 2)
    }

    with open(f"{OUTPUT_DIR}/autoencoder_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    with open(f"{OUTPUT_DIR}/metrics.txt", "w") as f:
        f.write(json.dumps(metrics, indent=2))

    # Upload & Email
    files = [f"{OUTPUT_DIR}/" + fname for fname in os.listdir(OUTPUT_DIR)]
    for f in files: upload_to_s3(f)
    send_email("✅ Banking Validation Logs", "Attached: Anomaly + Validation Logs", files)

    print("\n FINAL VALIDATION COMPLETE\n")
    print(json.dumps(metrics, indent=2))

# Entry point
if __name__ == "__main__":
    detect_anomalies()

