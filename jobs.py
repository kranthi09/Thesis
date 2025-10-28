from dagster import op, In, Nothing,job
import pandas as pd
import random, io
from sqlalchemy import create_engine
import psycopg2
import subprocess

# ===== DB CONNECTIONS =====
SOURCE_DB = "postgresql://neondb_owner:npg_Dpqsjt16nZJr@ep-lingering-meadow-ade2pgq9-pooler.c-2.us-east-1.aws.neon.tech/source?sslmode=require&channel_binding=require"
TARGET_DB = "postgresql://neondb_owner:npg_Dpqsjt16nZJr@ep-lingering-meadow-ade2pgq9-pooler.c-2.us-east-1.aws.neon.tech/target?sslmode=require&channel_binding=require"
source_engine = create_engine(SOURCE_DB)

# ===== SQL to create target table =====
CREATE_TARGET_TABLE = """
DROP TABLE IF EXISTS fact_banking_target;
CREATE TABLE fact_banking_target (
    transaction_id     BIGINT,
    account_id         BIGINT,
    customer_id        BIGINT,
    txn_timestamp      TIMESTAMP,
    amount             NUMERIC(12,2),
    currency           VARCHAR(3),
    merchant_id        BIGINT,
    merchant_category  VARCHAR(40),
    txn_type           VARCHAR(20),
    channel            VARCHAR(15),
    country_code       VARCHAR(2),
    city               VARCHAR(40),
    device_id          VARCHAR(32),
    is_fraud           BOOLEAN,
    balance_after      NUMERIC(14,2),
    exchange_rate      NUMERIC(10,6),
    fee                NUMERIC(10,2),
    status             VARCHAR(12),
    approval_code      VARCHAR(10),
    card_type          VARCHAR(12),
    bin                VARCHAR(8),
    issuer_bank        VARCHAR(30),
    risk_score         NUMERIC(5,2)
);
"""

@op
def load_source_data():
    with source_engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM fact_banking", conn)
    return df

@op
def inject_anomalies(df: pd.DataFrame):
    n = len(df)

    dup_sample = df.sample(int(0.01 * n), random_state=1)
    df = pd.concat([df, dup_sample])

    for col in random.sample([c for c in df.columns if c != "transaction_id"], 3):
        idxs = random.sample(range(len(df)), int(0.01 * n))
        df.loc[df.index[idxs], col] = None

    df = df.drop(df.sample(int(0.01 * n), random_state=2).index)

    anomaly_rows = df.sample(int(0.005 * n), random_state=3).copy()
    anomaly_rows["amount"] *= random.choice([10, 50, 100])
    anomaly_rows["risk_score"] = 999
    df = pd.concat([df, anomaly_rows])

    broken_logic_idx = df.sample(int(0.01 * n), random_state=4).index
    df.loc[broken_logic_idx, "amount"] *= -1
    df.loc[broken_logic_idx, "risk_score"] = -1

    shuffled_columns = df.columns.tolist()
    random.shuffle(shuffled_columns)
    df = df[shuffled_columns]

    return df

@op
def load_to_target(df: pd.DataFrame):
    int_cols = ["transaction_id", "account_id", "customer_id", "merchant_id"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

    ordered_cols = [
        'transaction_id', 'account_id', 'customer_id', 'txn_timestamp', 'amount',
        'currency', 'merchant_id', 'merchant_category', 'txn_type', 'channel',
        'country_code', 'city', 'device_id', 'is_fraud', 'balance_after',
        'exchange_rate', 'fee', 'status', 'approval_code', 'card_type',
        'bin', 'issuer_bank', 'risk_score'
    ]

    missing = set(ordered_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    
    df = df[ordered_cols]

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=True)
    buffer.seek(0)

    conn = psycopg2.connect(TARGET_DB)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(CREATE_TARGET_TABLE)
    copy_sql = f"COPY fact_banking_target ({','.join(ordered_cols)}) FROM STDIN WITH CSV HEADER"
    cur.copy_expert(copy_sql, buffer)
    cur.close()
    conn.close()




@op(ins={"start": In(Nothing)})
def run_validation_script():
    """Run anomaly detection script after data is loaded to target"""
    import subprocess

    python_path = r"C:/Users/91829/Documents/Kranthis Project/banking_pipeline/v1/Scripts/python.exe"
    result = subprocess.run([python_path, "banking_anomaly_detection_to_s3.py"], capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"Validation failed: {result.stderr}")
    else:
        print("[INFO] Validation script executed successfully.")
        print(result.stdout)



@job
def banking_pipeline():
    data = load_source_data()
    data_with_anoms = inject_anomalies(data)
    loaded = load_to_target(data_with_anoms)

    run_validation_script(start=loaded)





