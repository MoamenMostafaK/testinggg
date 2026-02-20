import pandas as pd
import numpy as np

def run_etl():
    # 1. Generate Random Data
    df = pd.DataFrame({
        'id': range(1, 101),
        'user_name': [f"user_{np.random.randint(1, 1000)}" for _ in range(100)],
        'status': np.random.choice(['active', 'inactive', 'pending'], 100),
        'value': np.random.uniform(10.0, 500.0, 100)
    })

    # 2. Minimal ETL: Uppercase status and filter
    df['status'] = df['status'].str.upper()
    df = df[df['value'] > 15.0]  # Simple filter

    # 3. Save to CSV
    df.to_csv('raw_data.csv', index=False)
    print("ETL Complete: raw_data.csv generated.")

if __name__ == "__main__":
    run_etl()