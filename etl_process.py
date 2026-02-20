import pandas as pd
import numpy as np

def generate_raw_data(n=100):
    """Generates a synthetic dataset."""
    return pd.DataFrame({
        'order_id': range(1, n + 1),
        'customer': [f"Cust_{i}" for i in range(n)],
        'amount': np.random.uniform(10.0, 500.0, n),
        'region': np.random.choice(['North', 'South', 'East', 'West'], n)
    })

def transform_data(df):
    # 1. Filter: Only keep orders > $50
    df = df[df['amount'] > 50].copy()
    # 2. Transform: Uppercase region names
    df['region'] = df['region'].str.upper()
    return df

def run_pipeline():
    """Main execution flow."""
    raw = generate_raw_data()
    clean = transform_data(raw)
    clean.to_csv('processed_data.csv', index=False)
    return 'processed_data.csv'

if __name__ == "__main__":
    run_pipeline()