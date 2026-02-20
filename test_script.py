import pytest
import os
import pandas as pd
from etl_process import generate_raw_data, transform_data, run_pipeline

def test_transformation_logic():
    """Unit test: Verify ETL rules are applied correctly."""
    sample_data = pd.DataFrame({
        'order_id': [1, 2],
        'customer': ['A', 'B'],
        'amount': [10.0, 100.0],
        'region': ['north', 'south']
    })
    
    transformed = transform_data(sample_data)
    
    # Assert filter works (10.0 should be removed)
    assert len(transformed) == 1
    # Assert transformation works (north -> NORTH)
    assert transformed.iloc[0]['region'] == 'SOUTH'

def test_output_integrity():
    """Integration test: Verify the end-to-end file generation."""
    output_file = run_pipeline()
    
    assert os.path.exists(output_file)
    df = pd.read_csv(output_file)
    
    # Assert no nulls and minimum column presence
    assert not df.empty
    assert 'order_id' in df.columns
    assert (df['amount'] > 50).all()

def test_schema_validity():
    """Verify column names and types."""
    df = pd.read_csv('processed_data.csv')
    expected_columns = ['order_id', 'customer', 'amount', 'region']
    assert list(df.columns) == expected_columns
    assert df['amount'].dtype == 'float64'