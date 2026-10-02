import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestingSession").getOrCreate()

def test_clean_data(spark):
    mock_data = [
        ("Alice", 100.0),
        ("Bob", 0.0),
        ("Charlie", -20.0),
        (None, 150.0),
        ("David", 50.0)
    ]
    
    df = spark.createDataFrame(mock_data, ["name", "amount"])
    
    transformed_df = clean_data(df)
    results = transformed_df.collect()
    
    assert len(results) == 2
    
    names_kept = [row["name"] for row in results]
    assert "Alice" in names_kept
    assert "David" in names_kept
    assert "Bob" not in names_kept
    assert "Charlie" not in names_kept
    assert None not in names_kept
    
    alice_row = [row for row in results if row["name"] == "Alice"][0]
    assert alice_row["amount_with_tax"] == 120.0
    
    david_row = [row for row in results if row["name"] == "David"][0]
    assert david_row["amount_with_tax"] == 60.0