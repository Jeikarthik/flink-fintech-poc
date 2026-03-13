import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime

def main():
    print("Starting Spark Batch Feature Computation...")
    
    spark = SparkSession.builder \
        .appName("BatchFeatureComputation") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .getOrCreate()
        
    db_url = "jdbc:postgresql://postgres:5432/outbox_demo"
    db_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    
    # 1. Load Data
    try:
        customers_df = spark.read.jdbc(url=db_url, table="customers", properties=db_properties)
        print(f"Loaded {customers_df.count()} customers.")
    except Exception as e:
        print(f"Failed to load data from DB: {e}")
        sys.exit(1)
        
    # 2. Compute Batch Features (Simplified Mock Implementation for POC)
    # In a real scenario, this would aggregate historical transactions over months.
    # Here we simulate the logic to demonstrate the pipeline.
    
    features_df = customers_df.select("customer_id").distinct()
    
    # Mock computing salary_delay_days based on expected_salary_day vs current day
    current_day = datetime.now().day
    
    mock_features = customers_df.withColumn(
        "salary_delay_days", 
        (lit(current_day) - col("expected_salary_day")).cast("int")
    ).withColumn(
        "salary_delay_days", # Clamp negative values to 0
        col("salary_delay_days") * (col("salary_delay_days") > 0).cast("int")
    ).withColumn(
        "utility_payment_avg_delay_days", lit(2.5) # Mock value
    ).withColumn(
        "discretionary_spend_trend", lit(1.15) # Mock value
    ).withColumn(
        "feature_timestamp", current_timestamp()
    ).select(
        "customer_id", 
        "feature_timestamp", 
        "salary_delay_days", 
        "utility_payment_avg_delay_days", 
        "discretionary_spend_trend"
    )
    
    print("Computed batch features for customers.")
    mock_features.show(5)
    
    # 3. Write features to Offline Store (PostgreSQL)
    # Note: We use append mode here. In production, we'd upsert or Feast materializes the latest.
    try:
        mock_features.write \
            .jdbc(url=db_url, table="customer_features_offline", mode="append", properties=db_properties)
        print("Successfully wrote batch features to offline store.")
    except Exception as e:
        print(f"Failed to write features: {e}")
        
    spark.stop()

if __name__ == "__main__":
    main()
