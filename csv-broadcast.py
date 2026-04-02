from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Broadcast CSV Example") \
        .getOrCreate()

    sc = spark.sparkContext

    # CSV file taken from Datablist (people-100.csv)
    csv_path = "people-100.csv"   # Root folder location

    # Step 1: Load CSV into DataFrame
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Step 2: Collect rows into dictionary (broadcastable form)
    people_dict = {i: row.asDict() for i, row in enumerate(df.collect())}

    # Step 3: Broadcast dictionary
    broadcast_var = sc.broadcast(people_dict)

    # Step 4: Convert broadcasted data back into DataFrame
    broadcasted_df = spark.createDataFrame(list(broadcast_var.value.values()))

    # Step 5: Print results
    print("=== Original DataFrame ===")
    df.show(truncate=False)

    print("=== Broadcasted DataFrame ===")
    broadcasted_df.show(truncate=False)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
