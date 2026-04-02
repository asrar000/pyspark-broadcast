import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Broadcast JSON to Multiple DataFrames") \
        .getOrCreate()

    sc = spark.sparkContext

    # Step 1: Fetch JSON from API
    url = "https://dummyjson.com/products"
    response = requests.get(url)
    data = response.json()

    # Step 2: Convert JSON into dictionary
    products_dict = {p["id"]: p for p in data["products"]}

    # Step 3: Broadcast dictionary
    broadcast_var = sc.broadcast(products_dict)

    # Step 4: Normalize numeric fields
    products = []
    for p in broadcast_var.value.values():
    # Copy the product dictionary and force price into float
        product = dict(p)
        product["price"] = float(p["price"])
        products.append(product)


    # Explicit schema to avoid type conflicts
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("category", StringType(), True)
    ])

    # Create DataFrame with schema
    df_all = spark.createDataFrame(products, schema=schema)

    # Example split: electronics vs non-electronics
    df_electronics = spark.createDataFrame(
        [p for p in products if p["category"] == "smartphones"], schema=schema
    )
    df_non_electronics = spark.createDataFrame(
        [p for p in products if p["category"] != "smartphones"], schema=schema
    )

    # Step 5: Print results
    print("=== All Products ===")
    df_all.show(truncate=False)

    print("=== Electronics ===")
    df_electronics.show(truncate=False)

    print("=== Non-Electronics ===")
    df_non_electronics.show(truncate=False)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
