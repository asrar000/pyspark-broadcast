import requests
from pyspark.sql import SparkSession

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
    # The API returns a dict with key "products" containing a list
    products_dict = {p["id"]: p for p in data["products"]}

    # Step 3: Broadcast dictionary
    broadcast_var = sc.broadcast(products_dict)

    # Step 4: Create multiple DataFrames using broadcasted data
    # Example: split into two DataFrames based on category
    products = list(broadcast_var.value.values())

    # Convert to DataFrame
    df_all = spark.createDataFrame(products)

    # Example split: electronics vs non-electronics
    df_electronics = spark.createDataFrame(
        [p for p in products if p["category"] == "smartphones"]
    )
    df_non_electronics = spark.createDataFrame(
        [p for p in products if p["category"] != "smartphones"]
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
