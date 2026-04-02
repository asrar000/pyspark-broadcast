# PySpark Broadcast Examples

This repository contains two small PySpark examples that demonstrate how to broadcast driver-side Python data and turn it back into Spark DataFrames.

## Project Overview

The examples in this project show two simple broadcast workflows:

- `csv-broadcast.py` reads a local CSV file into Spark, collects the rows on the driver, broadcasts the resulting dictionary, and recreates a DataFrame from the broadcasted values.
- `json-broadcast.py` fetches product data from a public JSON API, broadcasts it as a Python dictionary, and creates multiple DataFrames from the broadcasted dataset.

These scripts are best treated as learning or demonstration code for small datasets. Because both examples collect data on the driver before broadcasting it, they are not suitable for large-scale production workloads.

## Project Structure

```text
.
├── csv-broadcast.py
├── json-broadcast.py
├── people-100.csv
├── requirements.txt
└── README.md
```

## Requirements

- Python 3.10 or newer
- Java installed and available on your `PATH`
- Apache Spark or PySpark available in your environment

## Installation

Create and activate a virtual environment if you want an isolated local setup, then install the project dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Run the CSV example:

```bash
python csv-broadcast.py
```

Run the JSON example:

```bash
python json-broadcast.py
```

If your environment is set up around Spark tooling, you can also run the scripts with `spark-submit`.

## Example Details

### CSV Broadcast Example

The CSV example uses the included `people-100.csv` sample dataset. It:

1. Creates a `SparkSession`
2. Reads the CSV into a DataFrame
3. Collects the rows into a Python dictionary
4. Broadcasts that dictionary with the Spark context
5. Rebuilds a Spark DataFrame from the broadcasted values

### JSON Broadcast Example

The JSON example fetches data from `https://dummyjson.com/products`. It:

1. Creates a `SparkSession`
2. Downloads product data with `requests`
3. Converts the response into a dictionary keyed by product ID
4. Broadcasts the dictionary
5. Builds one full DataFrame and two filtered DataFrames based on product category

## Notes

- Internet access is required to run `json-broadcast.py`.
- The included CSV file is kept in the repository as sample input data.
- Broadcast variables are most useful when you need to share a relatively small read-only dataset across Spark tasks.

## License

No license file is currently included in this repository.
