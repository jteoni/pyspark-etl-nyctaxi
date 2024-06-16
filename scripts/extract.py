from pyspark.sql import SparkSession
import os


class Extract:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.spark = SparkSession.builder.getOrCreate()

    def load_taxi_data(self):
        try:
            json_files = [f for f in os.listdir(self.data_dir) if f.endswith('.json')]
            if not json_files:
                raise FileNotFoundError(f"No JSON files found in directory: {self.data_dir}")

            data_frames = []
            for file in json_files:
                df = self.spark.read.json(os.path.join(self.data_dir, file))
                data_frames.append(df)

            # Union all DataFrames in the list
            combined_df = data_frames[0]
            for df in data_frames[1:]:
                combined_df = combined_df.union(df)

            return combined_df

        except FileNotFoundError as e:
            print(f"Error loading JSON files: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error loading taxi data: {e}")
            raise

    def load_payment_lookup(self):
        try:
            payment_lookup_path = os.path.join(self.data_dir, 'data-payment_lookup.csv')
            if not os.path.isfile(payment_lookup_path):
                raise FileNotFoundError(f"File not found: {payment_lookup_path}")

            # Load payment lookup data with custom header
            payment_data = self.spark.read.option("header", True).csv(payment_lookup_path)

            # Rename columns to match expected schema
            payment_data = payment_data.selectExpr("A as payment_type", "B as payment_lookup")

            # Ensure 'payment_type' column exists
            if 'payment_type' not in payment_data.columns:
                raise ValueError("Missing required column 'payment_type' in payment lookup data")

            return payment_data

        except FileNotFoundError as e:
            print(f"Error loading payment lookup file: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error loading payment lookup data: {e}")
            raise

    def load_vendor_lookup(self):
        try:
            vendor_lookup_path = os.path.join(self.data_dir, 'data-vendor_lookup.csv')
            if not os.path.isfile(vendor_lookup_path):
                raise FileNotFoundError(f"File not found: {vendor_lookup_path}")

            return self.spark.read.csv(vendor_lookup_path, header=True, inferSchema=True)
        except FileNotFoundError as e:
            print(f"Error loading vendor lookup file: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error loading vendor lookup data: {e}")
            raise
