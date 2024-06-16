from pyspark.sql import SparkSession
import os
from scripts.extract import Extract
from scripts.transform import Transform
from scripts.load import Load


def main():
    spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
    data_dir = "data/"
    output_dir = "output/"
    output_file = "etl_output.csv"
    output_path = os.path.join(output_dir, output_file)

    # Initialize Extractor to load data from 'data/' directory
    extractor = Extract(data_dir)

    # Load taxi data, payment lookup data, and vendor lookup data
    taxi_data = extractor.load_taxi_data()
    payment_data = extractor.load_payment_lookup()
    vendor_data = extractor.load_vendor_lookup()

    # Initialize Transformer with loaded data
    transformer = Transform(taxi_data, payment_data, vendor_data)

    # Process the loaded data to transform it
    transformed_data = transformer.process_data(output_path)

    # Initialize Loader to save transformed data to 'output/etl_output.csv'
    loader = Load(output_dir)

    # Save transformed data to CSV file
    loader.save_data(transformed_data)

    # Stop SparkSession
    spark.stop()


if __name__ == '__main__':
    main()
