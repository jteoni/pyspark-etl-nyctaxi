from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Transform:
    def __init__(self, taxi_data: DataFrame, payment_data: DataFrame, vendor_data: DataFrame):
        self.taxi_data = taxi_data
        self.payment_data = payment_data
        self.vendor_data = vendor_data

    def calculate_most_trips_per_year(self):
        logger.info("Calculating most trips per year")
        return self.taxi_data.groupby('vendor_id', 'year').count().withColumnRenamed('count', 'trip_count')

    def calculate_most_trips_per_week(self):
        logger.info("Calculating most trips per week")
        return self.taxi_data.groupby('year', 'week').count().withColumnRenamed('count', 'trip_count')

    def calculate_vendor_with_most_trips_per_year(self):
        logger.info("Calculating vendor with most trips per year")
        return self.taxi_data.groupby('vendor_id', 'pickup_datetime').agg({'total_amount': 'sum'}) \
            .orderBy(['vendor_id', col('sum(total_amount)').desc()]) \
            .dropDuplicates(['vendor_id'])

    def process_data(self, output_path: str) -> DataFrame:
        logger.info("Starting data processing")
        # Ensure the columns used in transformations exist
        required_columns = ["vendor_id", "payment_type", "fare_amount", "total_amount"]
        for column in required_columns:
            if column not in self.taxi_data.columns:
                raise ValueError(f"Missing required column '{column}' in taxi data")

        # Example transformation logic: Adding a new column based on existing data
        transformed_data = self.taxi_data.withColumn("new_fare_amount", self.taxi_data["fare_amount"] * 1.1)

        # Ensure 'payment_type' column exists in payment_data
        if "payment_type" not in self.payment_data.columns:
            raise ValueError(f"Missing required column 'payment_type' in payment data")

        # Ensure 'vendor_id' column exists in vendor_data
        if "vendor_id" not in self.vendor_data.columns:
            raise ValueError(f"Missing required column 'vendor_id' in vendor data")

        # Join the taxi data with payment lookup data
        transformed_data = transformed_data.join(self.payment_data, on="payment_type", how="left")

        # Join the resulting data with vendor lookup data
        transformed_data = transformed_data.join(self.vendor_data, on="vendor_id", how="left")

        # Coalesce to a single partition before writing to ensure one output file
        transformed_data = transformed_data.coalesce(1)

        # Write the transformed data to the specified output path in CSV format
        transformed_data.write.csv(output_path, header=True, mode="overwrite")

        logger.info("Data processing completed")
        return transformed_data

    def process_data_example(self) -> DataFrame:
        logger.info("Starting example data processing")
        # Example transformation: Filter by 'fare_amount' greater than 15 and add new columns
        transformed_data = self.taxi_data.filter(col("fare_amount") > 15)
        transformed_data = transformed_data.withColumn("new_fare_amount", col("fare_amount") * 1.1)

        # Join with payment_data and vendor_data
        transformed_data = transformed_data.join(self.payment_data, on="payment_type", how="left")
        transformed_data = transformed_data.join(self.vendor_data, on="vendor_id", how="left")

        logger.info("Example data processing completed")
        return transformed_data
