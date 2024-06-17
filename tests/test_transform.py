import unittest
import sys
import os
import tempfile
from pyspark.sql import SparkSession, Row
import logging

# Add the root directory of the project to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.transform import Transform

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_taxi_data(spark):
    return spark.createDataFrame([
        Row(vendor_id="CMT", payment_type="Cash", fare_amount=10.0, total_amount=12.0),
        Row(vendor_id="VTS", payment_type="Credit", fare_amount=20.0, total_amount=25.0)
    ])


def create_payment_data(spark):
    return spark.createDataFrame([
        Row(payment_type="Cash", payment_lookup="Cash"),
        Row(payment_type="Credit", payment_lookup="Credit")
    ])


def create_vendor_data(spark):
    return spark.createDataFrame([
        Row(vendor_id="CMT", name="Creative Mobile Technologies"),
        Row(vendor_id="VTS", name="VeriFone Inc")
    ])


class TestTransform(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session
        cls.spark = SparkSession.builder \
            .appName("ETL Pipeline Test") \
            .master("local[2]") \
            .getOrCreate()

        # Create a temporary directory for storing test data
        cls.temp_dir = tempfile.mkdtemp()

        # Create sample data DataFrames
        cls.taxi_data = create_taxi_data(cls.spark)
        cls.payment_data = create_payment_data(cls.spark)
        cls.vendor_data = create_vendor_data(cls.spark)

    def test_process_data(self):
        # Initialize the Transform class with sample data
        transformer = Transform(self.taxi_data, self.payment_data, self.vendor_data)

        # Process the data using the transform class
        output_path = os.path.join(self.temp_dir, "output")
        transformed_data = transformer.process_data(output_path)

        # Assert the transformed data is not None and contains expected columns
        self.assertIsNotNone(transformed_data)
        self.assertIn("new_fare_amount", transformed_data.columns)
        self.assertIn("payment_lookup", transformed_data.columns)
        self.assertIn("name", transformed_data.columns)

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session and remove the temporary directory
        cls.spark.stop()
        import shutil
        shutil.rmtree(cls.temp_dir)


if __name__ == '__main__':
    unittest.main()
