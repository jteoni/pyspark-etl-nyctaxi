import unittest
import os
from pyspark.sql import SparkSession, Row
from scripts.load import Load


class TestLoad(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("ETL Pipeline Test") \
            .master("local[2]") \
            .getOrCreate()
        cls.output_dir = "output/"
        os.makedirs(cls.output_dir, exist_ok=True)

    def test_save_data(self):
        loader = Load(self.output_dir)
        df = self.spark.createDataFrame([
            Row(vendor_id="CMT", payment_type="Cash", fare_amount=10.0, total_amount=12.0)
        ])
        loader.save_data(df)
        output_path = os.path.join(self.output_dir, "etl_output.csv")
        self.assertTrue(os.path.exists(output_path))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        # Clean up the output directory after the test
        output_path = os.path.join(cls.output_dir, "etl_output.csv")
        if os.path.exists(output_path):
            os.remove(output_path)


if __name__ == '__main__':
    unittest.main()
