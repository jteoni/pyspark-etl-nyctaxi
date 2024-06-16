import unittest
import sys
import os
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))
from scripts.extract import Extract


class TestExtract(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("ETL Pipeline Test") \
            .master("local[2]") \
            .getOrCreate()
        cls.data_dir = "data/"

    def test_load_taxi_data(self):
        extractor = Extract(self.data_dir)
        taxi_data = extractor.load_taxi_data()
        self.assertIsNotNone(taxi_data)
        self.assertGreater(taxi_data.count(), 0)

    def test_load_payment_lookup(self):
        extractor = Extract(self.data_dir)
        payment_data = extractor.load_payment_lookup()
        self.assertIsNotNone(payment_data)
        self.assertGreater(payment_data.count(), 0)

    def test_load_vendor_lookup(self):
        extractor = Extract(self.data_dir)
        vendor_data = extractor.load_vendor_lookup()
        self.assertIsNotNone(vendor_data)
        self.assertGreater(vendor_data.count(), 0)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
