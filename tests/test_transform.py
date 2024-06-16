import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from scripts.transform import Transform


class TestTransform(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("ETL Pipeline Test") \
            .master("local[2]") \
            .getOrCreate()

        cls.taxi_data = cls.spark.createDataFrame([
            Row(vendor_id="CMT", payment_type="Cash", fare_amount=10.0, total_amount=12.0),
            Row(vendor_id="VTS", payment_type="Credit", fare_amount=20.0, total_amount=25.0)
        ])

        cls.payment_data = cls.spark.createDataFrame([
            Row(payment_type="Cash", payment_lookup="Cash"),
            Row(payment_type="Credit", payment_lookup="Credit")
        ])

        cls.vendor_data = cls.spark.createDataFrame([
            Row(vendor_id="CMT", name="Creative Mobile Technologies"),
            Row(vendor_id="VTS", name="VeriFone Inc")
        ])

    def test_process_data(self):
        transformer = Transform(self.taxi_data, self.payment_data, self.vendor_data)
        transformed_data = transformer.process_data()
        self.assertIsNotNone(transformed_data)
        self.assertIn("new_fare_amount", transformed_data.columns)
        self.assertIn("payment_lookup", transformed_data.columns)
        self.assertIn("name", transformed_data.columns)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
