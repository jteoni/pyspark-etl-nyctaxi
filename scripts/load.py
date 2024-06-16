from pyspark.sql import DataFrame


class Load:
    def __init__(self, output_dir):
        self.output_dir = output_dir

    def save_data(self, df: DataFrame):
        output_path = self.output_dir + "/etl_output.csv"
        df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
