from pyspark.sql import SparkSession

class FakeDataFrame():
    
    def __init__(self, spark=SparkSession.builder.getOrCreate()) -> SparkSession:
        self.spark = spark

    def fake_frame(self):
        return self.spark.range(0,1000)
    
    def write_fake_frame(self, df, destination_path):
        return df.write.mode("overwrite").format("delta").save(destination_path)

f = FakeDataFrame()

df = f.fake_frame()
df.show(vertical=True)

f.write_fake_frame(df, destination_path="s3://owshq-bronze-777696598735/fake/")