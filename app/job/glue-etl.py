from pyspark.sql import SparkSession
from delta.tables import DeltaTable

class FakeDataFrame():
    
    def __init__(self,
                spark=SparkSession.builder.getOrCreate(), 
                destination_path="s3://owshq-bronze-777696598735/fake/"
                ) -> SparkSession:
        
        self.spark = spark
        self.destination_path = destination_path

    def fake_frame(self):
        return self.spark.range(0,1000)
    
    def write_fake_frame(self, df):
        return (
            df.coalesce(1).write.mode("overwrite").format("delta").save(self.destination_path)
        )

    def show_history_table(self):
        return DeltaTable.forPath(self.spark, self.destination_path).history().show(truncate=False, vertical=True)

f = FakeDataFrame()

df = f.fake_frame()
df.show(vertical=True)

f.write_fake_frame(df)
f.show_history_table()