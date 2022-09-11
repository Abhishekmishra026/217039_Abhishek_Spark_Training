from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import org.apache.spark.sql.functions.lit
from pyspark.sql.functions import udf

sc = SparkContext("local[4]", "pysparkWordCount")

spark = SparkSession.builder.appName('sparkWordCount').getOrCreate()

# Disable the Logs
spark.sparkContext.setLogLevel("WARN")

ssc = StreamingContext(sc, 5)

lines = ssc.socketTextStream("localhost", 9999)

amazon_DF = spark.read.csv("/usr/hadoop/all_transaction_1.csv")
amazon_DF.show(10, truncate=False)


revenue_range = udf(lambda revenue : 'Over Achieved' if TransactionAmount < 80,000,000  else
                              'On Target' if (TransactionAmount >= 60,000,000  and TransactionAmount <=80,000,000) else
                              'Under Achieved' if (TransactionAmount > 60,000,000)

amazon_DF.withColumn("Total_revenue",revenue_range(amazon_DF.TransactionAmount))

amazon_DF.show()


ssc.start()
ssc.awaitTermination()
