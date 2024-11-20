from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession

# Step 1: Initialize SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)

# Step 2: Create a Spark DataFrame (or load your data into a DataFrame)
spark = SparkSession.builder.getOrCreate()
data = [("John", 28), ("Jane", 32), ("Sam", 45)]
columns = ["Name", "Age"]

# Create the DataFrame
df = spark.createDataFrame(data, columns)

# Step 3: Convert DataFrame to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Step 4: Define the S3 output path
output_path = "s3://oxy-glue-cicd-bucket/output/"

# Step 5: Write the DynamicFrame to Parquet format on S3
glueContext.write_dynamic_frame.from_options(
    dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet"
)

print('Parquet files created')