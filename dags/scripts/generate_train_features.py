app_name = 'train_data_preprocessing'
spark_ui_port = 4041

import pyspark
from pyspark import SparkFiles
from scripts.feature_extraction_pipeline_v1 import get_feature_extraction_pipeline
from scripts.s3_operator import S3_operator as op

spark = (
    pyspark.sql.SparkSession.builder
        .appName(app_name)        
        .config("spark.ui.port", spark_ui_port)
        .getOrCreate()
        )
df_train = op.get_object('train.parquet').repartition(8)
feature_extraction_pipeline = get_feature_extraction_pipeline()
feature_extraction_pipeline_model = feature_extraction_pipeline.fit(df_train)
df_train = feature_extraction_pipeline_model.transform(df_train)

feature_extraction_pipeline_model.save('feature_extraction_pipeline_model_v1')
df_train.write.parquet('train_features.parquet')
op.send_object('feature_extraction_pipeline_model_v1')
op.send_object('train_features.parquet')   