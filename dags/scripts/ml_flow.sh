spark-submit \
--jars mlflow-spark-1.27.0.jar \
ml_flow.py \
--train_artifact 's3a://airflow-cc-input/train.parquet' \
--output_artifact 'Spark_GBTClassifier_v1'