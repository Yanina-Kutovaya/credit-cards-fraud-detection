spark-submit \
--jars mlflow-spark-1.27.0.jar \
ml_flow.py \
--train_artifact "s3a://airflow-cc-input/train.csv" \
--output_artifact Fraud_Detection_Model_Pipline