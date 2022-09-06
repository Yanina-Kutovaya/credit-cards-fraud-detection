ubuntu@rc1a-dataproc-m-egdf4spmi4u42kty:~$ /usr/bin/pyspark
Python 3.8.10 (default, Jun  4 2021, 15:09:15)
[GCC 7.5.0] :: Anaconda, Inc. on linux
Type "help", "copyright", "credits" or "license" for more information.
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/lib/spark/jars/slf4j-log4j12-1.7.30.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/lib/hadoop/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
2022-09-06 08:35:22,996 WARN util.Utils: spark.executor.instances less than spark.dynamicAllocation.minExecutors is invalid, ignoring its setting, please update your configs.
2022-09-06 08:35:29,965 WARN util.Utils: spark.executor.instances less than spark.dynamicAllocation.minExecutors is invalid, ignoring its setting, please update your configs.
2022-09-06 08:35:29,984 WARN cluster.YarnSchedulerBackend$YarnSchedulerEndpoint: Attempted to request executors before the AM has registered!
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.0.3
      /_/

Using Python version 3.8.10 (default, Jun  4 2021 15:09:15)
SparkSession available as 'spark'.
>>> APP_NAME = 'MLFlow and Automated Retraining'
>>> import logging
>>> from datetime import datetime
>>> from pyspark import SparkFiles
>>> from pyspark.sql import SparkSession
>>> import mlflow
>>> from mlflow.tracking import MlflowClient
>>> from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
>>> from pyspark.ml.evaluation import BinaryClassificationEvaluator
>>> from custom_transformers import *
>>> from fraud_detection_model_pipeline import get_fraud_detection_model_pipeline
>>> logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
>>> logger = logging.getLogger()
>>> logger.info('Creating Spark Session ...')
2022-09-06 08:37:44,470 Creating Spark Session ...
>>> spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
>>> logger.info('Loading data ...')
2022-09-06 08:38:04,116 Loading data ...
>>> data = spark.read.parquet('train.parquet')
>>> client = MlflowClient()
>>> experiment_id = client.create_experiment('Spark_GBTClassifier_v1')
>>> run_name = f'Credit_cards_fraud_detection_pipline {str(datetime.now())}' 
>>> with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
...     inf_pipeline = get_fraud_detection_model_pipeline()
...     classifier = inf_pipeline.getStages()[-1]
...     paramGrid = (ParamGridBuilder()
...                     .addGrid(classifier.maxDepth, [10])
...                     .addGrid(classifier.minInstancesPerNode, [100])
...                     .build())
...     evaluator = BinaryClassificationEvaluator(labelCol='isFraud', rawPredictionCol="rawPrediction",
...                     metricName="areaUnderROC")
...     trainRatio = 0.8
...     tvs = TrainValidationSplit(estimator=inf_pipeline, estimatorParamMaps=paramGrid,
...                                     evaluator=evaluator, trainRatio=trainRatio, parallelism=2)
...     logger.info("Fitting new inference pipeline ...")
...     model = tvs.fit(data)
...     run_id = mlflow.active_run().info.run_id
...     logger.info(f"Logging optimal parameters to MLflow run {run_id} ...")
...     best_maxDepth = model.bestModel.stages[-1].getMaxDepth()
...     best_minInstancesPerNode = model.bestModel.stages[-1].getMinInstancesPerNode()
...     logger.info(model.bestModel.stages[-1].explainParam('maxDepth'))
...     logger.info(model.bestModel.stages[-1].explainParam('minInstancesPerNode'))
...     mlflow.log_param('maxDepth', best_maxDepth)
...     mlflow.log_param('minInstancesPerNode', best_minInstancesPerNode)
...     logger.info('Scoring the model ...')
...     predictions = model.transform(data)
...     roc_auc = evaluator.evaluate(predictions)
...     logger.info(f'Logging metrics to MLflow run {run_id} ...')
...     mlflow.log_metric('roc_auc', roc_auc)
...     logger.info(f'Model roc_auc: {roc_auc}')
...     logger.info("Saving pipeline ...")
...     mlflow.spark.save_model(model, 'Spark_GBTClassifier_v1')
...     logger.info("Exporting/logging pipline ...")
...     mlflow.spark.log_model(model, args.output_artifact)
...     logger.info('Done')
...
2022/09/06 08:48:38 WARNING mlflow.utils.git_utils: Failed to import Git (the Git executable is probably not on your PATH), so Git SHA is not available. Error: Failed to initialize: Bad git executable.       
The git executable must be specified in one of the following ways:
    - be included in your $PATH
    - be set via $GIT_PYTHON_GIT_EXECUTABLE
    - explicitly set via git.refresh()

All git commands will error until this is rectified.

This initial warning can be silenced or aggravated in the future by setting the
$GIT_PYTHON_REFRESH environment variable. Use one of the following values:
    - quiet|q|silence|s|none|n|0: for no warning or exception
    - warn|w|warning|1: for a printed warning
    - error|e|raise|r|2: for a raised exception

Example:
    export GIT_PYTHON_REFRESH=quiet

2022-09-06 08:48:38,773 Fitting new inference pipeline ...
2022-09-06 08:48:39,921 WARN util.package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
2022-09-06 08:50:22,227 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1002.1 KiB2022-09-06 08:50:22,458 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1011.9 KiB2022-09-06 08:50:22,713 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1024.9 KiB2022-09-06 08:50:22,949 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1016.7 KiB2022-09-06 08:50:23,298 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1017.2 KiB2022-09-06 08:50:23,511 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1017.8 KiB2022-09-06 08:50:23,812 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1018.9 KiB2022-09-06 08:50:24,066 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1021.0 KiB2022-09-06 08:50:24,292 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1024.5 KiB2022-09-06 08:50:24,530 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1031.4 KiB2022-09-06 08:50:24,765 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1041.6 KiB2022-09-06 08:50:25,009 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1055.4 KiB2022-09-06 08:50:25,278 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1072.9 KiB2022-09-06 08:50:25,532 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1058.3 KiB2022-09-06 08:50:25,869 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1058.8 KiB2022-09-06 08:50:26,060 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1059.4 KiB2022-09-06 08:50:26,239 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1060.5 KiB2022-09-06 08:50:26,474 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1062.8 KiB2022-09-06 08:50:26,765 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1066.8 KiB2022-09-06 08:50:27,155 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1072.2 KiB2022-09-06 08:50:27,424 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1080.4 KiB2022-09-06 08:50:27,646 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1091.1 KiB2022-09-06 08:50:27,889 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1107.8 KiB2022-09-06 08:50:32,834 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1095.1 KiB2022-09-06 08:51:07,323 WARN storage.BlockManager: Asked to remove block broadcast_726, which does not exist
2022-09-06 08:51:43,075 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1011.0 KiB2022-09-06 08:51:43,336 Logging optimal parameters to MLflow run f4eb564e81a14a3a81bb0cdb5587ad63 ...
2022-09-06 08:51:43,336 maxDepth: Maximum depth of the tree. (>= 0) E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (default: 5, current: 10)
2022-09-06 08:51:43,336 minInstancesPerNode: Minimum number of instances each child must have after split. If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid. Should be >= 1. (default: 1, current: 100)
2022-09-06 08:51:43,338 Scoring the model ...
2022-09-06 08:51:46,474 WARN scheduler.DAGScheduler: Broadcasting large task binary with size 1001.0 KiBchannel 3: open failed: connect failed: Connection refused
channel 3: open failed: connect failed: Connection refused
2022-09-06 08:51:50,183 Logging metrics to MLflow run f4eb564e81a14a3a81bb0cdb5587ad63 ...
2022-09-06 08:51:50,184 Model roc_auc: 0.7960545116741903
2022-09-06 08:51:50,184 Saving pipeline ...
Traceback (most recent call last):
  File "<stdin>", line 30, in <module>
  File "/home/ubuntu/.local/lib/python3.8/site-packages/mlflow/spark.py", line 651, in save_model
    spark_model.save(tmp_path)
  File "/usr/lib/spark/python/pyspark/ml/util.py", line 224, in save
    self.write().save(path)
  File "/usr/lib/spark/python/pyspark/ml/util.py", line 128, in save
    self.saveImpl(path)
  File "/usr/lib/spark/python/pyspark/ml/pipeline.py", line 226, in saveImpl
    PipelineSharedReadWrite.saveImpl(self.instance, stages, self.sc, path)
  File "/usr/lib/spark/python/pyspark/ml/pipeline.py", line 360, in saveImpl
    stage.write().save(PipelineSharedReadWrite
  File "/usr/lib/spark/python/pyspark/ml/tuning.py", line 927, in write
    return JavaMLWriter(self)
  File "/usr/lib/spark/python/pyspark/ml/util.py", line 168, in __init__
    _java_obj = instance._to_java()
  File "/usr/lib/spark/python/pyspark/ml/tuning.py", line 978, in _to_java
    self.bestModel._to_java(),
  File "/usr/lib/spark/python/pyspark/ml/pipeline.py", line 316, in _to_java
    java_stages[idx] = stage._to_java()
AttributeError: 'DiscreteToBinaryTransformer' object has no attribute '_to_java'
>>>