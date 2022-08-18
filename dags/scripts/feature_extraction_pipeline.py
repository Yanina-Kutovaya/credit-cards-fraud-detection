import pyspark.ml.feature as MF
from pyspark.ml import Pipeline
from custom_transformers import *


IDENTIFIERS = ['TransactionID']
TARGET_COLUMN = ['isFraud']
TIME_COLUMNS = ['TransactionDT']

## Binary columnes passed Chi2 test (EDA)
BINARY_FEATURES = ['V286', 'V26']

## Categorical columns passed Chi2 test (EDA)
CATEGORICAL_FEATURES = []

## Discrete columnes passed Chi2 test (EDA)
DISCRETE_FEATURES = ['weekdays', 'minutes']

## Continuous columns with correlation < 0.8 (EDA)
CONTINUOUS_FEATURES = [
    'addr1', 'addr2', 'C7', 'V97', 'V183', 'V236', 'V279', 
    'V280', 'V290', 'V306', 'V308', 'V317'
    ]
max_thresholds = {
    'addr1': 540.0,
    'addr2': 87.0,
    'C7': 10.0,
    'V97': 7.0,
    'V183': 4.0,
    'V236': 3.0,
    'V279': 5.0,
    'V280': 9.0,
    'V290': 3.0,
    'V306': 938.0,
    'V308': 1649.5,
    'V317': 1762.0
    }
# 0, >0
binary_1 = ['V26', 'V286']


def get_feature_extraction_pipeline():
    discrete_to_binary = DiscreteToBinaryTransformer(
        inputCols=binary_1,
        outputCols=binary_1
        )
    cap_countinuous_outliers = ContinuousOutliersCapper(
        maximum=list(max_thresholds.values()),
        inputCols=list(max_thresholds.keys())
        ) 
    get_time_features = TimeFeaturesGenerator(inputCols=TIME_COLUMNS)
    make_string_columns_from_discrete = StringFromDiscrete(
        inputCols=BINARY_FEATURES,
        outputCols= [var + '_str' for var in BINARY_FEATURES]    
        )
    discrete_fill_nan = ScalarNAFiller(
        inputCols=BINARY_FEATURES + DISCRETE_FEATURES,
        outputCols=BINARY_FEATURES + DISCRETE_FEATURES,
        filler=-999
        )
    continuous_fill_nan = ScalarNAFiller(
        inputCols=CONTINUOUS_FEATURES,
        outputCols=CONTINUOUS_FEATURES,
        filler=0
        )
    binary_string_indexer = MF.StringIndexer(
        inputCols=[i + '_str' for i in BINARY_FEATURES], 
        outputCols=[i + '_index' for i in BINARY_FEATURES],
        handleInvalid='keep'
        )
    binary_one_hot_encoder = MF.OneHotEncoder(
        inputCols=[i + '_index' for i in BINARY_FEATURES], 
        outputCols=[i + '_encoded' for i in BINARY_FEATURES],    
        )
    discrete_features_assembler = MF.VectorAssembler(
        inputCols=DISCRETE_FEATURES, 
        outputCol='discrete_assembled'
        )
    discrete_minmax_scaler = MF.MinMaxScaler(
        inputCol='discrete_assembled', 
        outputCol='discrete_vector_scaled'
        )
    continuous_features_assembler = MF.VectorAssembler(
        inputCols=CONTINUOUS_FEATURES, 
        outputCol='continuous_assembled'
        )
    continuous_robust_scaler = MF.RobustScaler(
        inputCol='continuous_assembled', 
        outputCol='continuous_vector_scaled'
        )
    binary_vars = [i + '_encoded' for i in BINARY_FEATURES]
    vars = binary_vars + ['discrete_vector_scaled', 'continuous_vector_scaled']
    features_assembler = MF.VectorAssembler(
        inputCols=vars,
        outputCol='features'
        )
    
    feature_extraction_pipeline = Pipeline(
        stages=[        
            discrete_to_binary,
            cap_countinuous_outliers,
            get_time_features,
            discrete_fill_nan,
            continuous_fill_nan,        
            make_string_columns_from_discrete,
            binary_string_indexer,
            binary_one_hot_encoder,
            discrete_features_assembler,
            discrete_minmax_scaler,        
            continuous_features_assembler,
            continuous_robust_scaler,
            features_assembler
            ]
        )
    return feature_extraction_pipeline