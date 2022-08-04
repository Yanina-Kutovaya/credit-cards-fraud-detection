import pyspark.sql.functions as F
from pyspark import keyword_only
from pyspark.ml import Estimator, Model, Transformer
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import (
    HasInputCol,
    HasInputCols,
    HasOutputCol,
    HasOutputCols,
)
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.types import IntegerType, StringType 


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


class DiscreteToBinaryTransformer(
    Transformer,
    HasInputCol,
    HasOutputCol,
    HasInputCols,
    HasOutputCols,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Consolidates discrete variables to groups 0, >0."""

    @keyword_only
    def __init__(
        self,
        inputCol=None,
        outputCol=None,
        inputCols=None,
        outputCols=None
    ):
        super().__init__()        
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
        self,
        inputCol=None,
        outputCol=None,
        inputCols=None,
        outputCols=None
    ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, new_inputCol):
        return self.setParams(inputCol=new_inputCol)

    def setOutputCol(self, new_outputCol):
        return self.setParams(outputCol=new_outputCol)

    def setInputCols(self, new_inputCols):
        return self.setParams(inputCols=new_inputCols)

    def setOutputCols(self, new_outputCols):
        return self.setParams(outputCols=new_outputCols)

    def checkParams(self):
        # Test #1: either inputCol or inputCols can be set (but not both).
        if self.isSet("inputCol") and (self.isSet("inputCols")):
            raise ValueError(
                "Only one of `inputCol` and `inputCols`" "must be set."
            )

        # Test #2: at least one of inputCol or inputCols must be set.
        if not (self.isSet("inputCol") or self.isSet("inputCols")):
            raise ValueError(
                "One of `inputCol` or `inputCols` must be set."
            )

        # Test #3: if `inputCols` is set, then `outputCols`
        # must be a list of the same len()
        if self.isSet("inputCols"):
            if len(self.getInputCols()) != len(self.getOutputCols()):
                raise ValueError(
                    "The length of `inputCols` does not match"
                    " the length of `outputCols`"
                    )

    def _transform(self, dataset):
        self.checkParams()

        # If `inputCol` / `outputCol`, we wrap into a single-item list
        input_columns = (
            [self.getInputCol()]
            if self.isSet("inputCol")
            else self.getInputCols()
            )
        output_columns = (
            [self.getOutputCol()]
            if self.isSet("outputCol")
            else self.getOutputCols()
            )      
        for var_in, var_out in zip(input_columns, output_columns):
          dataset = dataset.withColumn(
              var_out, F.when(F.col(var_in) > 0, 1).otherwise(F.col(var_in))
              )
        return dataset


class ContinuousOutliersCapper(
    Transformer,
    HasInputCol,
    HasInputCols,  
    DefaultParamsReadable,
    DefaultParamsWritable
    ):
    """
    Caps max values of continuous variables by 99th percentile values if 
    the difference between max value and 99th percentile value exceeds 
    standard deviation.
    """
    maximum = Param(
        Params._dummy(),
       'maximum',
       "Values we want to replace our outliers values with.",
        typeConverter=TypeConverters.toListFloat
        )

    @keyword_only
    def __init__(self, inputCol=None, inputCols=None, maximum=None):     
        super().__init__()        
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, inputCols=None, maximum=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, new_inputCol):
        return self.setParams(inputCol=new_inputCol)
    
    def setInputCols(self, new_inputCols):
        return self.setParams(inputCols=new_inputCols)

    def setMaximum(self, new_maximum):
        return self.setParams(maximum=new_maximum)   

    def getMaximum(self):
        return self.getOrDefault(self.maximum)

    def checkParams(self):
        # Test #1: either inputCol or inputCols can be set (but not both).
        if self.isSet("inputCol") and (self.isSet("inputCols")):
            raise ValueError(
                "Only one of `inputCol` and `inputCols`" "must be set."
            )

        # Test #2: at least one of inputCol or inputCols must be set.
        if not (self.isSet("inputCol") or self.isSet("inputCols")):
            raise ValueError(
                "One of `inputCol` or `inputCols` must be set."
            )
         
    def _transform(self, dataset):
        self.checkParams()

        # If `inputCol`, we wrap into a single-item list  
        input_columns = (
            [self.getInputCol()]
            if self.isSet("inputCol")
            else self.getInputCols()
            )
        max_thresholds = (
            [self.getMaximum()]
            if self.isSet("inputCol")
            else self.getMaximum()
            )
        for k, v in zip(input_columns, max_thresholds):
            dataset = dataset.withColumn(
                k, F.when(F.isnull(F.col(k)), F.col(k)
                ).otherwise(F.least(F.col(k), F.lit(v)))
                ) 
        return dataset


class TimeFeaturesGenerator(
    Transformer,
    HasInputCol,    
    HasInputCols,    
    DefaultParamsReadable,
    DefaultParamsWritable
    ):
    """
    Generates weekdays, hours and minutes from time variable.
    """
    @keyword_only
    def __init__(self, inputCol=None, inputCols=None):
        super().__init__()        
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
        self,
        inputCol=None,        
        inputCols=None
        ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, new_inputCol):
        return self.setParams(inputCol=new_inputCol)
    
    def setInputCols(self, new_inputCols):
        return self.setParams(inputCols=new_inputCols)    

    def checkParams(self):
        # Test #1: either inputCol or inputCols can be set (but not both).
        if self.isSet("inputCol") and (self.isSet("inputCols")):
            raise ValueError(
                "Only one of `inputCol` and `inputCols`" "must be set."
            )

        # Test #2: at least one of inputCol or inputCols must be set.
        if not (self.isSet("inputCol") or self.isSet("inputCols")):
            raise ValueError(
                "One of `inputCol` or `inputCols` must be set."
            )

    def _transform(self, dataset):
        self.checkParams()

        # If `inputCol`, we wrap into a single-item list
        input_columns = (
            [self.getInputCol()]
            if self.isSet("inputCol")
            else self.getInputCols()
        )        
        w = 60 * 60 * 24 * 7
        d = 60 * 60 * 24
        h = 60 * 60
        m = 60 
        time_var = input_columns[0] 
        dataset = dataset.withColumn(
            'weekdays', (F.col(time_var) % w / d).cast(IntegerType())
            )
        dataset = dataset.withColumn(
            'hours', (F.col(time_var) % d / h).cast(IntegerType())
            )
        dataset = dataset.withColumn(
            'minutes', (F.col(time_var) % d % h / m).cast(IntegerType())
            )      
       
        return dataset


class ScalarNAFiller(
    Transformer,
    HasInputCol,
    HasOutputCol,
    HasInputCols,
    HasOutputCols,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Fills the `null` values of inputCol with a scalar value `filler`."""

    filler = Param(
        Params._dummy(),
        "filler",
        "Value we want to replace our null values with.",
        typeConverter=TypeConverters.toFloat
        )
    @keyword_only
    def __init__(
        self,
        inputCol=None,
        outputCol=None,
        inputCols=None,
        outputCols=None,
        filler=None
        ):
        super().__init__()
        self._setDefault(filler=None)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
        self,
        inputCol=None,
        outputCol=None,
        inputCols=None,
        outputCols=None,
        filler=None
        ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setFiller(self, new_filler):
        return self.setParams(filler=new_filler)

    def setInputCol(self, new_inputCol):
        return self.setParams(inputCol=new_inputCol)

    def setOutputCol(self, new_outputCol):
        return self.setParams(outputCol=new_outputCol)

    def setInputCols(self, new_inputCols):
        return self.setParams(inputCols=new_inputCols)

    def setOutputCols(self, new_outputCols):
        return self.setParams(outputCols=new_outputCols)

    def getFiller(self):
        return self.getOrDefault(self.filler)

    def checkParams(self):
        # Test #1: either inputCol or inputCols can be set (but not both).
        if self.isSet("inputCol") and (self.isSet("inputCols")):
            raise ValueError(
                "Only one of `inputCol` and `inputCols`" "must be set."
            )

        # Test #2: at least one of inputCol or inputCols must be set.
        if not (self.isSet("inputCol") or self.isSet("inputCols")):
            raise ValueError(
                "One of `inputCol` or `inputCols` must be set."
            )

        # Test #3: if `inputCols` is set, then `outputCols`
        # must be a list of the same len()
        if self.isSet("inputCols"):
            if len(self.getInputCols()) != len(self.getOutputCols()):
                raise ValueError(
                    "The length of `inputCols` does not match"
                    " the length of `outputCols`"
                )

    def _transform(self, dataset):
        self.checkParams()

        # If `inputCol` / `outputCol`, we wrap into a single-item list
        input_columns = (
            [self.getInputCol()]
            if self.isSet("inputCol")
            else self.getInputCols()
            )
        output_columns = (
            [self.getOutputCol()]
            if self.isSet("outputCol")
            else self.getOutputCols()
            )

        answer = dataset

        # If input_columns == output_columns, we overwrite and no need to create
        # new columns.
        if input_columns != output_columns:
            for in_col, out_col in zip(input_columns, output_columns):
                answer = answer.withColumn(out_col, F.col(in_col))

        na_filler = self.getFiller()
        return dataset.fillna(na_filler, output_columns)


class StringFromDiscrete(
    Transformer,
    HasInputCol,
    HasOutputCol,
    HasInputCols,
    HasOutputCols,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Transforms discrete variables to string format (for one-hot encoding)."""

    @keyword_only
    def __init__(
        self,
        inputCol=None,
        outputCol=None,
        inputCols=None,
        outputCols=None
        ):
        super().__init__()        
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
        self,
        inputCol=None,
        outputCol=None,
        inputCols=None,
        outputCols=None 
        ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, new_inputCol):
        return self.setParams(inputCol=new_inputCol)

    def setOutputCol(self, new_outputCol):
        return self.setParams(outputCol=new_outputCol)

    def setInputCols(self, new_inputCols):
        return self.setParams(inputCols=new_inputCols)

    def setOutputCols(self, new_outputCols):
        return self.setParams(outputCols=new_outputCols)

    def checkParams(self):
        # Test #1: either inputCol or inputCols can be set (but not both).
        if self.isSet("inputCol") and (self.isSet("inputCols")):
            raise ValueError(
                "Only one of `inputCol` and `inputCols`" "must be set."
            )

        # Test #2: at least one of inputCol or inputCols must be set.
        if not (self.isSet("inputCol") or self.isSet("inputCols")):
            raise ValueError(
                "One of `inputCol` or `inputCols` must be set."
            )

        # Test #3: if `inputCols` is set, then `outputCols`
        # must be a list of the same len()
        if self.isSet("inputCols"):
            if len(self.getInputCols()) != len(self.getOutputCols()):
                raise ValueError(
                    "The length of `inputCols` does not match"
                    " the length of `outputCols`"
                )

    def _transform(self, dataset):
        self.checkParams()

        # If `inputCol` / `outputCol`, we wrap into a single-item list
        input_columns = (
            [self.getInputCol()]
            if self.isSet("inputCol")
            else self.getInputCols()
        )
        output_columns = (
            [self.getOutputCol()]
            if self.isSet("outputCol")
            else self.getOutputCols()
        )       

        for var_in, var_out in zip(input_columns, output_columns):
          dataset = dataset.withColumn(
              var_out,
              dataset[var_in].cast(StringType())
          )
        return dataset


import pyspark.ml.feature as MF
from pyspark.ml import Pipeline

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