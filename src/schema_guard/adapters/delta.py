from __future__ import annotations

from typing import Optional,Dict,TYPE_CHECKING
from ..core.report import check_schema_transfer, format_result

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

try:
    import pyspark
    PYSPARK_AVAILABLE = True
except:
    PYSPARK_AVAILABLE = False

class compareDelta:

    
    def __init__(self,source_schema:str,target_schema:Optional[str]=None,SparkSession:Optional[SparkSession]=None,return_type:Dict=None):
        self.source_schema = source_schema
        self.target_schema = target_schema if target_schema else source_schema
        self.spark = SparkSession
        self.return_type = return_type

    
    def read_schema(self,compare_version:Optional[str]=None):
        if not compare_version:
            source_Struct = self.spark.read.table(self.source_schema).schema
            source_schema = self.spark.read.table(self.target_schema).schema
            return source_schema,self.target_schema
        else:
            source_Struct = self.spark.read.table(self.source_schema).
        
    
    def compare_delta_tables(self):
        """
        Compare schemas of two live Delta tables.
        """
        if PYSPARK_AVAILABLE:
            source_Struct = self.read_schema(self.source_schema)
            target_Struct = self.read_schema(self.target_schema)

            result = check_schema_transfer(source_Struct,target_Struct)
            comparison_report = format_result(result)
            return comparison_report

        else:
            return EnvironmentError("Pyspark Not installed.Please install Pyspark to proceed with delta table comparison")
        
    def compare_delta_versions(source_schema:str,source_version:int, target_schema:str,target_version:int, sparkSession:Optional[SparkSession]=None,return_type: Dict=None)->Dict:
        """
        Compare schemas of the same Delta table 
        at two different versions.
        """
        pass