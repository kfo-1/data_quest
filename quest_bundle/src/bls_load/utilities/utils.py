from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re

# ============================================
# General Utilities
# ============================================

def clean_column_names(df):
    """Clean column names by removing tabs and extra spaces to ensure Delta Lake compatibility."""
    for old_col in df.columns:
        new_col = old_col.replace('\t', '').replace(' ', '_').strip('_')
        df = df.withColumnRenamed(old_col, new_col.lower())
    return df


# ============================================
# BLS (Bureau of Labor Statistics) Source
# ============================================

bls_options = {'header': 'true', 'inferSchema': 'true', 'sep': '\t'}


# ============================================
# Future Data Sources
# Add source-specific configurations and utilities below
# ============================================
