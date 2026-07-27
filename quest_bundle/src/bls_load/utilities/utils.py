from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re

# ============================================
# General Utilities
# ============================================

@udf(returnType=BooleanType())
def is_valid_email(email):
    """
    This function checks if the given email address has a valid format using regex.
    Returns True if valid, False otherwise.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if email is None:
        return False
    return re.match(pattern, email) is not None


def clean_column_names(df):
    """Clean column names by removing tabs and extra spaces to ensure Delta Lake compatibility."""
    for old_col in df.columns:
        new_col = old_col.replace('\t', '').replace(' ', '_').strip('_')
        df = df.withColumnRenamed(old_col, new_col)
    return df


# ============================================
# BLS (Bureau of Labor Statistics) Source
# ============================================

bls_options = {'header': 'true', 'inferSchema': 'true', 'sep': '\t'}


# ============================================
# Future Data Sources
# Add source-specific configurations and utilities below
# ============================================
