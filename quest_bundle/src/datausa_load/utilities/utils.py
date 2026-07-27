from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re

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

from pyspark.sql.types import StructType, ArrayType, MapType, StructField

def clean_column_names(df):
    """Recursively clean column names in DataFrame and nested StructTypes for Delta Lake compatibility."""
    def clean_name(name):
        return name.replace('\t', '').replace(' ', '_').strip('_').lower()

    def clean_schema(schema):
        if isinstance(schema, StructType):
            fields = []
            for field in schema.fields:
                new_name = clean_name(field.name)
                new_type = clean_schema(field.dataType)
                fields.append(StructField(new_name, new_type, field.nullable, field.metadata))
            return StructType(fields)
        elif isinstance(schema, ArrayType):
            return ArrayType(clean_schema(schema.elementType), schema.containsNull)
        elif isinstance(schema, MapType):
            return MapType(clean_schema(schema.keyType), clean_schema(schema.valueType), schema.valueContainsNull)
        else:
            return schema

    cleaned_schema = clean_schema(df.schema)
    df = df.toDF(*[clean_name(col) for col in df.columns])
    return df.select([df[col].cast(cleaned_schema[col].dataType).alias(col) for col in df.columns])