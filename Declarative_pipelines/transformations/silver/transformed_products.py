import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#transforming products view
@dlt.view(
    name='product_stg_views'
)
def product_stg_views():
  df = spark.readStream.table('products_stg')
  df = df.withColumn('price',col('price').cast(IntegerType()))
  return df


dlt.create_streaming_table(
    name='products_enr'
)

dlt.create_auto_cdc_flow(
    target='products_enr',
    source='product_stg_views',
    keys=['product_id'],
    sequence_by='last_updated',
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None

)