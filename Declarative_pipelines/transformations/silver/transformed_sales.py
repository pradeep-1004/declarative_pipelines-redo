import dlt
from pyspark.sql.functions import *

#transforming sales data
@dlt.view(
    name='append_sales_views'
)
def append_sales_views():
  df = spark.readStream.table('sales_stg')
  df = df.withColumn('total_amount',col('quantity')*col('amount'))
  return df


dlt.create_streaming_table(
    name='sales_enr'
)

dlt.create_auto_cdc_flow(
    target='sales_enr',
    source='append_sales_views',
    keys=['sales_id'],
    sequence_by='sale_timestamp',
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None

)