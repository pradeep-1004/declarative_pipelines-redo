import dlt


@dlt.table(
    name = 'first_stream_table'
)

def first_stream_table():

    df = spark.readStream.table('declarative_pipeline.practice.orders')
    return df

