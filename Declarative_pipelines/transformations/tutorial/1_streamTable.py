'''import dlt

#create streaming table 
@dlt.table(
    name = 'first_stream_table'
)

def first_stream_table():

    df = spark.readStream.table('declarative_pipeline.practice.orders')
    return df


# create materialized view

@dlt.table(
    name = 'first_mat_view'
)

def first_mat_view():

    df = spark.read.table('declarative_pipeline.practice.orders')
    return df 




#create view

@dlt.view(
    name = 'first_view_batch'
)

def first_view_batch():

    df = spark.read.table('declarative_pipeline.practice.orders')
    return df



#create streaming view

@dlt.view(
    name = 'first_view_stream'
)

def first_view_stream():

    df = spark.readStream.table('declarative_pipeline.practice.orders')
    return df





'''