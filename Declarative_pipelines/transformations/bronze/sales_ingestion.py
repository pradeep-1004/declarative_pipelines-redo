import dlt

#expectations
sales_rules = {
    'rule_1' : 'sales_id IS NOT NULL'
}


#create streaming table
dlt.create_streaming_table(
    name = 'sales_stg',
    expect_all_or_drop=sales_rules
)

#create flow east
@dlt.append_flow(target='sales_stg')

def east_sales():
    df = spark.readStream.table('parent_cata.dlt_source.sales_east')
    return df


#create flow west
@dlt.append_flow(target='sales_stg')

def west_sales():
    df = spark.readStream.table('parent_cata.dlt_source.sales_west')
    return df

