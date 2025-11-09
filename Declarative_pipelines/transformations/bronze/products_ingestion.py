import dlt 

products_rules = {
    'rule_1' : 'product_id IS NOT NULL',
    'rule_2' : 'price IS NOT NULL'
 }

@dlt.table(
    name = 'products_stg'
)
@dlt.expect_all_or_drop(products_rules)

def products_stg():

    df = spark.readStream.table('parent_cata.dlt_source.products')
    return df