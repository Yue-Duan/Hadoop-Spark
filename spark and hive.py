# set up hive + spark
sudo cp /etc/hive/conf.dist/hive-site.xml /usr/lib/spark/conf/
PYSPARK_DRIVER_PYTHON=ipython pyspark
# test
sqlCtx.createDataFrame([("somekey", 1)])


# load the package to load csv
PYSPARK_DRIVER_PYTHON=ipython pyspark --packages com.databricks:spark-csv_2.10:1.3.0

# load yelp data
yelp_df = sqlCtx.load(source="com.databricks.spark.csv", header='true', inferSchema='true', path='file:///usr/lib/hue/apps/search/examples/collections/solr_configs_yelp_demo/index_data.csv')

# filtering
yelp_df.filter(yelp_df.useful >= 1).count()
yelp_df.filter('useful >= 1').count()

# select - subset of data createDataFrame
yelp_df.select("useful")
yelp_df.select("useful").agg({"useful" : "max"}).collect()

# scale
yelp_df.select("id", yelp_df.useful/28*100).show(5)
yelp_df.select("id", (yelp_df.useful/28*100).cast('int')).show(5)

# rename
yelp_df.select("id", (yelp_df.useful/28*100).cast('int').alias('useful_pct')).show(5)

# order
useful_perc_data = yelp_df.select(yelp_df["id"].alias('uid'), (yelp_df.useful/28*100).cast('int').alias('useful_pct')).orderBy(desc('useful_pct'))

# join + select
useful_perc_data.join(
	yelp_df,
	yelp_df.id == useful_perc_data.uid,
	"inner").select(useful_perc_data.uid, "useful_pct", "review_count").show(5)

# cache - after caching, second run became much faster
useful_perc_data.join(
	yelp_df,
	yelp_df.id == useful_perc_data.uid,
	"inner").cache().select(useful_perc_data.uid, "useful_pct", "review_count").show(5)

# logs
# set delimiter to windows line end
sc._jsc.hadoopConfiguration().set('textinputformat.record.delimiter','\r\n')

logs_df = sqlCtx.load(
source="com.databricks.spark.csv",
header = 'true', inferSchema = 'true',
path =
'file:///usr/lib/hue/apps/search/examples/collections/solr_configs_log_analytics_demo/index_data.csv')
logs_df.count()

# count by different code type
logs_df.groupBy("code").count().show()
# rank by counts
from pyspark.sql.functions import asc, desc
logs_df.groupBy('code').count().orderBy(desc('count')).show()

# calculate average size of different code
logs_df.groupBy("code").avg("bytes").show()
# more calculation by code - average, min, max
import pyspark.sql.functions as F
logs_df.groupBy("code").agg(
logs_df.code,
F.avg(logs_df.bytes),
F.min(logs_df.bytes),
F.max(logs_df.bytes)
).show()


# homework
# 1
yelp_df.select("cool").agg({"cool" : "mean"}).collect()
# 2
import pyspark.sql.functions as F
yelp_df.filter('review_count >= 10').groupBy("stars").agg(yelp_df.stars, F.avg(yelp_df.cool)).show()
# 3
yelp_df.filter((yelp_df.review_count >= 10) & (yelp_df.open == 'True')).groupBy("stars").agg(yelp_df.stars, F.avg(yelp_df.cool)).show()
# 4
from pyspark.sql.functions import asc, desc
yelp_df.filter((yelp_df.review_count >= 10) & (yelp_df.open == 'True')).groupBy('state').count().orderBy(desc('count')).show()
# 5
yelp_df.groupBy('business_id').count().orderBy(desc('count')).show()


# test
sqlCtx.sql("SELECT customer_fname, customer_lname FROM customers").limit(2).collect()

# homework
# 1 - 1558
orders_df = sqlCtx.sql("SELECT * FROM orders")
orders_df.show(2)

sqlCtx.sql("""select order_status,
count(order_id) as count from orders 
group by order_status
order by count desc
limit 10"""
).show()

# 2 -2840
order_items_df = sqlCtx.sql("SELECT * FROM order_items")
order_items_df.show(2)
order_items_df.printSchema()

sqlCtx.sql("""select order_item_order_id,
sum(order_item_subtotal) as total from order_items 
group by order_item_order_id
order by total desc
limit 10"""
).show()

# 3 - 133
order_items = sqlCtx.createDataFrame(order_items_df.rdd, order_items_df.schema)
orders = sqlCtx.createDataFrame(orders_df.rdd, orders_df.schema)

sqlCtx.sql("""select o.order_status, 
avg(oi.order_item_product_price) as average 
from orders as o inner join order_items as oi 
on o.order_id = oi.order_item_order_id 
group by o.order_status"""
).cache().show()

# 4 - 6585
sqlCtx.sql("""select o.order_customer_id, 
sum(oi.order_item_product_price) as max 
from orders as o inner join order_items as oi 
on o.order_id = oi.order_item_order_id 
where o.order_status='COMPLETE' 
group by o.order_customer_id 
order by max desc"""
).cache().show()

# where o.order_status='COMPLETE' 

# 5 - 2779
sqlCtx.sql("""select o.order_id, 
sum(oi.order_item_subtotal) as total 
from orders as o inner join order_items as oi 
on o.order_id = oi.order_item_order_id 
where o.order_status != 'COMPLETE' 
group by o.order_id 
order by total desc"""
).cache().show()