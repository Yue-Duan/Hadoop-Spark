# $ hdfs dfs -ls input_join2
# PYSPARK_DRIVER_PYTHON=ipython pyspark

show_views_file = sc.textFile("input_join2/join2_gennum?.txt")
show_views_file.take(2)

# parse views
def split_show_views(line):
    show, views = line.strip().split(",")
    return (show, views)

show_views = show_views_file.map(split_show_views)

# parse channels
show_channel_file = sc.textFile("input_join2/join2_genchan?.txt")

def split_show_channel(line):
    show, channel = line.strip().split(",")
    return (show, channel)

show_channel = show_channel_file.map(split_show_channel)

# joint the two
joined_dataset = show_views.join(show_channel)

# extract channel
def extract_channel_views(show_views_channel): 
    pair = show_views_channel[1]
    channel = pair[1]
    views = int(pair[0])
    return (channel, views)

channel_views = joined_dataset.map(extract_channel_views)

# sum over all channels
def sum_for_channel(a, b):
    return a+b

channel_views.reduceByKey(sum_for_channel).collect()