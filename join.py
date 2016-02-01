# PYSPARK_DRIVER_PYTHON=ipython pyspark

def split_fileA(line):
    # split the input line in word and count on the comma
    key_value  = line.split(",")
    word = key_value[0].strip()
    # turn the count to an integer  
    count = int(key_value[1].strip())
    return (word, count)

fileA_data = fileA.map(split_fileA)
fileA_data.collect()

def split_fileB(line):
    # split the input line into word, date and count_string
    date = line.split(" ")[0]
    word, count_string = line.split(" ")[1].split(",")
    return (word, date + " " + count_string) 

fileB_joined_fileA = fileB_data.join(fileA_data)