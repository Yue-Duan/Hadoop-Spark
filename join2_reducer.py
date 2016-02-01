#!/usr/bin/env python
import sys

line_cnt           = 0  #count input lines
total_count = 0
abc_found          = False
prev_word          = "  "                #initialize previous word  to blank string

# for debug
# show_to_output = [] #an empty list to hold show name for ABC
# count_to_output = []

for line in sys.stdin:              # key, value
    line       = line.strip()       #strip out carriage return
    key_value  = line.split('\t')   #split line, into key and value, returns a list
    line_cnt   = line_cnt+1     
 
    curr_word  = key_value[0]         #key is first item in list, indexed by 0
    value_in   = key_value[1]         #value is 2nd item

    if curr_word != prev_word:
 
	# now write out the join result, but not for the first line input
        if line_cnt>1 :
            # count_to_output.append(total_count)
            print('{0} {1}'.format(prev_word, total_count))
        total_count = 0
        abc_found = False
        prev_word = curr_word  #set up previous word for the next set of input lines

    # check if it's ABC show
    if value_in == 'ABC': 
        abc_found = True
        
    else:
        total_count += int(value_in)  # if the value field was count then calculate total

# ---------------------------------------------------------------
#now write out the LAST join result
# ---------------------------------------------------------------
print('{0} {1}'.format(prev_word, total_count))