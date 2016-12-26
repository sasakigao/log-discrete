# gamease-discrete
Discrete the raw datum into personal one and team one, with the former one in player grade form.
# Key skills used
* **Curry function**: Divide the params of the split function into 2 parts, with event code and its 
index to decide one func, seqs to decide the other one.
* **Generic**: Print any type objects.
* **combineByKey**: Better avoid groupByKey. And most of the time, `groupByKey` can be replaced with
`combineByKey` to compose a seq, eg., under each key.
* **Option**: Almost any data processing steps return Option as a container. That way I hardly use 
try/catch block to process exception. Holly scala!

# Store consideration
In order to keep a friendly store format and reduce the work of parsing, 
I considered 3 ways to solve this.
* **text**: Text file dosen't need a lot of work on saving. But correspondingly reading from HDFS
costs relatively large efforts to parse the string, due to its raw object form.
* **SequenceFile**: Hadoop provides this format for serialization. Parsing is saved under this 
situation but the files persisted are unfriendly to read.
* **JSON**: This one is finally chosen, for the natural support from SparkSQL. So I spare some time
on a rdd-to-dataframe transformation. The loading process later on also needs to do a 
dataframe-to-rdd work. 
I prefer to draw logics on RDD, rather than DateFrame as a SQL interface.
