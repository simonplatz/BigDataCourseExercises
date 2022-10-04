import encodings
from tkinter.tix import COLUMN
from hdfs import InsecureClient
from collections import Counter
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa

client = InsecureClient('http://namenode:9870', user='root')

# Make wordcount reachable outside of the with-statement
wordcount = None

with client.read('/alice-in-wonderland.txt', encoding='utf-8') as reader:
    wordcount = Counter(reader.read().split()).most_common(10)

dataframe = pd.DataFrame(wordcount, columns=['word', 'columns'])


# To-Do: Save the wordcount in a Parquet file and read it again!
table = pa.Table.from_pandas(dataframe)

client.write('/wordcount.parquet', encoding='utf-8')

readTable = pq.read_table('/wordcount.parquet')
