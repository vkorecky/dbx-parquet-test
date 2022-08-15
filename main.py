import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np

## Read snappy parquet
table = pq.read_table('sample.parquet')
data_frame = table.to_pandas()
print(data_frame)

## Writing snappy parquet
data_frame = pd.DataFrame({'one': [-1, np.nan, 2.5],
                           'two': ['foo', 'bar', 'baz'],
                           'three': [True, False, True]},
                          index=list('abc'))
table = pa.Table.from_pandas(data_frame)
pq.write_to_dataset(table, root_path='example.parquet',
                    partition_cols=['one', 'two', 'three'])
