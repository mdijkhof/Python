import numpy as np
import pandas as pd

# series with a default integer index
s = pd.Series ([1,3,5,np.nan,6,8])

# date range
dates = pd.date_range("20220101", periods=20,freq='D')

# table with random values in 4 different columns and index specified in dates above
df = pd.DataFrame(np.random.randn(20,4), index=dates, columns = list("ABCD"))

# table with different data types in different columns
df2 = pd.DataFrame(
    {
        "A": 1.0,
        "B": pd.Timestamp("20130102"),
        "C": pd.Series(1, index=list(range(4)), dtype="float32"),
        "D": np.array([4] * 4, dtype="int32"),
        "E": pd.Categorical(["test", "train", "test", "train"]),
        "F": "foo",
        "G": pd.date_range("20220101",periods=4,freq='D')
    }
)
# print(df2.dtypes)

# view top or bottom rows of table
# print(df.head(3))
# print(df.tail(2))

# show the index for the table (as a list)
# print(df.index)

# joining tables
left = pd.DataFrame({"key": ["foo", "bar"], "lval": [1, 2]})
right = pd.DataFrame({"key": ["foo", "bar"], "rval": [4, 5]})

print(left)
print(right)

print(pd.merge(left, right, on="key"))

 

    