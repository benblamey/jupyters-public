import h5py


# Sample file from http://millionsongdataset.com/pages/getting-dataset/
songs = h5py.File('/foo/bar/wibble/million-song-TRAXLZU12903D05F94.h5') # type = HDF5 file.

print(f'keys are {list(songs.keys())}')

group_analysis = songs['analysis'] # type: HDF5 group.

group_analysis.attrs # AttributeManager -- these are like a small metadata dictionary attached anywhere in the tree.
print(list(group_analysis.attrs.items()))

# lets drill down the hierarchy...

bars_start = songs['analysis']['bars_start'] # Dataset.

# this dataset has a single column. Lets print all the values.
print(bars_start.value) # an ndarray (from numpy)

# Lets look at a more complex table elsewhere in the tree...

songs = songs['analysis']['songs'] # Dataset.

# This dataset has rows and columns. Lets get the first row...
song_row = songs.value[0]

# print the column names
print(song_row.dtype.names)

# get a value from that 1 row by column name.
print(song_row['duration'])

# lets get all the values for a column (from all rows)...
print(songs['duration'])


# Note: these numpy and h5py data types will likely not survive or behave strangely when passed through Java.
# You may need to extract the information you need, and convert it to regular python lists to 'survive the trip'.






