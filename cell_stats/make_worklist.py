import os
import pandas as pd
import sys

pqsrc = "../cell_stats/results_healpy"

is_folder = os.path.isdir(pqsrc)

if not is_folder:
    print(f"{pqsrc} is not directory")
    sys.exit(1)
else:
    is_writable = os.access(pqsrc, os.W_OK)
    if not is_writable:
        print(f"{pqsrc} is not writable")
        sys.exit(1)

healpy_files = []
for a, b, c in os.walk(pqsrc):
    healpy_files = list(filter(lambda s: s.startswith('healpy_') and '.parquet' in s, c))

file_df = pd.DataFrame(healpy_files)
file_df = file_df.rename(columns={0: 'infile'})
file_df['outfile'] = file_df['infile'].apply(lambda s: s.replace('.parquet', '_step2.parquet'))

outcsv = os.path.join(pqsrc, "worklist.csv")

file_df.to_csv(outcsv)
print(f"worklist csv at {outcsv}")

file_df.loc[0, 'infile']

items = len(file_df.index)
print(f"{items} number of files")
