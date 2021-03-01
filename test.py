import pandas as pd
import gcsfs

fs = gcsfs.GCSFileSystem(project='my-project')
with fs.open('bucket/path.csv') as f:
    df = pd.read_csv(f)
