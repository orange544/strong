import time
print('t0', flush=True)
import pandas as pd
print('pandas ok', flush=True)
df1=pd.read_csv(r'program/database/valentine_experiments/data/authors1.csv')
df2=pd.read_csv(r'program/database/valentine_experiments/data/authors2.csv')
print('csv ok', len(df1), len(df2), flush=True)
import valentine
print('valentine imported', flush=True)
from valentine.algorithms import SimilarityFlooding
print('sf imported', flush=True)
from valentine import valentine_match
print('match fn imported', flush=True)
m=SimilarityFlooding()
print('matcher built', flush=True)
r=valentine_match(df1,df2,m)
print('matched', len(r), flush=True)
