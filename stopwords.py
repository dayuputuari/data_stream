import pandas as pd

stopwords = [x[0] for x in pd.read_csv('./stopwordbahasa.csv').values]
