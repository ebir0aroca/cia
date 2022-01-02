import seaborn as sns
import pandas as pd
import numpy as np

def missing_data_heatmap(df):
  '''
    Technique #1: Missing Data Heatmap
    When there is a smaller number of features, we can visualize the missing data via heatmap.
    https://towardsdatascience.com/data-cleaning-in-python-the-ultimate-guide-2020-c63b88bf0a0d
  '''
  cols = df.columns[:30] # first 30 columns
  colours = ['#000099', '#ffff00'] # specify the colours - yellow is missing. blue is not missing.
  sns.heatmap(df[cols].isnull(), cmap=sns.color_palette(colours))

def missing_data_perc_list(df):
  '''
      Technique #2: Missing Data Percentage List
      This produces a list below showing the percentage of missing values for each of the features.
  '''
  # if it's a larger dataset and the visualization takes too long can do this.
  # % of missing.
  for col in df.columns:
      pct_missing = np.mean(df[col].isnull())
      print('{} - {}%'.format(col, round(pct_missing*100)))
      

def show_col_hist(df, col_name, bins):
  '''
  When the feature is numeric, we can use a histogram and box plot to detect outliers.
  '''
  df[col_name].hist(bins=bins)
