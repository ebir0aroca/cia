try:
  import pandas as pd  
  import numpy as np     
  import datetime
  
  #from IPython.display import HTML
  
  import seaborn as sns
  import matplotlib.pyplot as plt
  import missingno as msno
  import bokeh.io
  bokeh.io.output_notebook()

  import bokeh.layouts
  import bokeh.plotting  

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    input('press enter to exit....')
    exit()
    
    


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
      

def show_col_hist(df, col_name):
  '''
  When the feature is numeric, we can use a histogram and box plot to detect outliers.
  '''
  df[col_name].hist(bins=100)




def info(df, title):
  print(f"-------------{title}-------------") 
  start_datetime = datetime.datetime.strptime(df.iloc[-1]['scrap__spider_date_start'], '%Y-%m-%d %H:%M:%S')
  finish_datetime = datetime.datetime.strptime(df.iloc[-1]['scrap__spider_date_end'], '%Y-%m-%d %H:%M:%S')
  currency = df.iloc[-1]['currency']
  delta = finish_datetime - start_datetime
  print(f'Spider: {df.iloc[-1]["scrap__spider_marketplace"]}.{df.iloc[-1]["scrap__spider_country"]}, version {df.iloc[-1]["scrap__spider_version"]}')
  print(f'    Delta:  {delta}, ')
  print(f'    Start:  {start_datetime}')
  print(f'    Finish:  {finish_datetime}')
  print(f'    ')
  print(f'    CURRENCY:  {currency}')

  

  print("")
  print("Quantity of SKU's:  " + str((df['sku'].nunique())) )
  print("")
  
  print("-------------PRICES-------------")

  print("Min            = " + str(df['price'].min()) )
  print("Quantile [0,25]= " + str(df['price'].quantile(0.25)) )
  print("Quantile [0,50]= " + str(df['price'].quantile(0.5)) )
  print("Mean           = " + str(df['price'].mean()) )
  print("Quantile [0,75]= " + str(df['price'].quantile(0.75)) )
  print("Max            = " + str(df['price'].max()))
  print("")
  
  print("------------- SUMMARY OF MIN/MAX PRICE PRODUCT-------------")
  
  print("Cheapest:          " + df.loc[df['price'].idxmin(),:]['title'])
  print("                   " + df.loc[df['price'].idxmin(),:]['product_url'])
  print("Most expensive:    " + df.loc[df['price'].idxmax(),:]['title'])  
  print("                   " + df.loc[df['price'].idxmax(),:]['product_url'])
  print("")
  
  print("-------------REVIEWS-------------")
  print("Min            = " + str(round(df[df["reviews_count"]>0]['reviews_rating'].min(), 2)) )
  print("Quantile [0,25]= " + str(round(df[df["reviews_count"]>0]['reviews_rating'].quantile(0.25), 2)) )
  print("Quantile [0,50]= " + str(round(df[df["reviews_count"]>0]['reviews_rating'].quantile(0.5), 2)) )
  print("Mean           = " + str(round(df[df["reviews_count"]>0]['reviews_rating'].mean(), 2)) )
  print("Quantile [0,75]= " + str(round(df[df["reviews_count"]>0]['reviews_rating'].quantile(0.75), 2)) )
  print("Max            = " + str(round(df[df["reviews_count"]>0]['reviews_rating'].max(), 2)) )  
  print("")
  
  
  print("-------------SUMMARY OF REVIEWED PRODUCT-------------")  
  print("Worstly  evaluated :  " + df.loc[df[df["reviews_count"]>0]['reviews_rating'].idxmin(),:]['title'])
  print("                   " + df.loc[df[df["reviews_count"]>0]['reviews_rating'].idxmin(),:]['product_url'])
  print("Bestly   evaluated :   " + df.loc[df[df["reviews_count"]>0]['reviews_rating'].idxmax(),:]['title'])  
  print("                   " + df.loc[df[df["reviews_count"]>0]['reviews_rating'].idxmax(),:]['product_url'])
  print("Mostly   evaluated :   " + df.loc[df[df["reviews_count"]>0]['reviews_rating'].idxmax(),:]['title'])  
  print("                   " + df.loc[df[df["reviews_count"]>0]['reviews_rating'].idxmax(),:]['product_url'])
  
  
  print("")
        

def tablesummary_by_sku(dataframe, groupby):
  _df = dataframe.groupby([groupby]).agg({'sku': 'count'})
  _df1=_df.groupby(groupby).agg({'sku':['sum',lambda x: x.sum()*100/ _df['sku'].sum(),'mean']})
  _df1.columns = _df1.columns.map('_'.join).str.replace('<lambda_0>','%')
  return _df1[['sku_sum', 'sku_%']].sort_values(by="sku_%",  ascending=False)
        
  
def peoples_best_choice(dataframe, product_reviews_qty, product_reviews_rating):
  """
    More than the indicated product_reviews_qty and ratings above.
  """
  dataframe=dataframe.query(f'product_reviews_qty>={product_reviews_qty} and product_reviews_rating>={product_reviews_rating}')
  return dataframe[['sku', 'title', 'img', 'product_url', 'product_reviews_rating', 'product_reviews_qty', 'product_brand']]
  

def peoples_worst_choice(dataframe, product_reviews_qty, product_reviews_rating):
  """
    More than the indicated product_reviews_qty and product_reviews_rating below.
  """
  dataframe=dataframe.query(f'product_reviews_qty>={product_reviews_qty} and product_reviews_rating<={product_reviews_rating}')
  return dataframe[['sku', 'title', 'img', 'product_url', 'product_reviews_rating', 'product_reviews_qty', 'product_brand']]
  
  
  
def plot_scatter_comparison(dataframe, value_name, group_name):
  plt.figure(figsize=(16, 4))
  plt.xlabel(xlabel=value_name)
  plt.ylabel(ylabel=group_name)
  plt.scatter(x=dataframe[value_name],y=dataframe[group_name], s = 50, c = 'b')
  plt.show()
  
  

def groupby(dataframe, classification="subcategory1", count_label="count"):
  results = dataframe.groupby(by=classification)[classification].agg([count_label]).reset_index(drop=False)
  print(classification + ": " + str(results[classification].count()))
  print("Totals: " + str(results[count_label].sum()))
  return results

  
def image_formatter(im):
  return f'<img src="{im}" height="100">'   

def link_formatter(link):
  return f'<a href="{link}">View Page.</a>'



def func(pct, allvals):
    absolute = int(pct/100.*np.sum(allvals))
    return "{:.1f}%\n({:d} refs)".format(pct, absolute)

def plot_pie(dataframe, classification, spider, count_label="count"):
  fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(aspect="equal"))


  wedges, texts, autotexts = ax.pie(dataframe[count_label], autopct=lambda pct: func(pct, dataframe[count_label]), 
                                    textprops=dict(color="w"))

  ax.legend(wedges, dataframe[classification],
            title=classification,
            loc="center left",
            bbox_to_anchor=(1, 0, 0.5, 1))

  plt.setp(autotexts, size=14, weight="bold")


  ax.set_title(classification + ' (Source: '+ spider +')')
  plt.show()


def plot_hist(dataframe, classification, plot_start, plot_stop, plot_step, min_occurrences=0, show_occurrences=False):
  fig, ax = plt.subplots(figsize=(16, 5))
  dt = plt.hist(x=dataframe[classification], bins=np.arange(plot_start, plot_stop, plot_step), label="(Bin #/"+classification+" value/Occurrences)")
  hist_cols = dt[0]

  for i, v in enumerate(hist_cols):
    #Filtrar por cantidad de ocurrencias
    if (v>min_occurrences): 
      if(show_occurrences):
        ax.text(i*plot_step , v+0.2, str('#{:.0f}'.format(i)) + "/"+str(plot_start+plot_step*i)+"/" + str('{:.0f}'.format(v)), color="black", ha='center', va='bottom')          
        
  plt.legend()
  plt.xticks(rotation=90)

  plt.show()


  
def plot_compare2metrics(dataframe, x_col_name, y1_col_name, y2_col_name ):
  x = dataframe[x_col_name]
  y = dataframe[y1_col_name]
  y_ = dataframe[y2_col_name]
  TOOLS = "pan,wheel_zoom,box_zoom,reset,save,box_select,lasso_select"

  # create a plot
  s1 = bokeh.plotting.figure(tools=TOOLS, width=500, plot_height=350, title=y1_col_name + " vs. " + x_col_name)
  s1.circle(x, y, size=10, color="navy", alpha=0.5)

  # new plot with a shared range
  s2 = bokeh.plotting.figure(tools=TOOLS, width=500, height=350, x_range=s1.x_range, y_range=s1.y_range, title=y2_col_name + " vs. " + x_col_name )
  s2.square(x, y_, size=10, color="firebrick", alpha=0.5)

  # put subplots into a griplot (notice the allowance of a toolbar)
  p = bokeh.layouts.gridplot([[s1, s2]])

  bokeh.plotting.show(p)
  
  
  
from pandas.plotting import lag_plot

def show_time_series_lag_plot(dataframe):
  i=0
  f2,ax  = plt.subplots(1, len(dataframe.columns), figsize=(15, 5))
    
  for kw in dataframe:
    f2.tight_layout()

    lag_plot(dataframe[kw], ax=ax[i])
    ax[i].set_title(kw);
    i+=1
    
  plt.show()
