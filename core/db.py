try:
  import pandas as pd
  import ast

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    input('press enter to exit....')
    exit()

    
def load_database(db_file_path, compression="infer"):
  print('----------------------------')
  print(f'Loading database file:')
  print(f'    > file: {db_file_path}')
  
  db_df = pd.read_csv(db_file_path)
  return db_df


def flatten_product_reviews(df): 
  #'Id' retrieved
  columns = ['sku', 'title', 'brand',
            'category', 'price'
            ]
  
  data_dict = df.to_dict('records')

  df = pd.json_normalize(data_dict, "reviews",  columns, errors='ignore' )
  return df


def flatten_product_reviews(df):
  cols = ['sku', 'title', 'category', 'brand', 'isStoreBrand', 'price', 'reviews', 'reviews_count', 'reviews_rating', 'scrap__spider_marketplace', 'scrap__spider_country', 'product_url']
  df1 = df[df['reviews_count'] > 0][cols]

  for i, row in df1.iterrows():
      df1.at[i, 'reviews'] = ast.literal_eval(str(df1.at[i, 'reviews']))
  di = df1.to_dict('records')
  cols.remove('reviews')
  df2 = pd.json_normalize(di, record_path=['reviews'], meta=cols)
  return df2

