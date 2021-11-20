try:
  import pandas as pd

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
