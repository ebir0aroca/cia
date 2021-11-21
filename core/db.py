try:
  import pandas as pd
  import numpy as np
  import ast

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    input('press enter to exit....')
    exit()


# DATA MODEL
HIST_DATAFRAME_COLUMNS = {
                      "title":"string",
                      "sku":"string",
                      "product_type1":"string",
                      "product_type2":"string",
                      "isStoreBrand":"bool",
                      "product_url":"string",		
                      "product_pos_in_page":"int",
                      "product_page":"int",
                      "source_category_url":"string",	
                      "confs":"string",
                      "isConfigurable":"bool",
                      "hasVariants":"bool",	
                      "reviews_rating":"float",
                      "reviews_count":"float",
                     	"currency":"string",	
                      "price":"float",
                     	"creation_date":"string", #later change to date	
                      "seller":"string", #dropshipping seller
                      "EAN":"string",	
                      "description":"string",		
                      "isAvailableInShop":"bool",
                     	"isAvailableOnline":"bool",	
                      "onlineShippingCost":"float",
                     	"onlineShippingLeadtime":"string",	
                      "clickCollectLeadtime":"string",	
                     #datasheet_urls	
                     #"logoUrl":"string",	
                      "clickAndCollectState":"string",	
                      "clickAndCollectAvailableQuantity":"float",
                     #"metaKeywords":"string",		
                      "deliveryTimeText":"string",
                      "specialPrice":"float",
                      "isSpecialPrice":"bool",
                     #nearby_markets	
                      "brand":"string",	
                      "img_urls":"string",
                     #breadcrumbs	
                      "reviews":"string",	
                      "isHighlight":"bool", #custom.highlight	
                     #custom.multipleVariantsText	
                     #custom.warranty	
                     #specs.Tip 
                     #produs	
                      "scrap__guid":"string",
                      "scrap__maincategory_url":"string",	
                      "scrap__spider_country":"string",
                      "scrap__spider_date_start":"string",
                      "scrap__spider_date_end":"string",
                      "scrap__spider_date":"string",
                      "scrap__spider_marketplace":"string",	
                      "scrap__spider_name":"string",	
                      "scrap__spider_version":"string",	
                      "scrap__title":"string",
                      "hist_status":"string"
                  }

    
def load_database(db_file_path, compression="infer"):
  print('----------------------------')
  print(f'Loading database file:')
  print(f'    > file: {db_file_path}')
  
  db_df = pd.read_csv(db_file_path)
  return db_df

def create_empty_database(db_file_path):
  df = pd.DataFrame(columns = HIST_DATAFRAME_COLUMNS)
  df.to_csv(db_file_path)
  
 
def append_to_database(df, db_file_path):
  database = load_database(db_file_path)
  database = database.append(df, ignore_index=True)
  database.to_csv(db_file_path)

def save_database(database, db_file_path):
  database.to_csv(db_file_path)

  

def hist_diff(df, date0, date1):
  '''
    returns series of union, intersect and not common 
  '''
  df0 = df[df['scrap__spider_date']==date0]
  df1 = df[df['scrap__spider_date']==date1]

  idx0 = pd.Index(df0['sku'])
  idx1 = pd.Index(df1['sku'])

  # union of the series
  idx_union = pd.Series(np.union1d(idx0, idx1))
    
  # intersection of the series
  idx_intersect = pd.Series(np.intersect1d(idx0, idx1))
    
  # uncommon elements in both the series 
  idx_notcommon = idx_union[~idx_union.isin(idx_intersect)]
  
  idx_left = pd.Index(df0['sku'])
  idx_right = pd.Index(df1['sku'])

  # intersection of the series
  idx_intersect_left = pd.Series(np.intersect1d(idx_notcommon, idx_left))
  idx_intersect_right = pd.Series(np.intersect1d(idx_notcommon, idx_right))

  '''
    union :         suma de todos (sin duplicados)
    intersect:      intersección solo
    not_common:     todo fuera de la intersección
    intersect_left: elementos izq sin communes
  '''
  return idx_union, idx_intersect, idx_notcommon, idx_intersect_left, idx_intersect_right

def save_hist_diffs(df, db_file_path):
  all_dates = df['scrap__spider_date'].unique()
  date_ranges =[{}]
  df['hist_status'] = "ACTIVE"

  for i in range(len(all_dates)):
    if(i<len(all_dates)-1): 
      date_ranges[i]['start_date'] =all_dates[i]
      date_ranges[i]['end_date'] =all_dates[i+1]

  for date_range in date_ranges:
    idx_union, idx_intersect, idx_notcommon, idx_intersect_left, idx_intersect_right = DB.hist_diff(db, date_range['start_date'], date_range['end_date'])
                
    db0 = db[db['scrap__spider_date']==date_range['start_date']]
    db1 = db[db['scrap__spider_date']==date_range['end_date']]

    df_removed = db0[db0['sku'].isin(idx_intersect_left)]
    df_active = db1[db1['sku'].isin(idx_intersect)]
    df_new = db1[db1['sku'].isin(idx_intersect_right)]

    df_hist_diffs = pd.DataFrame() 
    df_removed['hist_status'] = "REMOVED"
    df_active['hist_status'] = "ACTIVE"
    df_new['hist_status'] = "NEW"
    df_hist_diffs = df_hist_diffs.append(df_removed)
    df_hist_diffs = df_hist_diffs.append(df_active)
    df_hist_diffs = df_hist_diffs.append(df_new)
  
  df_hist_diffs = df_hist_diffs.append(db[db['scrap__spider_date']==all_dates[0]])
  df_hist_diffs.to_csv(db_file_path)
    
    
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

