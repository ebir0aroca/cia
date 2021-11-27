try:
  import pandas as pd
  import numpy as np
  import regex as re
  import os
  import os.path
  from os import walk
  import sys
  from zipfile import ZipFile 
  import json
  import ast
  from datetime import datetime

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    exit()



# JSON NORMALIZE
SCRAP_META = [['scrap_meta', 'guid'], ['scrap_meta', 'date_start'],
            ['scrap_meta', 'maincategory_url'], ['scrap_meta', 'spider_country'],
            ['scrap_meta', 'spider_date_end'], ['scrap_meta', 'spider_marketplace'],
            ['scrap_meta', 'spider_name'], ['scrap_meta', 'spider_version'],
            ['scrap_meta', 'title'], ['scrap_meta', 'spider_date_start']]


def dataframe_info(df, title):
  print('----------------------------')
  print(f'Report  {title}')
  scrap_processes_count = df.groupby(by=["scrap_meta.guid"])['scrap_meta.guid'].unique().count()
  print(f'    > Scrap processes count:  {scrap_processes_count}')
  print(f"    > marketplaces:       {df['scrap_meta.spider_marketplace'].unique()}")
  print(f"    > countries:          {df['scrap_meta.spider_country'].unique()}")
  print(f"    > total SKUs:         {df['sku'].count()}")
  print(f"    > scrap dates:        {df['scrap_meta.spider_date'].unique()}")
  print(f"    > categories:         {df['category'].unique()}")
  print(f"    > brands:         {df['brand'].unique()}")


  print('----------------------------')
  df['scrap_meta.spider_date_end'] = pd.to_datetime(df['scrap_meta.spider_date_end'])
  df['scrap_meta.spider_date_start'] = pd.to_datetime(df['scrap_meta.spider_date_start'])
  df['scrap_meta.spider_time_elapsed'] = (df['scrap_meta.spider_date_end']-df['scrap_meta.spider_date_start']).dt.total_seconds()

  print(" MEAN TIME [seconds] FOR SCRAPPING BY MARKETPLACE, COUNTRY AND DATE")
  table = df.groupby(by=['scrap_meta.spider_date', 'scrap_meta.spider_marketplace', 'scrap_meta.spider_country']).mean()[['scrap_meta.spider_time_elapsed']]
  print(table)

  print('----------------------------')
  table = df.groupby(by=['scrap_meta.spider_marketplace', 'scrap_meta.spider_country','scrap_meta.spider_date']).count()[['sku']]
  print(" SKUs FOR EACH SCRAPPING BY MARKETPLACE, COUNTRY AND DATE")
  print(table)




def load_scrap(scrap_file_path, compression="infer"):
  with open(scrap_file_path, 'r', encoding='utf-8') as json_file:
    data = json.load(json_file)  
 
  db_df =  pd.DataFrame()
  db_df = pd.json_normalize(
              data, record_path=['scraped_products_data'], 
              meta=SCRAP_META
            )
  return db_df



def rename_dataframe_cols(rename_cols, df):
  for from_name, to_name in rename_cols.items():
    df.rename(columns={from_name:to_name}, inplace=True)
  return df

def drop_dataframe_cols(rename_cols, df):
  for from_name, to_name in rename_cols.items():
    df = df.drop(columns={to_name})
  return df


def rename_field_value(field_conf, field_name, df):
  result_df = pd.DataFrame()
  for field_value, filter_criterias in field_conf.items():
    conditions = []
    joined_condition = False
    joined_df = pd.DataFrame()
    for i in range(len(filter_criterias)):
      filter_name = next(iter(filter_criterias[i].keys()))
      filter_value = next(iter(filter_criterias[i].values()))
      conditions.append(df[filter_name].str.contains(filter_value)==True)
      if(i>0):
        joined_condition = joined_condition & conditions[i]
      else:
        joined_condition = conditions[i]
      joined_df = df[joined_condition]

    joined_df.loc[:, field_name] = field_value
    result_df = result_df.append(joined_df)
  return result_df


def get_transformer_fields_conf(transformer, marketplace_name, country):
    countries_transf = pd.DataFrame.from_dict(next(iter(transformer[transformer['marketplace'] == marketplace_name]['countries'])))
    fields_transf = countries_transf[countries_transf['country'] == country]['fields']
    return fields_transf

def transform(dataframe, transformer):
  #change scrap_meta col names
  curr_marketplace_name, curr_marketplace_country= dataframe.tail(1)["scrap_meta.spider_marketplace"].values[0], dataframe.tail(1)["scrap_meta.spider_country"].values[0]

  #select specs cols & clean
  if(curr_marketplace_name=="hornbach"):
      print(curr_marketplace_name + "." + curr_marketplace_country)
      df_0 = dataframe
      #select custom cols & clean
      df_0.rename(columns={"custom.highlight":"isHighlight"}, inplace=True)
      df_0['isHighlight'].replace(to_replace=['TOP'], value=True, inplace=True)
      df_0['isHighlight'].replace(to_replace=[None], value=False, inplace=True)
      df_0['specialPrice']=df_0['specialPrice'].replace(np.nan, 0)
      df_0['onlineShippingCost']=df_0['onlineShippingCost'].replace(np.nan, 0)
        
      #Store Brand
      df_0['isStoreBrand'] = False
      df_0['isStoreBrand']=df_0['brand'].isin(['basano'])

      #Product is configurable?
      df_0['isConfigurable'] = False

      #breadcrumbs, creation_date (last review date),
      for i, row in df_0.iterrows():
        if(type(df_0.at[i,'breadcrumbs'])==list):
            for j in range(len(df_0.at[i,'breadcrumbs'])):
              df_0.at[i,'category'+str(j)] = df_0.at[i,'breadcrumbs'][j]
        if(df_0.at[i,'reviews_count']>0):
            dates = []
            for each_date in df_0.at[i,'reviews']:
              dates.append(pd.to_datetime(each_date['review_date']))
            if(len(dates)>0):
              df_0.at[i,'creation_date'] = max(dates)

      df_0.at[i, 'img_url'] = df_0.at[i, 'img_urls'][0] #No need to transform ast.literal_eval(...)
      df_0.at[i,'isConfigurable']= (df_0.at[i,'confs'] != "[]")

      # Blanks, no NaN
      df_0['category'] = ""

      res_df = pd.DataFrame()
      # transform for each country
      for curr_country in df_0['scrap_meta.spider_country'].unique():
          df_1 = df_0.loc[(df_0['scrap_meta.spider_country'] == curr_country)]

          # Category, take the first element from the dict next(iter(...))
          transformer_fields_conf = get_transformer_fields_conf(transformer, curr_marketplace_name, curr_country)
          for field_conf in transformer_fields_conf:
              field_name = field_conf['name']
              assignation = field_conf['assignation']
              df_1 = rename_field_value(assignation, field_name, df_1)

          # Join all results,
          # drop rows without category assignation
          # and remove temporal cols created at the beginning.
          res_df = res_df.append(df_1)
          res_df.dropna(subset=["category"], inplace=True)

          # end of hornbach process
          # Blanks, no NaN
          df_0['seller'] = ''
          df_0['brand'] = df_0['brand'].fillna('')


  #covert dates
  res_df["scrap_meta.spider_date_start"] = pd.to_datetime(res_df["scrap_meta.spider_date_start"])
  res_df["scrap_meta.spider_date_end"] = pd.to_datetime(res_df["scrap_meta.spider_date_end"])
  res_df["scrap_meta.spider_date"] = pd.to_datetime(res_df["scrap_meta.spider_date_end"]).dt.date
  res_df["creation_date"] = pd.to_datetime(res_df["creation_date"]).dt.date


  #delete not used cols
  del_columns = [col for col in res_df if 'specs.' in col]
  res_df.drop(columns=del_columns, inplace=True)
  del_columns = [col for col in res_df if 'custom.' in col]
  res_df.drop(columns=del_columns, inplace=True)

  #res_df.drop(columns=['reviews'], inplace=True) #delete reviews

  # drop categories
  res_df.drop(columns=['category0', 'category1', 'category2', 'category3', 'category4', 'category5', 'category6'],
               inplace=True, errors='ignore')

  print(f'Cleaned Scrap file for marketplace: {curr_marketplace_name}.{curr_marketplace_country}')
  return res_df


def flatten_product_reviews(df):
  cols = ['sku', 'title', 'category', 'brand', 'isStoreBrand', 'price', 'reviews']
  df1 = df[df['reviews_count'] > 0][cols]

  for i, row in df1.iterrows():
      df1.at[i, 'reviews'] = ast.literal_eval(str(df1.at[i, 'reviews']))
  di = df1.to_dict('records')
  cols.remove('reviews')
  df2 = pd.json_normalize(di, record_path=['reviews'], meta=cols)
  return df2



def get_lastdate(database, date_format):
    all_dates = database['scrap_meta.spider_date']
    all_datetimes = [datetime.strptime(x, date_format) for x in all_dates]

    return max(all_datetimes)

def hist_diff(df, date0, date1):
    '''
      returns series of union, intersect and not common
    '''
    df0 = df[df['scrap_meta.spider_date'] == date0]
    df1 = df[df['scrap_meta.spider_date'] == date1]

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
    all_dates = df['scrap_meta.spider_date'].unique()
    date_ranges = [{}]
    df['hist_status'] = "ACTIVE"

    for i in range(len(all_dates)):
        if (i < len(all_dates) - 1):
            date_ranges[i]['start_date'] = all_dates[i]
            date_ranges[i]['end_date'] = all_dates[i + 1]

    for date_range in date_ranges:
        idx_union, idx_intersect, idx_notcommon, idx_intersect_left, idx_intersect_right = hist_diff(df, date_range[
            'start_date'], date_range['end_date'])

        df0 = df[df['scrap_meta.spider_date'] == date_range['start_date']]
        df1 = df[df['scrap_meta.spider_date'] == date_range['end_date']]

        df_removed = df0[df0['sku'].isin(idx_intersect_left)]
        df_active = df1[df1['sku'].isin(idx_intersect)]
        df_new = df1[df1['sku'].isin(idx_intersect_right)]

        df_hist_diffs = pd.DataFrame()
        df_removed['hist_status'] = "REMOVED"
        df_active['hist_status'] = "ACTIVE"
        df_new['hist_status'] = "NEW"
        df_hist_diffs = df_hist_diffs.append(df_removed)
        df_hist_diffs = df_hist_diffs.append(df_active)
        df_hist_diffs = df_hist_diffs.append(df_new)

    df_hist_diffs = df_hist_diffs.append(df[df['scrap_meta.spider_date'] == all_dates[0]])
    df_hist_diffs.to_csv(db_file_path)



def transform_all(settings, delete_scrap_files):
    print('----------------------------')
    if (not os.path.exists(settings.products_database_filepath)):
        database = pd.DataFrame()
        print(f'Database file created: {settings.products_database_filepath}')
    else:
        database = pd.read_csv(settings.products_database_filepath)
        print(f'Database file loaded: {settings.products_database_filepath}')

    for scrap_file_name in settings.scrapes_filepath_list:
        try:
            scrap_filepath = os.path.join(settings.scrapes_folderpath, scrap_file_name)
            df_scrap = load_scrap(scrap_filepath)
            print(f'Scrap file loaded: {scrap_filepath}')

            # empty scrap, delete
            if (df_scrap['scrap_meta.guid'].count() != 0):
                df_scrap = transform(df_scrap, settings.transformer)
                print(f'Scrap transformed.')

                database = database.append(df_scrap, ignore_index=True)
                print(f'Appended database.')

                del_columns = [col for col in database if 'Unnamed' in col]
                database.drop(columns=del_columns, inplace=True)

                database.to_csv(settings.products_database_filepath)
                print(f'Database saved.')

                #delete_scrap(scrap_filepath)
                if(delete_scrap_files):
                    if os.path.exists(scrap_file_path):
                        os.remove(scrap_file_path)
                    else:
                        print(f"The file {scrap_file_path} does not exist.")
                    print(f'Scrap deleted.')

        except Exception as error:
            settings.show_error(str(error))
            print(str(error))
            exit()
