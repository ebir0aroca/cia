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
                     "img_url":"string",
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
                     "scrap__title":"string"
                  }

HIST_DATAFRAME_COLUMNS_RENAME = [{"scrapper":"hornbach", "country":"de",
                                        "rename_cols":[
                                            {"scrap_meta.", "scrap__"},
                                            {"specs.Artikeltyp":"product_type1"}
                                        ],
                                        "category": {
                                            "Bathroom mirror":[{"category4":"Badspiegel"}],
                                            "Mirror cabinet":[{"category4":"Spiegelschränke"}],
                                            "Cosmetic mirror":[{"category4":"Kosmetikspiegel"}],
                                            "Bathroom lighting":[{"category4":"Badleuchten & Spiegelleuchten"}]
                                         }
                                }]


def load_scrap(scrap_file_path, compression="infer"):
  with open(scrap_file_path, 'r', encoding='utf-8') as json_file:
    data = json.load(json_file)  
 
  db_df =  pd.DataFrame()
  db_df = pd.json_normalize(
              data, record_path=['scraped_products_data'], 
              meta=SCRAP_META
            )
  
  return db_df


def dataframe_metainfo(db_df, title):
  print('----------------------------')
  print(f'Dataframe info: ')
  print(f'    > Name:                   {title}')
  print(f'    > SKU count:              {db_df["sku"].count()}')
  print(f'    > Scrap processes count:  {db_df.groupby(by=["scrap__guid"]).scrap__guid.unique().count()}')
  print(f'    > Marketplaces count:     {db_df.groupby(by=["scrap__spider_marketplace"]).scrap__spider_marketplace.unique().count()}')



def transform(dataframe):
  #change scrap_meta col names
  dataframe.rename(columns=lambda x: x.replace('scrap_meta.', 'scrap__'), inplace=True)
  marketplace_name, marketplace_country= dataframe.tail(1)["scrap__spider_marketplace"].values[0], dataframe.tail(1)["scrap__spider_country"].values[0]

  #select specs cols & clean
  if(marketplace_name=="hornbach"):
      df_0 = dataframe.loc[(dataframe['scrap__spider_marketplace'] == "hornbach")]
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

      #By marketplace and country
      if(marketplace_country=="de"):
            df_1 = df_0.loc[(df_0['scrap__spider_country'] == "de")]
            df_1.rename(columns={"specs.Artikeltyp":"product_type1"}, inplace=True)
            df_1.rename(columns={"specs.Ausführung":"product_type2"}, inplace=True)
            df_1.rename(columns={"specs.Variante":"product_variant"}, inplace=True)
            df_1.rename(columns={"specs.Art":"product_art"}, inplace=True)

            #delete all unwanted categories
            category4_list = ['Badspiegel','Kosmetikspiegel','Badezimmerschränke','Badleuchten & Spiegelleuchten']
            subset = df_1[( ~df_1['category4'].isin(category4_list) )]
            df_1.drop(subset.index, inplace = True)

            #Category
            df_1.loc[(df_1["category4"]=="Badspiegel"), "category"]= "Bathroom mirror"
            df_1.loc[(df_1["category4"]=="Spiegelschränke"), "category"]= "Mirror cabinet"
            df_1.loc[(df_1["category4"]=="Kosmetikspiegel"), "category"]= "Cosmetic mirror"
            df_1.loc[(df_1["category4"]=="Badleuchten & Spiegelleuchten"), "category"]= "Bathroom lighting"

            df_1.loc[(df_1["category4"] == "Badspiegel") & (df_1["product_type2"] == "Lichtspiegel"), "category"] = "Illuminated bathroom mirror"
            #df_1.loc[(df_1["category4"] == "Badspiegel")& (df_1["product_type2"] == "Standspiegel"), "category"] = "Foot bathroom Mirror"
            df_1.loc[(df_1["category4"] == "Spiegelschränke") & (df_1["product_variant"] == "Mit Beleuchtung"), "category"] = "Illuminated mirror cabinet"

            res_df = res_df.append(df_1)

      elif(marketplace_country=="se"):
            df_1 = df_0.loc[(df_0['scrap__spider_country'] == "se")]
            df_1.rename(columns={"specs.Artikeltyp":"product_type1"}, inplace=True)
            df_1.rename(columns={"specs.Utförande":"product_type2"}, inplace=True)
            df_1.rename(columns={"specs.Variant":"product_variant"}, inplace=True)

            #delete all unwanted categories
            category4_list = ['Badrumsspeglar','Spegelskåp','Badrumsbelysning','Badrumsaccessoarer']
            #category5_list = ['Sminkspeglar']
            subset = df_1[( ~df_1['category4'].isin(category4_list) )]
            #& ~df_1['category5'].isin(category5_list))]
            df_1.drop(subset.index, inplace = True)

            df_1.loc[(df_1["category4"]=="Badrumsspeglar"), "category"]= "Bathroom mirror"
            df_1.loc[(df_1["category4"]=="Spegelskåp"), "category"]= "Mirror cabinet"
            #df_1.loc[(df_1["category5"]=="Sminkspeglar"), "category"]= "Cosmetic mirror"
            df_1.loc[(df_1["category4"]=="Badrumsbelysning"), "category"]= "Bathroom lighting"

            df_1.loc[(df_1["category4"] == "Badrumsspeglar")& (df_1["product_type2"] == "Belyst spegel"), "category"] = "Illuminated bathroom mirror"
            df_1.loc[(df_1["category4"] == "Spegelskåp") & (df_1["product_variant"] == "Med belysning"), "category"] = "Illuminated mirror cabinet"


            res_df = res_df.append(df_1)

      #end of hornbach process
      # Blanks, no NaN
      #df_0['seller'] = df_0['seller'].fillna('')
      #df_0['brand'] = df_0['brand'].fillna('')

  '''
  #convert to type when all names have been changed
  for column_name, column_type in HIST_DATAFRAME_COLUMNS.items():
    #If col doesn't exist, create a blank one
    if not column_name in res_df.columns:
      res_df[column_name] = ""
    if(column_type != "string"):
      res_df[column_name] = pd.to_numeric(res_df[column_name], errors='coerce')
  res_df[column_name] = res_df[column_name].astype(column_type)
  '''
  #covert dates
  res_df["scrap__spider_date_start"] = pd.to_datetime(res_df["scrap__spider_date_start"])
  res_df["scrap__spider_date_end"] = pd.to_datetime(res_df["scrap__spider_date_end"])
  res_df["scrap__spider_date"] = pd.to_datetime(res_df["scrap__spider_date_end"]).dt.date
  res_df["creation_date"] = pd.to_datetime(res_df["creation_date"]).dt.date


  #delete not used cols
  del_columns = [col for col in res_df if 'scrap_meta.' in col]
  res_df.drop(columns=del_columns, inplace=True)
  del_columns = [col for col in res_df if 'specs.' in col]
  res_df.drop(columns=del_columns, inplace=True)
  del_columns = [col for col in res_df if 'custom.' in col]
  res_df.drop(columns=del_columns, inplace=True)

  #res_df.drop(columns=['reviews'], inplace=True) #delete reviews

  # drop categories
  res_df.drop(columns=['category0', 'category1', 'category2', 'category3', 'category4', 'category5', 'category6'],
                inplace=True, errors='ignore')

  # drop any other col that is not in the data model
  # res_df = res_df[res_df.columns.intersection(HIST_DATAFRAME_COLUMNS)]


  print(f'Cleaned Scrap file for marketplace: {marketplace_name}.{marketplace_country}')
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
    all_dates = database['scrap__spider_date']
    all_datetimes = [datetime.strptime(x, date_format) for x in all_dates]

    return max(all_datetimes)

def hist_diff(df, date0, date1):
    '''
      returns series of union, intersect and not common
    '''
    df0 = df[df['scrap__spider_date'] == date0]
    df1 = df[df['scrap__spider_date'] == date1]

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
                df_scrap = transform(df_scrap)
                print(f'Scrap transformed.')

                database = database.append(df_scrap, ignore_index=True)
                print(f'Appended database.')

                del_columns = [col for col in database if 'Unnamed' in col]
                database.drop(columns=del_columns, inplace=True)

                database.to_csv(settings.products_database_filepath)
                print(f'Database saved.')

                #delete_scrap(scrap_filepath)
                if(delete_scrap_file):
                    if os.path.exists(scrap_file_path):
                        os.remove(scrap_file_path)
                    else:
                        print(f"The file {scrap_file_path} does not exist.")
                    print(f'Scrap deleted.')

        except Exception as error:
            settings.show_error(str(error))
            print(str(error))
            exit()

def transform_reviews(settings):
    # db2 = flatten_product_reviews(db)
    # save_database(db2, revs_snapshot_file_path)
    pass
