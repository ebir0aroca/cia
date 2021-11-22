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
  import arv.core.settings as APP

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    input('press enter to exit....')
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


def load_scrap(scrap_file_path, compression="infer"):
  with open(scrap_file_path, 'r', encoding='utf-8') as json_file:
    data = json.load(json_file)  
 
  db_df =  pd.DataFrame()
  db_df = pd.json_normalize(
              data, record_path=['scraped_products_data'], 
              meta=SCRAP_META
            )
  
  return db_df

def create_empty_database(db_file_path):
  df = pd.DataFrame(columns = HIST_DATAFRAME_COLUMNS)
  df.to_csv(db_file_path)

def dataframe_metainfo(db_df, title):
  print('----------------------------')
  print(f'Dataframe info: ')
  print(f'    > Name:                   {title}')
  print(f'    > SKU count:              {db_df["sku"].count()}')
  print(f'    > Scrap processes count:  {db_df.groupby(by=["scrap__guid"]).scrap__guid.unique().count()}')
  print(f'    > Marketplaces count:     {db_df.groupby(by=["scrap__spider_marketplace"]).scrap__spider_marketplace.unique().count()}')


def append_to_database(df_scrap, db_file_path):
  database = load_database(db_file_path)
  database = database.append(df_scrap, ignore_index=True)
  database.to_csv(db_file_path)

def save_database(database, db_file_path):
  database.to_csv(db_file_path)



def load_database(database_filepath):
  database = pd.read_csv(database_filepath)
  return database


def delete_scrap(scrap_file_path):  
  if os.path.exists(scrap_file_path):
    os.remove(scrap_file_path)
  else:
    print(f"The file {scrap_file_path} does not exist.")



def transform(df):
  #change scrap_meta col names
  df.rename(columns=lambda x: x.replace('scrap_meta.', 'scrap__'), inplace=True)
  marketplace_name, marketplace_country= df.tail(1)["scrap__spider_marketplace"].values[0] ,df.tail(1)["scrap__spider_country"].values[0]

  #select specs cols & clean
  if(marketplace_name=="hornbach"):
    #select custom cols & clean
    df.rename(columns={"custom.highlight":"isHighlight"}, inplace=True)
    df['isHighlight'].replace(to_replace=['TOP'], value=True, inplace=True)
    df['isHighlight'].replace(to_replace=[None], value=False, inplace=True)
    df['specialPrice']=df['specialPrice'].replace(np.nan, 0)
    df['onlineShippingCost']=df['onlineShippingCost'].replace(np.nan, 0)
        
    #Store Brand
    df['isStoreBrand'] = False
    df['isStoreBrand']=df['brand'].isin(['basano'])

    # Product is configurable?
    df['isConfigurable'] = False

    #breadcrumbs, creation_date (last review date),
    for i, row in df.iterrows():
      if(type(df.at[i,'breadcrumbs'])==list):
        for j in range(len(df.at[i,'breadcrumbs'])):
          df.at[i,'category'+str(j)] = df.at[i,'breadcrumbs'][j]      
      if(df.at[i,'reviews_count']>0):
        dates = []
        for each_date in df.at[i,'reviews']:
          dates.append(pd.to_datetime(each_date['review_date']))
        if(len(dates)>0):
          df.at[i,'creation_date'] = max(dates)

      df.at[i, 'img_url'] = ast.literal_eval(df.at[i, 'img_urls'])[0]
      df.at[i,'isConfigurable']= (df.at[i,'confs'] != "[]")


    # Blanks, no NaN
    df['category'] = ""


    #By marketplace and country
    if(marketplace_country=="de"):
      df.rename(columns={"specs.Artikeltyp":"product_type1"}, inplace=True)
      df.rename(columns={"specs.Ausführung":"product_type2"}, inplace=True)

      category4_list = ['Badspiegel','Kosmetikspiegel','Badezimmerschränke','Badleuchten & Spiegelleuchten'] 
      subset = df[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="de") &  (~df['category4'].isin(category4_list))]
      df.drop(subset.index, inplace = True)

      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="de") & (df["category4"]=="Badspiegel"), "category"]= "Bathroom mirror"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="de") & (df["category4"]=="Badezimmerschränke"), "category"]= "Mirror cabinet"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="de") & (df["category4"]=="Kosmetikspiegel"), "category"]= "Cosmetic mirror"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="de") & (df["category4"]=="Badleuchten & Spiegelleuchten"), "category"]= "Bathroom lighting"


    elif(marketplace_country=="se"):
      df.rename(columns={"specs.Artikeltyp":"product_type1"}, inplace=True)
      df.rename(columns={"specs.Utförande":"product_type2"}, inplace=True)

      category4_list = ['Badrumsspeglar','Spegelskåp','Badrumsbelysning','Badrumsaccessoarer']
      #category5_list = ['Sminkspeglar']
      subset = df[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="se") &  (~df['category4'].isin(category4_list) )]#& ~df['category5'].isin(category5_list))]
      df.drop(subset.index, inplace = True)
      
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="se") & (df["category4"]=="Badrumsspeglar"), "category"]= "Bathroom mirror"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="se") & (df["category4"]=="Spegelskåp"), "category"]= "Mirror cabinet"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="se") & (df["category4"]=="Badrumsbelysning"), "category"]= "Bathroom lighting"

    elif(marketplace_country=="cz"):
      df.rename(columns={"specs.Druh výrobku":"product_type1"}, inplace=True)
      df.rename(columns={"specs.Provedení":"product_type2"}, inplace=True)
            
      category4_list = ['Zrcadla do koupelny', 'Kosmetická zrcadla', 'Zrcadlové skříňky']
      #category5_list = ['Osvětlení do koupelny a nad zrcadlo']
      subset = df[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="cz") &  (~df['category4'].isin(category4_list) )]#& ~df['category5'].isin(category5_list))]
      df.drop(subset.index, inplace = True)

      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="cz") & (df["category4"]=="Zrcadla do koupelny"), "category"]= "Bathroom mirror"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="cz") & (df["category4"]=="Zrcadlové skříňky"), "category"]= "Mirror cabinet"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="cz") & (df["category4"]=="Kosmetická zrcadla"), "category"]= "Cosmetic mirror"

    elif(marketplace_country=="sk"):
      df.rename(columns={"specs.Druh výrobku":"product_type1"}, inplace=True)
      df.rename(columns={"specs.Vyhotovenie":"product_type2"}, inplace=True)
      df.rename(columns={"specs.Provedení":"product_type2"}, inplace=True)

      category4_list = ['Zrkadlá do kúpeľne', 'Kozmetické zrkadlá', 'Zrkadlové skrinky']
      #category5_list = ['Kúpeľňové a zrkadlové svietidlá'] 
      subset = df[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="sk") &  (~df['category4'].isin(category4_list) )]#& ~df['category5'].isin(category5_list))]
      df.drop(subset.index, inplace = True)

      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="sk") & (df["category4"]=="Zrkadlá do kúpeľne"), "category"]= "Bathroom mirror"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="sk") & (df["category4"]=="Zrkadlové skrinky"), "category"]= "Mirror cabinet"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="sk") & (df["category4"]=="Kozmetické zrkadlá"), "category"]= "Cosmetic mirror"

    elif(marketplace_country=="ro"):
      df.rename(columns={"specs.Tip produs":"product_type1"}, inplace=True)
      df.rename(columns={"specs.Model":"product_type2"}, inplace=True)

      category4_list = ['Oglinzi baie','Oglinzi cosmetice','Dulapuri cu oglinda','Iluminat pentru baie']
      subset = df[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="sk") &  (~df['category4'].isin(category4_list))]
      df.drop(subset.index, inplace = True)      

      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="ro") & (df["category4"]=="Dulapuri cu oglinda"), "category"]= "Mirror cabinet"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="ro") & (df["category4"]=="Oglinzi baie"), "category"]= "Bathroom mirror"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="ro") & (df["category4"]=="Oglinzi cosmetice"), "category"]= "Cosmetic mirror"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="ro") & (df["category4"]=="Iluminat pentru baie"), "category"]= "Bathroom lighting"

    elif(marketplace_country=="at"):
      df.rename(columns={"specs.Artikeltyp":"product_type1"}, inplace=True)
      df.rename(columns={"specs.Ausführung":"product_type2"}, inplace=True)

      #'Badleuchten & Spiegelleuchten' not in the Bathroom section
      category4_list = ['Badezimmerspiegel','Kosmetikspiegel','Badezimmerschränke'] 
      subset = df[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="de") &  (~df['category4'].isin(category4_list))]
      df.drop(subset.index, inplace = True)      

      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="at") & (df["category4"]=="Badezimmerspiegel"), "category"]= "Bathroom mirror"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="at") & (df["category4"]=="Badezimmerschränke"), "category"]= "Mirror cabinet"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="at") & (df["category4"]=="Kosmetikspiegel"), "category"]= "Cosmetic mirror"


    elif(marketplace_country=="ch"):
      df.rename(columns={"specs.Artikeltyp":"product_type1"}, inplace=True)
      df.rename(columns={"specs.Ausführung":"product_type2"}, inplace=True)      

      #Kosmetikspiegel is inside Badezimmerspiegel
      category4_list = ['Badezimmerspiegel','Badezimmerschränke','Badleuchten & Spiegelleuchten'] 
      subset = df[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="de") &  (~df['category4'].isin(category4_list))]
      df.drop(subset.index, inplace = True)      

      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="ch") & (df["category4"]=="Badezimmerschränke"), "category"]= "Mirror cabinet"      
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="ch") & (df["category4"]=="Badezimmerspiegel"), "category"]= "Bathroom mirror"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="ch") & (df["category4"]=="Badleuchten & Spiegelleuchten"), "category"]= "Bathroom lighting"

    #Lacks Luxemburg

    elif(marketplace_country=="nl"):
      df.rename(columns={"specs.Type artikel":"product_type1"}, inplace=True)
      df.rename(columns={"specs.Uitvoering":"product_type2"}, inplace=True)

      #'Kosmetikspiegel' inside ,'Badleuchten & Spiegelleuchten' don't knw where
      category4_list = ['Badkamerspiegels','Badkamerkasten'] 
      subset = df[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="de") &  (~df['category4'].isin(category4_list))]
      df.drop(subset.index, inplace = True)

      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="nl") & (df["category4"]=="Badkamerspiegels"), "category"]= "Bathroom mirror"
      df.loc[(df['scrap__spider_marketplace']=="hornbach") & (df['scrap__spider_country']=="nl") & (df["category4"]=="Badkamerspiegelkasten"), "category"]= "Mirror cabinet"



  #convert to type when all names have been changed
  for column_name, column_type in HIST_DATAFRAME_COLUMNS.items(): 
    #If col doesn't exist, create a blank one
    if not column_name in df.columns:
      df[column_name] = ""
    if(column_type != "string"):
      df[column_name] = pd.to_numeric(df[column_name], errors='coerce')      
    df[column_name] = df[column_name].astype(column_type)

  #covert dates
  df["scrap__spider_date_start"] = pd.to_datetime(df["scrap__spider_date_start"])
  df["scrap__spider_date_end"] = pd.to_datetime(df["scrap__spider_date_end"])
  df["scrap__spider_date"] = pd.to_datetime(df["scrap__spider_date_end"]).dt.date
  df["creation_date"] = pd.to_datetime(df["creation_date"]).dt.date

  
  #delete not used cols
  del_columns = [col for col in df if 'specs.' in col]
  df.drop(columns=del_columns, inplace=True)
  del_columns = [col for col in df if 'custom.' in col]
  df.drop(columns=del_columns, inplace=True)
  del_columns = [col for col in df if 'scrap_meta.' in col]
  df.drop(columns=del_columns, inplace=True)

  #df.drop(columns=['reviews'], inplace=True) #delete reviews

  # drop categories
  df.drop(columns=['category0', 'category1', 'category2', 'category3', 'category4', 'category5', 'category6'],
          inplace=True, errors='ignore')

  # drop any other col that is not in the data model
  # df = df[df.columns.intersection(HIST_DATAFRAME_COLUMNS)]

  # Blanks, no NaN
  df['seller'] = df['seller'].fillna('')
  df['brand'] = df['brand'].fillna('')

  print(f'Cleaned Scrap file for marketplace: {marketplace_name}.{marketplace_country}')

  return df


def flatten_product_reviews(df):
  cols = ['sku', 'title', 'category', 'brand', 'isStoreBrand', 'price', 'reviews']
  df1 = df[df['reviews_count'] > 0][cols]

  for i, row in df1.iterrows():
      df1.at[i, 'reviews'] = ast.literal_eval(str(df1.at[i, 'reviews']))
  di = df1.to_dict('records')
  cols.remove('reviews')
  df2 = pd.json_normalize(di, record_path=['reviews'], meta=cols)
  return df2


def load_database(file_path, compression="infer"):
    print('----------------------------')
    print(f'Loading database file: {file_path}')

    database = pd.read_csv(file_path)
    return database


def create_empty_database(db_file_path):
    df = pd.DataFrame(columns=HIST_DATAFRAME_COLUMNS)
    df.to_csv(db_file_path)


def append_to_database(df, db_file_path):
    database = load_database(db_file_path)
    database = database.append(df, ignore_index=True)
    database.to_csv(db_file_path)


def save_database(database, db_file_path):
    database.to_csv(db_file_path)

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
    all_dates = df['scrap__spider_date'].unique()
    date_ranges = [{}]
    df['hist_status'] = "ACTIVE"

    for i in range(len(all_dates)):
        if (i < len(all_dates) - 1):
            date_ranges[i]['start_date'] = all_dates[i]
            date_ranges[i]['end_date'] = all_dates[i + 1]

    for date_range in date_ranges:
        idx_union, idx_intersect, idx_notcommon, idx_intersect_left, idx_intersect_right = hist_diff(df, date_range[
            'start_date'], date_range['end_date'])

        df0 = df[df['scrap__spider_date'] == date_range['start_date']]
        df1 = df[df['scrap__spider_date'] == date_range['end_date']]

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

    df_hist_diffs = df_hist_diffs.append(df[df['scrap__spider_date'] == all_dates[0]])
    df_hist_diffs.to_csv(db_file_path)


def transform_all(settings):
    scrapes_filenames = next(settings.scrapes_folderpath, (None, None, []))[2]

    for scrap_file_name in scrapes_filenames:
        try:
            scrap_filepath = os.walk(settings.scrapes_folderpath, scrap_file_name)
            print('----------------------------')
            print(f'Loading Scrap file: {scrap_filepath}')
            df_scrap = load_scrap(scrap_filepath)

            # empty scrap, delete
            if (df_scrap['scrap_meta.guid'].count() != 0):
                print(f'Database file: {settings.products_database_filepath}')
            print(f'Transform database.')
            transform(df_scrap)

            print(f'Append to database.')
            append_to_database(df_scrap, settings.products_database_filepath)
            db = load_database(settings.products_database_filepath)

            del_columns = [col for col in db if 'Unnamed' in col]
            db.drop(columns=del_columns, inplace=True)
            print(f'Save database.')
            save_database(db, settings.products_database_filepath)


            print(f'Delete scrap.')
            delete_scrap(scrap_filepath)

        except Exception as error:
            settings.show_error(str(error))
            print(str(error))
            exit()

def transform_reviews(settings):
    # db2 = flatten_product_reviews(db)
    # save_database(db2, revs_snapshot_file_path)
    pass
