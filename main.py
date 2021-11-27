'''
    pip install pandas
    pip install regex
    pip install PyDrive
    pip install bs4
'''

try:
    import os
    import pandas as pd
    import numpy as np
    import sys
    import arv.data.transformer as DB
    import arv.core.settings as APP
    import arv.sources.website_monitor as WSM
    from zipfile import ZipFile

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    exit()

settings = APP.Settings(sys.path[0])


print(" 0. Transform all")
print(" 1. Get DB info")
print(" 2. Get DB info (zip)")
print(" 3. Get last Snapshot")
print(" 4. Get last Snapshot")
chose = input("Choose an option.")
if(chose=="0"):
    #ATENCION: no elimina el SCRAP
    DB.transform_all(settings=settings, delete_scrap_files=False)
elif(chose=="1"):
    db = settings.load_db(fromZip=False)
    DB.dataframe_info(db, "TITULAR")
elif(chose=="2"):
    db = settings.load_db(fromZip=True)
    DB.dataframe_info(db, "TITULAR")
elif(chose=="3"):
    db = settings.load_db()
    last_date = DB.get_lastdate(db, settings.date_format)
    db_snapshot = db[db['scrap_meta.spider_date'] == last_date]
elif(chose=="4"):
    websites_to_monitor = settings.load_websites_monitor_list()
    monitoring_results_df = WSM.monitor_websites(websites_to_monitor)
    monitoring_results_df.to_csv(os.path.join(settings.databases_folderpath, 'monitoring_results.csv'))


#idx_union, idx_intersect, idx_notcommon, idx_intersect_left, idx_intersect_right = DB.hist_diff(db, '2021-11-06', '2021-11-13')
#print(idx_intersect_right)
'''

ROADMAP:
    Cambiar el proceso de append a la base de datos y que no cargue toda cada vez    
    Controlar los data types en los procesos de append para evitar DtypeWarning
    
    Diffs entre scrappers: altas y bajas
    Sistema de limpieza y detección de errores
    Snapshot 
    Y snapshot reviews 
    BI analytics 



#print(settings.scrapes_filepath_list)


last_date = DB.get_lastdate(database=db, date_format=settings.date_format)
last_snapshot =db[db['scrap__spider_date']==last_date.strftime(settings.date_format)]

'''