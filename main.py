'''
    pip install pandas
    pip install regex
'''

try:
    import os
    import pandas as pd
    import numpy as np
    import sys
    import arv.data.transformer as DB
    import arv.core.settings as APP

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    exit()


settings = APP.Settings(sys.path[0])

print(" 0. Transform all")
print(" 1. Get DB info")
chose = input("Choose an option.")
if(chose=="0"):
    #ATENCION: no elimina el SCRAP
    B.transform_all(settings=settings, delete_scrap_files=False)
elif(chose=="1"):
    db = pd.read_csv(settings.products_database_filepath)
    DB.dataframe_info(db, "TITULAR")


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