try:
    import os
    from datetime import datetime
    import pandas as pd
    import csv
    import json

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    exit()


class Settings:
    root_path = ''
    log_problems = True
    date_format = '%Y-%m-%d'
    transformer = pd.DataFrame()

    def __init__(self, root_path):
        self.root_path = root_path

        file = os.path.join(self.root_path, 'arv', 'core', 'config', 'product_data_transformer.json')
        with open(file, encoding='utf-8') as f:
            data = json.load(f)

        self.transformer = pd.DataFrame.from_dict(next(iter(data['transformer'])))

    @property
    def scrapes_folderpath(self):
        return os.path.join(self.root_path, 'scrapes')

    @property
    def databases_folderpath(self):
        return os.path.join(self.root_path, 'dbs')

    @property
    def products_database_filepath(self):
        return os.path.join(self.root_path, 'dbs', 'db_prods_history.csv')

    @property
    def reviews_database_filepath(self):
        return os.path.join(self.root_path, 'dbs', 'db_revs_snapshot.csv')

    @property
    def monitoring_results_filepath(self):
        return os.path.join(self.databases_folderpath, 'monitoring_results.csv')

    @property
    def log_filepath(self):
        return os.path.join(self.root_path, 'logs', 'log.csv')

    @property
    def scrapes_filepath_list(self):
        return  next(os.walk(self.scrapes_folderpath), (None, None, []))[2]


    @property
    def product_meta(self):
        file = os.path.join(self.root_path, 'arv', 'core','config', 'product_datamodel.json')
        return pd.read_csv(file)

    def load_websites_monitor_list(self):
        file = os.path.join(self.root_path, 'arv', 'core', 'config', 'websites_monitor_list.json')
        with open(file, encoding='utf-8') as f:
            data = json.load(f)
        return pd.DataFrame.from_dict(data['websites_monitor_list'])


    def load_db(self, fromZip):
        dtype = {}
        for key in self.product_meta.keys():
            dtype[key] = self.product_meta[key]
        if(fromZip):
            return pd.read_csv(self.products_database_filepath, dtype=dtype, low_memory=False)
        else:
            return pd.read_csv(self.products_database_filepath.replace('csv','zip'), dtype=dtype, low_memory=False, compression = 'zip')


    def show_error(self, error, e=True):
        if self.log_problems:
            if e:
                el = str(error) + 'on line:' + str(error.__traceback__.tb_lineno)
            else:
                el = error
            print(el)
            el = [datetime.datetime.now().strftime('%D %T'), "PRE-PREPROCESS", "", el]
            with open(self.log_filepath, 'a', newline='') as f:
                cw = csv.writer(f)
                cw.writerow(el)