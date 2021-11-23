try:
    import os
    from datetime import datetime
    import csv

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    exit()


class Settings:
    root_path = ''
    log_problems = True
    date_format = '%Y-%m-%d'


    def __init__(self, root_path):
        self.root_path = root_path

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
    def log_filepath(self):
        return os.path.join(self.root_path, 'logs', 'log.csv')

    @property
    def scrapes_filepath_list(self):
        return  next(os.walk(self.scrapes_folderpath), (None, None, []))[2]


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