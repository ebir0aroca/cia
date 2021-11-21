try:
    import os

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    #input('press enter to exit....')
    exit()


class Settings:
    root_path = ''
    dbs_file_path = ''
    revs_snapshot_file_path = ''
    scrapes_file_path = ''
    log_file_path = ''
    log_problems = True

    def __init__(self, root_path):
        self.root_path = root_path

        self.dbs_file_path = os.path.join(root_path, 'dbs', 'db_prods_history.csv')
        self.revs_snapshot_file_path = os.path.join(root_path, 'dbs', 'db_revs_snapshot.csv')
        self.scrapes_file_path = os.path.join(root_path, 'scrapes')
        self.log_file_path = os.path.join(root_path, 'logs', 'log.csv')
