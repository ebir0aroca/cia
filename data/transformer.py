f2 = pd.json_normalize(di, record_path=['reviews'], meta=cols)
  return df2


def load_database(db_file_path, compression="infer"):
    print('----------------------------')
    print(f'Loading database file:')
    print(f'    > file: {db_file_path}')

    db_df = pd.read_csv(db_file_path)
    return db_df


def create_empty_database(db_file_path):
    df = pd.DataFrame(columns=HIST_DATAFRAME_COLUMNS)
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


def get_lastdate(df):
    all_dates = df['scrap__spider_date']
    all_datetimes = [datetime.strptime(x, '%d-%m-%Y') for x in all_dates]

    return max(all_datetimes)

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


def transform_all(scrapes_folder_path, dbs_file_path):
    scrapes_filenames = next(scrapes_folder_path, (None, None, []))[2]

    for scrap_file_name in scrapes_filenames:
        try:
            scrap_file_path = os.walk(scrapes_folder_path, scrap_file_name)
            print('----------------------------')
            print(f'Loading Scrap file: {scrap_file_path}')
            df_scrap = load_scrap(scrap_file_path)

            # empty scrap, delete
            if (df_scrap['scrap_meta.guid'].count() != 0):
                print(f'Database file: {db_file_path}')
            print(f'Transform database.')
            transform(df_scrap)

            print(f'Append to database.')
            append_to_database(df_scrap, db_file_path)
            db = load_database(db_file_path)

            del_columns = [col for col in db if 'Unnamed' in col]
            db.drop(columns=del_columns, inplace=True)
            print(f'Save database.')
            save_database(db, db_file_path)

            # db2 = flatten_product_reviews(db)
            # save_database(db2, revs_snapshot_file_path)

            print(f'Delete scrap.')
            delete_scrap(scrap_file_path)

        except Exception as error:
            show_error(str(error))
            print(str(error))
            exit()

