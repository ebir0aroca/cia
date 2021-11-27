'''
    !pip install ultimate_sitemap_parser
    https://urlwatch.readthedocs.io/en/latest/index.html
    https://visualping.io/
'''


try:
    import pandas as pd
    import csv
    import datetime
    from usp.tree import sitemap_tree_for_homepage
except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    exit()



def monitor_websites(websites_to_monitor):
  monitoring_results = []

  for index, row in websites_to_monitor.iterrows():
    name = row['name']
    for url in row['urls']:
      #print( f"STARTING : {name} >> {url}")
      #First searches the sitemap in the most common folders ...
      url_sitemap_tree = sitemap_tree_for_homepage(url)

      #...then retrieves the info
      for page in url_sitemap_tree.all_pages():
        try:
          monitoring_results.append({ 'site_name':name,
                                    'session_date': datetime.datetime.now().isoformat(),
                                    'site_url': url,
                                    'sitemap_page_url': page.url,
                                    'sitemap_page_frequency': page.change_frequency,
                                    'sitemap_page_last_modified': page.last_modified
                          })
        except :
            print(f" ERROR AT: {name} >> {url}")

  #convert to pandas and dates
  monitoring_results_df = pd.DataFrame.from_dict(monitoring_results)
  monitoring_results_df['sitemap_page_last_modified'] = pd.to_datetime(monitoring_results_df['sitemap_page_last_modified'], utc=True)
  monitoring_results_df['sitemap_page_last_modified'] = monitoring_results_df['sitemap_page_last_modified'].dt.date

  return monitoring_results_df

