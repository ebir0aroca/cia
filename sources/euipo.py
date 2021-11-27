try:
    import requests
    from bs4 import BeautifulSoup
    import re
    import time
    import random
    import threading
    import json
    import datetime
    import os
    import sys
    import uuid
    import csv
except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    input('press enter to exit....')
    exit()


# functions
def show_error(error, e=True):
    if log_problems:
        if e:
            el = str(error) + 'on line:' + str(error.__traceback__.tb_lineno)
        else:
            el = error
        print(el)
        el = [datetime.datetime.now().strftime('%D %T'), website_version, main_category_link, el]
        with open(log_file_path, 'a', newline='') as f:
            cw = csv.writer(f)
            cw.writerow(el)


def re_get(p, s):
    try:
        return re.findall(p, s, re.IGNORECASE)[0].strip()

    except Exception as error:
        show_error(error)
        return ''


def get(u):
    for _try_ in range(0, 3, 1):
        try:
            return requests.get(u, headers=headers, timeout=30).text

        except Exception as error:
            show_error(error)
            print('retrying in 3 seconds...')
            time.sleep(3)
    return ''



log_problems = True
main_link = "https://euipo.europa.eu/eSearch/#details/owners/732091"
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'
}

try:
    print("getting owners...")
    bso = BeautifulSoup(get(main_link), 'html.parser')
    header = bso.select('header')

    print(bso)

except Exception as error:
    show_error(error)
    print('failed to get sub-categories clinks from main category link.'
          '\nplease check your main link and ensure that there are any sub-categories links present in it.')

    exit()
