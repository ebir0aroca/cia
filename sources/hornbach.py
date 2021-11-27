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


# vars
main_category_link = sys.argv[2]  # "https://www.hornbach.de/shop/Badewannen/S516/artikelliste.html"
website_version = sys.argv[1]  # 'de'
print(f'hornbach scraper. website version: {website_version}, category link: {main_category_link}')

number_of_threads = 7
log_problems = True
sub_categories_links = []
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'
}
site_link = f"https://www.hornbach.{website_version}"
output_data = {
    "scrap_meta": {
        'guid': str(uuid.uuid4()),
        'date_start': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'date_end': "",
        'title': "Weekly scraping process",
        'spider_name': "hornbach_spider",
        'spider_date_start': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'spider_date_end': "",
        'spider_version': "1.0",
        'spider_marketplace': "hornbach",
        'spider_country': website_version,
        'spider_lang': website_version,
        'marketplace_url': f"https://www.hornbach.{website_version}",
        'maincategory_url': main_category_link
    },
    "scraped_products_data": []
}
log_file_path = os.path.join(sys.path[0], 'log.csv')


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


def get_subcategories_links():
    try:
        print("getting sub categories links from main link...")
        bso = BeautifulSoup(get(main_category_link), 'html.parser')

        # capturing links from cards
        links = bso.select('#content div.teasergroup a[href*="/shop/"]')
        for link in links:
            ll = site_link + link['href']
            if not(ll in sub_categories_links):
                sub_categories_links.append(ll)

        # capturing direct links
        links = bso.select('#content > a[href*="/shop/"]')
        for link in links:
            ll = site_link + link['href']
            if not (ll in sub_categories_links):
                sub_categories_links.append(ll)

        print('got', len(sub_categories_links), 'subcategories links from main link...')

    except Exception as error:
        show_error(error)
        print('failed to get sub-categories clinks from main category link.'
              '\nplease check your main link and ensure that there are any sub-categories links present in it.')
        input('Therefore, stopping further operations....')
        exit()


def get_market_category_code(sub_cat_link):
    try:
        ps_ = get(sub_cat_link)
        return [
            str(re_get(r", marketCode: '(.*?)',", ps_)),
            str(re_get(r", categoryCode: '(.*?)',", ps_))
        ]

    except Exception as error:
        show_error(error)
        return []


def get_total_pages_and_products(market_code, category_code):
    try:
        data_link = f'https://www.hornbach.{website_version}/mvc/article/load/article-list/{website_version}/' + market_code + '/' + category_code
        data_link += '/36/1/sortModeDv'

        jd = json.loads(get(data_link))
        return [jd['pageCount'], jd['countResult']]

    except Exception as error:
        show_error(error)
        return []


def json_parse(j_object, j_path):
    try:
        res = j_object
        for key in j_path.split('.'):
            if key == '0':
                key = 0
            res = res[key]
        return res
    except:
        return ''


def single_get_products_data(u, sub_cat_link, sub_cat_pagination):
    try:
        jd = json.loads(get(u))
        product_pos = 0
        for article in jd['articles']:
            product_pos += 1
            try:
                temp_row = {
                    'title': article['title'],
                    'sku': article['articleCode'],
                    'product_url': site_link + article['localizedExternalArticleLink'],
                    'product_pos_in_page': str(product_pos),
                    'product_page': str(sub_cat_pagination),
                    'source_category_url': sub_cat_link,
                    'isConfigurable': json_parse(article, 'configurable'),
                    'hasVariants': json_parse(article, 'hasVariants'),
                    'custom': {
                        "highlight": json_parse(article, 'highlight'),
                        'multipleVariantsText': json_parse(article, 'multipleVariantsText')
                    },
                    'reviews_rating': article['articleRatingsTotal']['ratingsAverage'],
                    'reviews_count': article['articleRatingsTotal']['ratingsCount'],
                    'currency': article['allPrices']['displayPrice']['currency']
                }
                try:
                    price = article['allPrices']['displayPrice']['price'].replace(',', '.')
                except Exception as error:
                    show_error(error)
                    price = ''
                temp_row['price'] = price

                if not(temp_row in output_data['scraped_products_data']):
                    output_data['scraped_products_data'].append(temp_row)

            except Exception as error:
                show_error(error)

    except Exception as error:
        show_error(error)


def get_products_data(sub_cat_link, cat_index):
    try:
        print('----------------------------------')
        print("getting links of all products in this category....")
        print('getting market and category code...')
        market_cat_code = get_market_category_code(sub_cat_link)
        if len(market_cat_code) == 0:
            show_error('could not parse market and category code from this sub-category link:' + sub_cat_link, e=False)
            return

        print('getting total number of products in this sub-category...')
        total_page_and_products = get_total_pages_and_products(market_cat_code[0], market_cat_code[1])
        if len(total_page_and_products) == 0:
            show_error('failed to fetch total number of products in this sub-category.', e=False)
            return
        print('total products in this sub-category:', total_page_and_products[1])
        time.sleep(1)

        print('getting products links...')
        print('---------------------------')
        time.sleep(1)
        prev_data = len(output_data['scraped_products_data'])

        for page_number in range(1, total_page_and_products[0] + 1, 1):
            print('category:', cat_index, '/', len(sub_categories_links), ' links scraped:',
                  len(output_data['scraped_products_data']) - prev_data,
                  '/', total_page_and_products[1])

            data_link = f'https://www.hornbach.{website_version}/mvc/article/load/article-list/{website_version}/' + market_cat_code[0] + '/'
            data_link += market_cat_code[1]
            data_link += '/36/' + str(page_number) + '/sortModeDv'
            while 1:
                if threading.active_count() < number_of_threads:
                    threading.Thread(
                        target=single_get_products_data,
                        args=[data_link, sub_cat_link, page_number]
                    ).start()
                    break
                else:
                    time.sleep(0.1)

        print('waiting for all threads to join main thread....')
        for _wait_ in range(0, 5, 1):
            if threading.active_count() == 1:
                break
            else:
                time.sleep(3)
                print('current threads count:', threading.active_count())

        print('OK. Done.... category:', cat_index, '/', len(sub_categories_links), ' links scraped:',
              len(output_data['scraped_products_data']) - prev_data,
              '/', total_page_and_products[1], '(duplicates excluded.)')
        time.sleep(2)
        print('Links scraped successfully....')
        print('--------------------------------------')

    except Exception as error:
        show_error(error)


def single_get_reviews_and_detail(data):
    try:
        ps_ = get(data['product_url'])
        ps_jd = re.findall(r'window\.__ARTICLE_DETAIL_STATE__ = (.*?)\n', ps_, re.DOTALL)
        data['creation_date'] = ''
        if len(ps_jd) > 0:
            ps_jd = json.loads(ps_jd[0].strip())
            data['EAN'] = json_parse(ps_jd, 'article.eans.0.code')
            data['description'] = json_parse(ps_jd, 'article.primaryAttributes.0.value')
            if json_parse(ps_jd, 'article.marketState') == 'AVAILABLE':
                data['isAvailableInShop'] = True
            else:
                data['isAvailableInShop'] = False

            if json_parse(ps_jd, 'article.shipmentState') == 'AVAILABLE':
                data['isAvailableOnline'] = True
            else:
                data['isAvailableOnline'] = False

            data['onlineShippingCost'] = json_parse(ps_jd, 'article.shipmentCosts.price')
            data['onlineShippingLeadtime'] = json_parse(ps_jd, 'article.shipmentTime')
            data['clickCollectLeadtime'] = json_parse(ps_jd, 'article.marketTime')

            try:
                data['datasheet_urls'] = [x['url'] for x in ps_jd['article']['datasheets']]
            except:
                data['datasheet_urls'] = []

            data['custom']['warranty'] = json_parse(ps_jd, 'article.warranty')
            data['logoUrl'] = json_parse(ps_jd, 'article.brand.logoUrl')
            data['clickAndCollectState'] = json_parse(ps_jd, 'article.clickAndCollectState')
            data['clickAndCollectAvailableQuantity'] = json_parse(ps_jd, 'article.clickAndCollectAvailableQuantity')
            data['metaKeywords'] = json_parse(ps_jd, 'article.metaKeywords')
            data['deliveryTimeText'] = json_parse(ps_jd, 'article.marketAvailabilityDisplay.deliveryTimeText')
            try:
                data['confs'] = [
                    {"config": obj['title'], "sku": obj['articleCode']} for obj in ps_jd['article']['variantGroups'][0]['variants']
                ]
            except:
                data['confs'] = []
            if json_parse(ps_jd, 'article.guidingPrice') is None:
                data['specialPrice'] = None
                data['isSpecialPrice'] = False
            else:
                data['price'] = str(json_parse(ps_jd, 'article.guidingPrice.price')).replace(',', '.')
                data['specialPrice'] = str(json_parse(ps_jd, 'article.displayPrice.price')).replace(',', '.')
                data['isSpecialPrice'] = True

        else:
            data['EAN'] = ''
            data['description1'] = ''
            data['isAvailableInShop'] = None
            data['isAvailableOnline'] = None

            data['onlineShippingCost'] = ''
            data['onlineShippingLeadtime'] = ''
            data['clickCollectLeadtime'] = ''
            data['datasheet_urls'] = []
            data['custom']['warranty'] = None
            data['logoUrl'] = ''
            data['clickAndCollectState'] = ''
            data['clickAndCollectAvailableQuantity'] = ''
            data['metaKeywords'] = ''
            data['deliveryTimeText'] = ''
            data['confs'] = []
            data['specialPrice'] = None
            data['isSpecialPrice'] = None

        if len(data['confs']) > 0:
            data['isConfigurable'] = True
            data['hasVariants'] = True

        try:
            mk_jd = requests.get(f"https://www.hornbach.de/mvc/market/current-market/markets-nearby-with-availability-information/{data['sku']}").json()
            data['nearby_markets'] = [
                [obj['title'], obj['availabilityInfoText']] for obj in mk_jd
            ]
        except:
            data['nearby_markets'] = []

        # adding brand name
        data['brand'] = re_get(r'"product.brand":"(.*?)",', ps_)
        images = re.findall(r'__typename":"Image","url":"(.*?)","', ps_)
        data['img_urls'] = []
        for img in images:
            data['img_urls'].append(img)

        # adding breadcrumbs
        try:
            breadcrumbs = re.findall(r'"itemListElement":(.*?)}</script>', ps_, re.DOTALL)
            data['breadcrumbs'] = []

            if len(breadcrumbs) > 0:
                jd = json.loads(breadcrumbs[0].strip())

                for d in jd:
                    try:
                        data['breadcrumbs'].append(d['name'])
                    except Exception as error:
                        show_error(error)
                        continue

        except Exception as error:
            show_error(error)
            data['breadcrumbs'] = []

        # adding specs
        try:
            attributes = re.findall(r'"combinedAttributes":\[(.*?)],"', ps_, re.DOTALL)
            data['specs'] = {}

            if len(attributes) > 0:
                j_raw = '[' + attributes[0].strip() + ']'
                jd = json.loads(j_raw)

                for d in jd:
                    try:
                        data['specs'][d['key']] = d['value']
                    except Exception as error:
                        show_error(error)
                        continue

        except Exception as error:
            show_error(error)
            data['specs'] = {}

        # adding reviews
        try:
            data['reviews'] = []
            review_url = f'https://services.hornbach.{website_version}/articlerating-service/api/article-reviews/v2?_sortby=SUBMIT_DATE'
            review_url += '&_sortorder=DESC&articleCode=' + str(data['sku']) + '&tenant=' + website_version
            jd = json.loads(get(review_url))

            for d in jd:
                try:
                    ts = datetime.datetime.fromtimestamp(int(d['date'])/1000)
                    data['reviews'].append({
                        'review_date': f'{ts.year}-{ts.month}-{ts.day}',
                        'review_heading': d['header'],
                        'review_rating': d['value'],
                        'review_body': d['body']
                    })

                except Exception as error:
                    show_error(error)
                    continue

        except Exception as error:
            show_error(error)
            data['reviews'] = []
        # data.pop('product_url')

    except Exception as error:
        show_error(error)


def get_reviews_and_detail():
    try:
        print('getting detail from each product link....')
        time.sleep(3)
        print('------------------------------------------')

        counter = 0
        total_records = len(output_data['scraped_products_data'])
        for data in output_data['scraped_products_data']:
            counter += 1
            print('getting detail:', counter, '/', total_records)
            while 1:
                if threading.active_count() < number_of_threads:
                    threading.Thread(target=single_get_reviews_and_detail, args=[data]).start()
                    break
                else:
                    time.sleep(0.1)

        print('waiting for all threads to join main thread....')
        for _wait_ in range(0, 10, 1):
            if threading.active_count() == 1:
                break
            else:
                time.sleep(3)
                print('current threads count:', threading.active_count())

        print('all records scraped successfully...!')

    except Exception as error:
        show_error(error)


get_subcategories_links()
cat_ind = 0
for category in sub_categories_links:
    cat_ind += 1
    get_products_data(category, cat_ind)

print('----------------- now, scraping products detail -----------------')
time.sleep(3)
get_reviews_and_detail()

time.sleep(3)
print('----------------- saving data -----------------')
output_data['scrap_meta']['spider_date_end'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

fn = datetime.datetime.now().strftime('output-D(%m_%d_%Y)-T(%H_%M).json')


folder_path = os.path.join(os.path.abspath(os.getcwd()), 'scrapes')
if not(os.path.exists(folder_path)):
    os.mkdir(folder_path)

json.dump(obj=[output_data], fp=open(os.path.join(folder_path, fn), 'w', encoding='utf-8'), indent=2)


print('successfully saved to:', fn)
