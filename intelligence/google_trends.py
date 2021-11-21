from datetime import datetime, timedelta, date, time
import pandas as pd
import time

from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError


def show_keywords_trends(keywords, timeframe, show_totals = True, category=0, geo='', gprop='' ): #max 5 tokens
  """
  “timeframe=’today 5-y'” means 5 years from today,
  “geo” attribute is for determining the geography for the data which will be pulled. https://www.iban.com/country-codes
  “group” stands for Google Property: images, news, youtube or froogle (for Google Shopping results)
  “categories” at https://github.com/pat310/google-trends-api/wiki/Google-Trends-Categories
  """
  trends = TrendReq ()

  trends.build_payload(
                        kw_list=keywords,
                        cat=category,
                        timeframe=timeframe, 
                        geo=geo, 
                        gprop=gprop
                      )
  interest_over_time = trends.interest_over_time()
  interest_over_time= interest_over_time.drop(labels=['isPartial'],axis='columns')

  if (show_totals):
    interest_over_time['overall']= interest_over_time.sum(axis = 1, skipna = True)
  
  image = interest_over_time.plot(title = f'Timeframe ({timeframe}) on Google Trends ', figsize=(20,8)).get_figure()

  return interest_over_time


def getTimeFrame(start_date, end_date):  
  start_d = datetime.strptime(start_date, '%Y-%m-%d')
  init_end_d = end_d = datetime.strptime(end_date, '%Y-%m-%d')

  init_end_d.replace(hour=23, minute=59, second=59)   
  delta=end_d-start_d
  itr_d = end_d - delta
  return itr_d.strftime('%Y-%m-%d')+' '+end_d.strftime('%Y-%m-%d')
