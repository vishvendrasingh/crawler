#!/usr/bin/python
#########################################USAGE###############################################
#$Surface Crawler By Vishvendra Singh-Multiprocess approach                                 #
#$crawler_2.0.py 8 http://example.com example.com                                           #
#$crawler_2.0.py workers Full_URL mongo_collection_name                                     #
#Details - Overload is sent to thread 1, rest of the load is distributed as equal to others #
#############################################################################################
'''START IMPORT HERE'''
import multiprocessing
import requests
import urlparse
import sys
import Queue
import lxml
from bs4 import BeautifulSoup
import re
import pymongo
import time
'''END IMPORT HERE'''
'''Do Not Change Below code'''
def getdescription(soup):
  meta_desc=''
  desc = soup.findAll(attrs={"name":"description"})
  if len(desc):
    meta_desc=desc[0]['content'].encode('utf-8')
    return meta_desc
   
def getkeywords(soup):
  meta_keyword=''
  meta_key = soup.findAll(attrs={"name":"keywords"})
  if len(meta_key):
    meta_keyword=meta_key[0]['content'].encode('utf-8')
    return meta_keyword
    
def gettitle(soup):
  title=''
  for title in soup.findAll('title'):
    title=title.text
  return title
  
def getbodytext(soup):
  body=''
  for elem in soup.findAll(['script', 'style']):
    elem.extract()
  for body in soup.findAll('body'):
    body=body.text
  return " ".join(body.split())

def getphone(html):
  ph_list=[]
  str_list=re.findall("(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{5})", html)
  ph_list=str_list = filter(None, str_list)
  ph_list = filter(None, ph_list)
  return ph_list

def getemail(html):
  email_match = re.findall(r'[\w\.-]+@[\w\.-]+', html)
  return email_match

def geturl(soup):
  urldict = {}
  for link in  soup.findAll("a"):
    url_mended=urlparse.urljoin(url_callable, link.get("href"))
    url_crap = urlparse.urldefrag(url_mended)
    url_mended=url_crap[0]
    urldict[link.text]=url_mended
  return urldict

def storeData(data_dump):
  try:
    db[collection].insert(data_dump,check_keys=False)
  except ValueError:
    print "Oops!  Dict 1 of functions throw this error..."
'''Do Not Change Above code'''

def worker(i,no_of_links):
    for i in range(1,no_of_links+1):
        print ''''''
        fetched = q.get_nowait()
        print "--",fetched,"--By Thread-",i,"--"
        print "Fetching...",fetched
        r=requests.get(fetched)
        html=r.text
        soup=BeautifulSoup(html,"lxml")
        all_url_dict=geturl(soup)
        data_dump = {'url':str(fetched),'page_title':gettitle(soup),'unix_time':time.time(),'meta_description':getdescription(soup),'meta_keywords':getkeywords(soup),'body':getbodytext(soup),'phone':getphone(html),'email':getemail(html),'all_url':all_url_dict}
        try:
            storeData(data_dump)
        except ValueError:
	        print "Opps! store funciton error"
        print ''''''
    return

'''START CRAWLABLE URL   '''
url=list(sys.argv)[2]
if url.endswith('/'):
  url = url[:-1]
if urlparse.urlparse(url).scheme=='':
  url='http://'+url
url_clean=urlparse.urlparse(url).netloc
url_callable=urlparse.urlparse(url).scheme+'://'+urlparse.urlparse(url).netloc
print url_callable
'''END CRAWLABLE URL '''
##mongodb authentication
#conn = pymongo.MongoClient('mongodb://username:password@hostname')
#db=conn.database
##mongodb authentication
try: 
  collection=list(sys.argv)[3]
except:
  collection=url_clean

no_of_threads=int(list(sys.argv)[1]) #No of thread define here
q = Queue.Queue()

user_agent = {'User-agent': 'Mozilla/5.0'}
r=requests.get(url, headers = user_agent)
html=r.text
soup=BeautifulSoup(html,"lxml")

all_url_dict=geturl(soup)
all_url_list=list(all_url_dict.values())
total_links=len(all_url_list)

for item in all_url_list:
    q.put(item)
        
print "-Queue Size is - ",q.qsize()

no_of_links_to_one_thread=q.qsize()/no_of_threads
if(no_of_links_to_one_thread<1):
    no_of_links_to_one_thread=1
    
if(q.qsize()<no_of_threads):
    no_of_thread=q.qsize()
else:
    no_of_thread=no_of_threads
print no_of_thread,"--no thread "
total_left=q.qsize()

for i in range(1,no_of_thread+1):
    if(i==1 and q.qsize()>no_of_threads ):
        extra=total_links%no_of_threads
        to_send=extra+no_of_links_to_one_thread
    else:
        to_send=no_of_links_to_one_thread
    print to_send,"to send to thread",i
    t = multiprocessing.Process(target=worker,args=(i,to_send))
    t.start()
    
t.join()
print ''''''