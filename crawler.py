#!/usr/bin/python
import requests
import sys
import lxml
from bs4 import BeautifulSoup
import re
import urlparse
import pymongo
import time
from time import sleep

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

'''Start Editing from here'''
start = time.time()
url=list(sys.argv)[1]

###Settings
exclude_these=('.jpeg','.jpg','.png')##put name of extention here to exclude with prefix '.'
user_agent = {'User-agent': 'Mozilla/5.0'}

##mongodb authentication
#conn = pymongo.MongoClient('mongodb://username:password@hostname')
#db=conn.database
##mongodb authentication

##define 
unprocessedlist=list()
processedlist=list()
count=1
count2=1
avg_loop_speed=int()
##define 

###CRAWLABLE URL   
if url.endswith('/'):
  url = url[:-1]
if urlparse.urlparse(url).scheme=='':
  url='http://'+url
###CRAWLABLE URL 

url_clean=urlparse.urlparse(url).netloc
url_callable=urlparse.urlparse(url).scheme+'://'+urlparse.urlparse(url).netloc

try: 
  collection=list(sys.argv)[2]
except:
  collection=url_clean

r = requests.get(url, headers = user_agent)
html=r.text
soup = BeautifulSoup(html,"lxml")

all_url_dict=geturl(soup)
all_url_list=list(all_url_dict.values())

unprocessedlist.append(url)
unprocessedlist=unprocessedlist+all_url_list
while True:
    if avg_loop_speed>100:
        print 'you are driving very fast!! Slow down buddy'
        break
    if len(unprocessedlist) == 0:
        break
    url_new=unprocessedlist[0]
    url_new_clean=urlparse.urlparse(url_new).netloc
    
    if url_new.endswith('/'):
      url_new = url_new[:-1]
    if url_new.endswith(exclude_these):
      print url_new
      print "Hey this is a file I am skipping it!"
      try: 
        unprocessedlist.remove(url_new)
      except:
        unprocessedlist.remove(url_new+'/')
        print url_new+' is not present in this list thrown exception from inner if'
    else:
      if (url_new_clean==url_clean) or (url_new_clean=='www.'+url_clean):##Keeps in the current domain only
          if url_new not in processedlist:
              print str(count)+' Fetching...'+url_new
  
              try:
                  r = requests.get(url_new, headers = user_agent)
                  html=r.text
                  soup = BeautifulSoup(html,"lxml")
                  all_url_dict=geturl(soup)
              except ValueError:
                  print "Oops!  request loop throw this error..."
                  
              data_dump = {'url':str(url_new),'page_title':gettitle(soup),'unix_time':time.time(),'meta_description':getdescription(soup),'meta_keywords':getkeywords(soup),'body':getbodytext(soup),'phone':getphone(html),'email':getemail(html),'all_url':all_url_dict}
              
              try:
                storeData(data_dump)
              except ValueError:
                print "Opps! store funciton error"
                
              all_url_list=list(all_url_dict.values())
              unprocessedlist = list(set(unprocessedlist + all_url_list))
              processedlist.append(url_new)
              try: 
                unprocessedlist.remove(url_new)
              except:
                unprocessedlist.remove(url_new+'/')
              end = time.time()
              time_taken=end - start
              print('Time Spent: '+str(time_taken))+' Sec'
              print('Average-crawler-speed: '+str(count/time_taken)+'  Link/Sec')
              print('Average-loop-speed: '+str(count2/time_taken)+'  Loop/Sec')
              print ''''''
              print ''''''
              print ''''''
              count=count+1
          else:
              try: 
                unprocessedlist.remove(url_new)
              except:
                unprocessedlist.remove(url_new+'/')
                print url_new+' is not present in this list thrown exception from inner else'
      else:
          try:
            unprocessedlist.remove(url_new)
          except:
            unprocessedlist.remove(url_new+'/')
            print url_new+' is not present in this list thrown exception from outer else'
    
    ##Do not touch 
    end = time.time()
    avg_loop_speed=count2/time_taken
    count2+=1
    ##Do not touch 