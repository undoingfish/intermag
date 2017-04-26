# -*- coding: utf-8 -*-
from intermagnet.items import IntermagnetItem
from scrapy.http import Request, FormRequest
from pymongo import MongoClient
import scrapy
import socket
import re
import requests
import json
import bson.binary
import time
import datetime
import urllib2
import hashlib
import pymongo
import urllib
import sys
import os
import pika

class IntermagnetSpider(scrapy.Spider):
    name = "intermag"
    allowed_domains = ["intermagnet.org"]
    start_urls = (
        'http://www.intermagnet.org/apps/download/php/ajax/search_catalogue.php',
    )

    rate = ''#'minute','second'
    data_type = ''#'variation','provisional','quasi-definitive','definitive','best'
    data_format = 'IAGA2002'
    year = ''
    month = ''
    day = ''
    region = 'America,Asia,Europe,Pacific,Africa'#'America,Asia,Europe,Pacific,Africa'
    latid = 'NH,NM,E,SM,SH'#'NH,NM,E,SM,SH'
    end_date = datetime.date(2011,1,1)#结束日期的后一天
    start_date = datetime.date(2010,1,1)

    def produce_message(self, file_info):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))#更改ip地址处1
        channel = connection.channel()

        channel.queue_declare(queue='filename_queue', durable=True)

        message = json.dumps(file_info)
        channel.basic_publish(
            exchange='', routing_key='filename_queue',
            body=message,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
            ))
        print "============================================"
        print " [x] Sent %r" % (message,)
        print "============================================"
        time.sleep(5)
        connection.close()


    def generate_date(self,r,count):
        '''生成日期列表'''
        scope = count
        total_days=scope
        dateList = []
        while scope != 0:    #开始日期到结束日期的遍历列表
            date = self.end_date - datetime.timedelta(days=scope)
            scope = scope - 1
            dateList.append(str(date))
        if r<=total_days:
            temp = dateList[r].split("-",2)
            self.year = temp[0]
            self.month = temp[1]
            self.day = temp[2]
            print "开始查询"+self.year+"-"+self.month+"-"+self.day+"的数据..........\n"

    def report(self, counter, blockSize, totalSize):
        percent = int(counter*blockSize*100/totalSize)
        sys.stdout.write("\r%d%%" % percent + ' complete')
        sys.stdout.flush()

    def start_query(self):	#向服务器提交查询请求
        data_for_query = {
            'rate':self.rate,
            'type':self.data_type,
            'format':self.data_format,
            'from_year':self.year,
            'from_month':self.month,
            'from_day':self.day,
            'to_year':self.year,
            'to_month':self.month,
            'to_day':self.day,
            'region':self.region,#'America,Asia,Europe,Pacific,Africa',
            'latid':self.latid,#'NH,NM,E,SM,SH'
            }
        query_url="http://www.intermagnet.org/apps/download/php/ajax/search_catalogue.php"
        try:
            r = requests.post (query_url, data = data_for_query, timeout = 5)
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ConnectTimeout) as e:
            r = requests.post (query_url, data = data_for_query, timeout = 5)
        '''
        try:
            socket.setdefaulttimeout(5)
            r = requests.post (query_url, data = data_for_query)
        except (requests.exceptions.ConnectionError, socket.timeout) as e:
            print 'request time out, begin another request....'
            r = requests.post (query_url, data = data_for_query)
        '''
        return r.content

    def get_file_info(self,content):
        res = content
        #print type(res)
        file_info = {
            'file_name':'',
            'rate':self.rate,
            'type':self.data_type,
            'format':self.data_format,
            'region':self.region,
            'latitude':self.latid,
            'date':self.year+'-'+self.month+'-'+self.day,
            'content':res
            }
        #print '======================================='
        #print file_info
        #print '======================================='
        return file_info

    def get_file_string(self, file_path):
        f=open(file_path,'rb')
        f.seek(0,0)    
        index=0
        file_string=f.read()
        #print "%3s" % file_string.encode('hex')
        #print '==================================='
        f.close()
        #os.remove(file_path)
        #print 'file has been deleted...'
        return file_string

    def download_file(self,url):
        file_name = url[-22:]
        file_path = '/home/zengfeng/Documents/crawl_data/'
        #file_path = '/home/undoingfish/Documents/INTERMAGNET/sec/'
        save_path = file_path + file_name
        print "file save path:"
        print save_path
        print "==============download begin=================="
        sys.stdout.write('\rFetching ' + file_name + '...\n')
        print "\n第一遍写入......."
        try:
            urllib.urlretrieve(url, save_path, reporthook=self.report)
            while os.path.getsize(save_path)<500:
                print "\n再次写入......."
                urllib.urlretrieve(url, save_path, reporthook=self.report)
            sys.stdout.flush()
        except (urllib2.URLError, IOError) as e:
            while os.path.getsize(save_path)<500:
                print "\n再次写入......."
                urllib.urlretrieve(url, save_path, reporthook=self.report)
            sys.stdout.flush()
        sys.stdout.write("\rDownload complete, saved as %s" % (file_name) + '\n\n')
        #time.sleep(2)
        #print "--------------sleep 2 seconds----------------"
        return save_path

    def get_download_link(self, url, data_for_download):
        re1='(\\/apps\\/download\\/products)'	# Unix Path 1
        re2='(\\/)'	# Any Single Character 1
        re3='(\\/)'	# Any Single Character 2
        re4='(data)'	# Word 1
        re5='(\\d+)'	# Integer Number 1
        re6='(\\.)'	# Any Single Character 3
        re7='(zip)'	# Word 2
        rg = re.compile(re1+re2+re3+re4+re5+re6+re7,re.IGNORECASE|re.DOTALL)
        url_header = 'http://www.intermagnet.org'
        try:
            r1 = requests.post(url, data = data_for_download)
        except requests.exceptions.ConnectionError as e:
            r1 = requests.post(url, data = data_for_download)
        #file_size = r1.xpath('//*[@id="wb-main-in"]/ul/li[1]/a').extract()
        #print 'file_size  ',file_size
        m = rg.search(r1.content)
        if m:
            unixpath1=m.group(1)
            c1=m.group(2)
            c2=m.group(3)
            word1=m.group(4)
            int1=m.group(5)
            c3=m.group(6)
            word2=m.group(7)
            temp_url = unixpath1+c1+c2+word1+int1+c3+word2
        final_url = url_header+temp_url
        return final_url

    def compute_SHA1(self, filename):
        file=filename
        fd = open(file)
        fd.seek(0)
        line = fd.readline()

        sha1 = hashlib.sha1()
        sha1.update(line)

        while line:
            line = fd.readline()
            sha1.update(line)

        fsha1 = sha1.hexdigest()
        #print fsha1
        return fsha1

    def store_into_mongodb(self, file_info, file_string,
                           sha1, file_size, create_date):
        client = MongoClient('localhost', 27017)
        db = client.intermag
        coll = db.intermag
        file_string =  bson.binary.Binary(file_string)
        coll.save(dict(
            rate = file_info['rate'],
            type = file_info['type'],
            format = file_info['format'],
            filename = file_info['file_name'],
            SHA1 = sha1,
            file_size = file_size,
            create_date = create_date,
            region = file_info['region'],
            latitude = file_info['latitude'],
            date = file_info['date'],
            file_content = file_string,
            content = file_info['content']
            ))
        print "File information saved successfully"
        return

    def get_name_in_mongodb(self, res):
        message = {
            'database':'intermag',
            'collection':'intermag',
            'filename':''
            }
        client = MongoClient('localhost', 27017)
        db = client.intermag
        coll = db.intermag
        name = coll.find_one({"content":res})
        message['filename'] = name['filename']
        #print message
        return message

    def confirm_exist(self, res):
        client = MongoClient('localhost', 27017)
        db = client.intermag
        coll = db.intermag
        result = coll.find_one({"content":res})
        return result

    def get_names(self, content):
        file_names = ''
        for each_content in content:
            if content[each_content]['0']['available']:
                file_names = file_names + ',' + content[each_content]['0']['filename']
        file_names = file_names.lstrip(',')
        #print file_names
        return file_names

    def final_parse(self, fileinfo, filename):
        file_info = fileinfo
        file_name = filename
        file_path = '/home/zengfeng/Documents/crawl_data/'
        file_url="http://www.intermagnet.org/data-donnee/download-2-eng.php"
        data_for_download = {
            'rate':self.rate,
            'type':self.data_type,
            'format':self.data_format,
            'from_year':self.year,
            'from_month':self.month,
            'from_day':self.day,
            'to_year':self.year,
            'to_month':self.month,
            'to_day':self.day,
            'filter_region':self.region,#'America,Asia,Europe,Pacific,Africa',
            'filter_lat':self.latid,#'NH,NM,E,SM,SH'
            'select':file_name,
            'email':'968754321@qq.com',
            'accept':'accept',
            }
        try:
            download_link = self.get_download_link(file_url, data_for_download)
            download_name = download_link[-22:]
            file_info['file_name'] = download_name
            print download_link
            socket.setdefaulttimeout(60)
            save_path = self.download_file(download_link)
        except socket.timeout as e:
            print "=======download time out, begin another request======"
            os.remove(save_path)
            download_link = self.get_download_link(file_url, data_for_download)
            download_name = download_link[-22:]
            file_info['download_name'] = download_name
            print download_link
            self.download_file(download_link)
        #print '==============file string=================='
        file_string = self.get_file_string(file_path+download_name)
        #print '==============string ends=================='
        file_size = os.path.getsize(file_path+download_name)
        statinfo = os.stat(file_path+download_name)
        create_time = statinfo.st_mtime
        timeArray = time.localtime(create_time)
        create_date = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        print "Current state............"
        print create_date,file_size
        sha1 = self.compute_SHA1(file_path+download_name)
        #print '================file stored into mongodb============='
        self.store_into_mongodb(file_info, file_string, sha1, file_size, create_date)
        #print '================store finished================'
        '''print "%3s" % file_string.encode('hex')
        with open(download_name, "wb") as code:
            code.write(file_string)
        print '==================================='
        '''
        #print file_info
        #os.remove(file_path+download_name)
        #print 'file has been removed......'
        print '==================================='
        #self.producer(file_info)

    def generate_search_condition(self):
        types = ['variation','provisional','quasi-definitive','definitive','best']
        for each_type in types:
            self.data_type = each_type
            res = self.start_query()    #type(res) = str
            content = json.loads(res)   #type(content) = dict{}
            print 'search date: '
            print self.year + '-' + self.month + '-' + self.day
            if not content.has_key('error'):
                selected_name = self.get_names(content)
                file_info = self.get_file_info(res)
                result = self.confirm_exist(res)
                if not result:
                    print "File does not exist, begin to download..."
                    #print "file names:"
                    #print selected_name
                    self.final_parse(file_info, selected_name)
                    send_message = self.get_name_in_mongodb(res)
                    self.produce_message(send_message)
                else:
                    print "File exists in MongoDB, send message to analysis module...."
                    send_message = self.get_name_in_mongodb(res)
                    self.produce_message(send_message)
                    time.sleep(1)
                    continue
            else:
                print "No result was found for the query, start another query..."

    def crawl_with_minute(self):
        self.rate = 'minute'
        print "---------crawl data format in minute--------"
        try:
            self.generate_search_condition()
        except Exception, e:
            self.generate_search_condition()

    def crawl_with_second(self):
        self.rate = 'second'
        print "---------crawl data format in second--------"
        try:
            self.generate_search_condition()
        except Exception, e:
            self.generate_search_condition()
    
    def parse(self, response):
        count = int((self.end_date-self.start_date).days)
        range = 0
        while(range<=count):
            self.generate_date(range,count)
            self.crawl_with_minute()
            self.crawl_with_second()
            range = range + 1