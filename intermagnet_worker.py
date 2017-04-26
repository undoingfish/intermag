#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pika
import time
import json
import zipfile
import gzip
import zlib
import shutil
import os
import pymongo
import hashlib
from pymongo import MongoClient

class IntermagWorker():

    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))
        
        channel = connection.channel()
        result = channel.queue_declare(queue='filename_queue', durable=True)
        self.callback_queue = result.method.queue
        print ' [*] Waiting for messages. To exit press CTRL+C'
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(self.on_response,
                              queue=self.callback_queue)
        #time.sleep(3)
        channel.start_consuming()

    def produce_message(self, wrong_file):#将解压、解析错误的文件信息发送给错误处理队列
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        channel.queue_declare(queue='error_queue', durable=True)

        message = json.dumps(wrong_file)
        channel.basic_publish(
            exchange='', routing_key='error_queue',
            body=message,
            properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
            ))
        print "============================================"
        print " [x] Sent %r" % (message,)
        print "============================================"
        connection.close()

    def decompress_zip(self, file_path, file_name):#解压缩.zip文件
        filepath = file_path
        filename = file_name
        file_read_path = ''
        save_path = filepath[:-4]+'/'
        try:
            zip_file = zipfile.ZipFile(filepath)
            print save_path
            if os.path.isdir(save_path): #判断文件夹是否已经存在
                pass
            else:
                os.mkdir(save_path)#创建文件夹
                for names in zip_file.namelist():
                    zip_file.extract(names,save_path)
                    if names[-4:]=='.txt':
                        os.remove(save_path+names)
                        print names + ' has been removed'
                zip_file.close()
            print 'file decompressed in ' + save_path
            os.remove(filepath)
            print filename + ' has been removed'
        except (zipfile.BadZipfile, zlib.error)  as e:
            print 'file %s decompressing failed...'%save_path
            if os.path.exists(save_path):
                shutil.rmtree(save_path)
            #os.remove(filepath)
            print filepath
            return filename
        return save_path

    def confirm_exist(self, filename, date):#查看文件是否存在
        client = MongoClient('localhost', 27017)
        db = client.files
        coll = db[date]
        if coll.find_one({'file_name':filename}):
            return True
        else:
            return False

    def store_into_mongoDB(self, file_info):#将文件存入数据库（未解析），直接按照日期创建集合名称
        try:
            date = file_info['source_date']
            client = MongoClient('localhost', 27017)
            db = client.files
            coll = db[date]
            print 'Connect to files in mongoDB...'
            content = file_info['file_content'].decode("unicode_escape")
            coll.save(dict(
                rate = file_info['rate'],
                format = file_info['format'],
                site = file_info['site'],
                file_name = file_info['file_name'],
                sha1 = file_info['sha1'],
                source_date = file_info['source_date'],
                available = file_info['available'],
                type = file_info['type'],
                file_size = file_info['file_size'],
                file_content = content,
                obs = file_info['obs'],
                create_date = file_info['create_date'],
                region = file_info['region'],
                latitude = file_info['latitude'],
                ))
            print 'File saved successfully......'
        except pymongo.errors.ConnectionFailure, e:
            print 'File saved failed......'
        client.close()
        return

    def read_context(self, file_path):#读取文件内容
        file = open(file_path)
        context = file.read()
        return context

    def compute_SHA1(self, filename):#计算文件校验值
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

    def decompress_gz(self, filepath):#解压缩.gz文件
        save_path = filepath
        print 'filepath: '+filepath
        print 'save_path: '+save_path
        filename = save_path.replace(".gz", "")
        print 'after: '+filename
        g_file = gzip.GzipFile(save_path)
        print save_path
        open(filename, "wb").write(g_file.read())
        g_file.close()
        os.remove(save_path)
        print filepath
        print " has been removed....."
        return filename

    def combine_info(self, res, decompress_path, format, rate, region, latitude):#合并存取的数据信息
        file_info = {
            'rate':rate,#
            'format':format,#
            'site':'',#
            'file_name':'',#
            'sha1':'',#
            'source_date':'',#
            'available':'',#
            'type':'',#
            'file_size':'',#
            'file_content':'',#
            'obs':'',#
            'create_date':'',
            'region':region,#~
            'latitude':latitude,#~
            }
        """
        print '========================================'
        print res
        print '========================================'
        """
        for each_record in res:
            if res[each_record]['0']['available']:
                name = res[each_record]['0']['filename']
                file_path = ''
                temp = ''
                if os.path.exists(decompress_path+name):
                    if name[-3:]=='.gz':
                        print type(decompress_path)
                        print temp
                        if not os.path.exists(decompress_path+name[:-3]):
                            #如果解压.gz文件出错，在此添加try catch，并将each_record发送给错误处理的消息队列
                            try:
                                file_path = self.decompress_gz(decompress_path+name)
                                name = name[:-3]
                            except IOError as e:
                                self.produce_message(res[each_record])
                                continue
                        else:
                            file_path = decompress_path + name[:-3]
                            print 'file exists......'
                    else:
                        file_path = decompress_path + name
                    statinfo = os.stat(file_path)
                    create_time = statinfo.st_mtime
                    timeArray = time.localtime(create_time)
                    create_date = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                    file_info['site'] = res[each_record]['name']
                    file_info['obs'] = res[each_record]['0']['obs']
                    file_info['source_date'] = res[each_record]['0']['date']
                    file_info['available'] = res[each_record]['0']['available']
                    if res[each_record]['0'].has_key('type'):
                        file_info['type'] = res[each_record]['0']['type']
                    print res[each_record]['0']['date']
                    print res[each_record]['0']
                    print file_path
                    print name
                    file_info['file_name'] = name
                    file_info['file_content'] = self.read_context(file_path)
                    file_info['sha1'] = self.compute_SHA1(file_path)
                    file_info['file_size'] = os.path.getsize(file_path)
                    file_info['create_date'] = create_date
                    if self.confirm_exist(name, file_info['source_date']):
                        print 'file %s already exists in mongoDB...' %name
                    else:
                        self.store_into_mongoDB(file_info)
                else:
                    print 'file %s does not exist...' %name
                    #由于爬取文件可能漏掉某个文件，因此可以在此添加代码，将文件信息发送给错误处理的消息队列，让爬虫根据文件信息重新爬取
                    message = json.dumps(res[each_record])
                    self.produce_message(message)
                    continue

    def save_files(self, document):#将.zip文件从数据库中写到本地文件夹
        res = document
        container_path = '/home/zengfeng/zip_temp/'
        save_path = container_path + res['filename']
        if not os.path.exists(save_path):
            with open(save_path, 'wb') as f:
                f.write(res['file_content'])
            print 'fetch succes.....'
            print 'saved as %s' %save_path
        else:
            print 'file has been fetched out....'
        return save_path

    def operation(self, filename):
        client = MongoClient('localhost', 27017)
        db = client.intermag
        coll = db.intermag
        document = coll.find_one({"filename":filename})
        file_name = document['filename']
        format = document['format']
        rate = document['rate']
        region = document['region']
        latitude = document['latitude']
        content = document['content']
        file_path = self.save_files(document)
        decompress = self.decompress_zip(file_path, file_name)
        if len(decompress)>30:
            res = json.loads(content)
            self.combine_info(res, decompress, format, rate, region, latitude)
            if os.path.exists(decompress):
                shutil.rmtree(decompress)
        else:
            #解压失败，将文件信息发送给错误处理的消息队列，让爬虫重新爬取
            self.produce_message(filename)
        #print type(document)

    def on_response(self, ch, method, properties, body):
        content = json.loads(body)
        print type(content)
        filename = content['filename']
        print(" [x] Received %r" %content)
        print filename
        self.operation(filename)
        #print 'analysed_data:   '
        #print analysed_data.keys()
        #print type(analysed_data)
        #save_analysed_data.save_data(analysed_data)
        
        ch.basic_ack(delivery_tag = method.delivery_tag)

if __name__ == "__main__":
    intermag = IntermagWorker()

