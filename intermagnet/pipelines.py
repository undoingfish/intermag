# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import urllib
import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline

from intermagnet import settings

class IntermagnetPipeline(FilesPipline):
    def process_item(self, item, spider):
		for download_link in item['download_link']:
            yield scrapy.Request(download_link)

	def item_completed(self, results, item, info):
    	file_paths = [x['path'] for ok, x in results if ok]
    	if not file_paths:
        	raise DropItem("Item contains no files")
    	item['file_paths'] = file_paths
    	return item
