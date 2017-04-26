intermagnet文件夹为  http://www.intermagnet.org  网站的爬虫代码。在安装了scrapy及其相关环境后，cd到intermagnet文件夹，输入scrapy crawl intermg命令启动爬虫，爬虫将下载压缩文件，之后将文件二进制编码以及文件信息存入mongodb，最后发送dict消息给rabbitmq server，对应的消息消费者为rqworker文件夹下的intermagnet_worker.py


intermagnet/intermagnet/intermagnet/spiders/intermag.py中的文件保存路径需要根据电脑保存路径修改，具体修改处为136行中的file_path = '/home/undoingfish/Documents/INTERMAGNET/'



数据存入mongoDB时没有设置数据库的用户名和密码
