# Author: dgm
# Description: 将一个mongodb的一个collection同步到另一个mongodb
# Date: 2021-10-08
import pymongo
import argparse
from tools import get_logger

# 日志
logger = get_logger(__name__, log_file=None, log_level='DEBUG')

# 参数
# 源mongodb参数
parser = argparse.ArgumentParser(description='backup_mongodb')
parser.add_argument('--source_host', type=str, default='127.0.0.1')
parser.add_argument('--source_port', type=int, default=27017)
parser.add_argument('--source_user', type=str, default=None)
parser.add_argument('--source_passwd', type=str, default=None)
parser.add_argument('--source_db', type=str, required=True)
parser.add_argument('--source_collection', type=str, required=True)
# 目标mongodb参数
parser.add_argument('--target_host', type=str, default='127.0.0.1')
parser.add_argument('--target_port', type=int, default=27017)
parser.add_argument('--target_user', type=str, default=None)
parser.add_argument('--target_passwd', type=str, default=None)
parser.add_argument('--target_db', type=str, required=True)
parser.add_argument('--target_collection', type=str, required=True)
# 每次同步的条数
parser.add_argument('--chunk', type=int, default=1000)
args = parser.parse_args()


class DataBase(object):
    def __init__(self, **kwargs):
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.user = kwargs['user']
        self.passwd = kwargs['passwd']
        self.db = kwargs['db']
        if self.user is not None:
            self.client = pymongo.MongoClient('mongodb://{user}:{passwd}@{host}:{port}/'.format(
                user=self.user, passwd=self.passwd, host=self.host, port=self.port))
        else:
            self.client = pymongo.MongoClient('mongodb://{host}:{port}/'.format(host=self.host, port=self.port))

    def read_data(self, collection, select_dict, skip=None, limit=None):
        """
        读取数据
        :param collection:
        :param select_dict:
        :return:
        """
        data = None
        try:
            if skip is not None and limit is None:
                data = self.client[self.db][collection].find(select_dict).skip(skip)
            elif skip is None and limit is not None:
                data = self.client[self.db][collection].find(select_dict).limit(limit)
            elif skip is not None and limit is not None:
                data = self.client[self.db][collection].find(select_dict).skip(skip).limit(limit)
            else:
                data = self.client[self.db][collection].find(select_dict)
            if data.count(True) == 0:
                data = None
        except Exception as ex:
            logger.error(ex)
        if self.client:
            self.client.close()
        return data

    def write2mongo(self, collection, data):
        """
        写入到mongo
        :param collection:
        :param data:
        :return:
        """
        count = data.count(True)
        i = 0
        for item in data:
            try:
                self.client[self.db][collection].insert(item)
                print("\r进度条百分比：{}%".format((i + 1) * 100 / count), end="", flush=True)
                i += 1
            except Exception as ex:
                logger.error(ex)


if __name__ == "__main__":
    # 参数
    # 源mongodb
    source_host = args.source_host
    source_port = args.source_port
    source_user = args.source_user
    source_passwd = args.source_passwd
    source_db = args.source_db
    source_collection = args.source_collection
    # 目标mongodb
    target_host = args.target_host
    target_port = args.target_port
    target_user = args.target_user
    target_passwd = args.source_passwd
    target_db = args.target_db
    target_collection = args.target_collection
    chunk = args.chunk
    # 批次读取数据备份到本地
    sdb = DataBase(host=source_host, port=source_port, user=source_user, passwd=source_passwd, db=source_db)
    tdb = DataBase(host=target_host, port=target_port, user=target_user, passwd=target_passwd, db=target_db)
    select_dict = {}
    skip = 102000
    limit = chunk
    while True:
        data = sdb.read_data(source_collection, select_dict, skip=skip, limit=limit)
        if data is None:
            logger.info('已经导入完成！')
            break
        num = data.count(True)
        tdb.write2mongo(target_collection, data)
        logger.info('已经导入了{}条'.format(str(skip+num)))
        skip += limit
    if sdb.client:
        sdb.client.close()
    if tdb.client:
        tdb.client.close()