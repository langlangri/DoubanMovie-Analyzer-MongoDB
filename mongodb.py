from datetime import time
from random import random

import pymongo
import pandas as pd
import requests
from lxml import etree
from matplotlib import pyplot as plt


plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置中文


def get_html(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                      " (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0"
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None


def parse_html(html):
    if html is None:
        return []

    doc = etree.HTML(html)
    out_list = []
    for row in doc.xpath('//div[@class="grid-16-8 clearfix"]/div[@class="article"]/ol[@class="grid_view"]/li'):
        rank = row.xpath('.//div[@class="pic"]/em/text()')[0].strip()
        title = row.xpath('.//div[@class="info"]/div[@class="hd"]/a/span[@class="title"]/text()')[0].strip()
        director_info = row.xpath('.//div[@class="bd"]/p/text()')[0].strip().split('\n')[0]
        director = director_info.split(' ')[1] if len(director_info.split(' ')) > 1 else ''
        comment = row.xpath('.//div[@class="bd"]/div[@class="star"]/span[@class="rating_num"]/text()')[0].strip()
        comment_quantity = row.xpath('.//div[@class="bd"]/div[@class="star"]/span[4]/text()')[0].strip().replace(
            "人评价", "")
        out_list.append([rank, title, director, comment, comment_quantity])
    return out_list


def pre_processing(data):
    df = pd.DataFrame(data, columns=['排名', '电影名', '导演', '评分', '参评人数'])
    print("数据详情:")
    print(df.describe())
    print("空值统计、删除重复数据:")
    print("空值统计:\n" + str(df.isnull().sum()))
    print("重复数据共：" + str(df.duplicated().sum()) + "个")
    print("===========================================================")
    return df.values.tolist()


class Database:
    def __init__(self):
        self.client = pymongo.MongoClient(host='localhost', port=27017)

    def getDBs(self, database, flag):
        dbs = self.client.list_database_names()
        print("MongoDB中所有的数据库:")
        for db in dbs:
            print(db)
        if database in dbs and flag == 0:
            print(database + " 数据库已存在...")
            self.delDBs(database)

    def delDBs(self, db_exit):
        self.client.drop_database(db_exit)
        print("数据库删除成功...")

    def create(self, lists, db_name, coll_name):
        db = self.client[db_name]
        collection = db[coll_name]
        documents = [
            {
                "rank": item[0],
                "file_name": item[1],
                "author": item[2],
                "grade": float(item[3]),
                "numbers": item[4]
            } for item in lists
        ]
        collection.insert_many(documents)
        print("已成功插入多条数据...")
        return [db, collection]

    def check(self, coll):
        print("查看集合中的文档：(5条)")
        for document in coll.find().limit(5):
            print(document)
        print("查看电影评分大于9.5的文档：")
        for good_doc in coll.find({"grade": {"$gt": 9.5}}):
            print(good_doc)


class MySQLDatabase:
    def __init__(self, host, user, password, database, mysql=None):
        try:
            self.connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            if self.connection.is_connected():
                print("成功连接到 MySQL 数据库")
        except Exception as e:
            print(f"错误连接到MySQL: {e}")

    def create_table(self, table_name):
        cursor = self.connection.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            rank INT PRIMARY KEY,
            title VARCHAR(255),
            director VARCHAR(255),
            grade FLOAT,
            comment_quantity INT
        )
        """
        try:
            cursor.execute(create_table_query)
            self.connection.commit()
            print(f"创建表 {table_name} 成功")
        except Exception as e:
            print(f"创建表时出错: {e}")
        finally:
            cursor.close()

    def insert_data(self, table_name, data):
        cursor = self.connection.cursor()
        insert_query = f"""
        INSERT INTO {table_name} (rank, title, director, grade, comment_quantity)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        title=VALUES(title), director=VALUES(director), grade=VALUES(grade), comment_quantity=VALUES(comment_quantity)
        """
        try:
            cursor.executemany(insert_query, data)
            self.connection.commit()
            print(f"插入数据到 {table_name} 表成功")
        except Exception as e:
            print(f"插入数据时出错: {e}")
        finally:
            cursor.close()

    def close(self):
        if self.connection.is_connected():
            self.connection.close()
            print("关闭连接")


def plot_rating_pie_chart(df):
    bins = [8.5, 8.7, 8.9, 9.1, 9.3, 9.5, 9.7]
    labels = ['8.5-8.7', '8.7-8.9', '8.9-9.1', '9.1-9.3', '9.3-9.5', '9.5-9.7']
    df['评分'] = pd.to_numeric(df['评分'], errors='coerce')
    df['评分区间'] = pd.cut(df['评分'], bins=bins, labels=labels, include_lowest=True)
    rating_counts = df['评分区间'].value_counts().sort_index()
    plt.figure(figsize=(8, 8))
    plt.pie(rating_counts, labels=rating_counts.index, autopct='%1.1f%%', startangle=140)
    plt.title('豆瓣Top250 电影评分区间分布')
    plt.axis('equal')
    plt.show()


if __name__ == '__main__':
    out_lists = []
    for page in range(0, 225, 25):
        url = f"https://movie.douban.com/top250?start={page}&filter="
        html = get_html(url)
        if html:
            out_list = parse_html(html)
            out_lists.extend(out_list)
            time.sleep(random.uniform(1, 3))  # 随机等待1到3秒
        else:
            print(f"Failed to fetch data from {url}")
            continue

    print("解析后的数据：")
    print(out_lists)
    print("=============================================")
    clean_list = pre_processing(out_lists)

    # 创建DataFrame并调用绘图函数
    df = pd.DataFrame(clean_list, columns=['排名', '电影名', '导演', '评分', '参评人数'])
    plot_rating_pie_chart(df)

    # MongoDB 操作
    mongo_db = Database()
    mongo_db.getDBs("big_homework", 0)
    info = mongo_db.create(clean_list, "big_homework", "my_collection")
    mongo_db.check(info[1])
    re_name = "my_data"
    mongo_db.modi(info[1], re_name)
    mongo_db.del_doc(info[1])
    mongo_db.del_coll(info[1])
    mongo_db.getDBs("big_homework", 1)

    # MySQL 操作
    mysql_db = MySQLDatabase(host='localhost', user='root', password='123456', database='douban_movies')
    mysql_db.create_table('top250_movies')
    mysql_db.insert_data('top250_movies', clean_list)
    mysql_db.close()