import csv
import pymysql
import os

import time


def today_str():
    return str(time.strftime("%Y%m%d"))


def deal_area_data(path, code):
    with open(path, newline="") as f:
        s = csv.reader(f)
        for row in s:
            # 房价
            row[4] = row[4].replace("元/m2", "")
            if row[4] == "暂无":
                row[4] = None
            # 二手房
            row[5] = row[5].split("套")[0]

            sql = "INSERT INTO xiaoqu(city, date, district, area, xiaoqu, price, sale) VALUE ('{}','{}','{}','{}','{}','{}','{}')" \
                .format(cities[code], row[0], row[1], row[2], row[3], row[4], row[5])
            myqsl_cursor.execute(sql)
        mysql_conn.commit()


if __name__ == '__main__':

    # 数据库参数
    target_db = "lianjia_house"
    target_table = "xiaoqu"
    cities = {
        'bj': '北京',
        'cd': '成都',
        'cq': '重庆',
        'cs': '长沙',
        'dg': '东莞',
        'dl': '大连',
        'fs': '佛山',
        'gz': '广州',
        'hz': '杭州',
        'hf': '合肥',
        'jn': '济南',
        'nj': '南京',
        'qd': '青岛',
        'sh': '上海',
        'sz': '深圳',
        'su': '苏州',
        'sy': '沈阳',
        'tj': '天津',
        'wh': '武汉',
        'xm': '厦门',
        'yt': '烟台',
    }
    city_code_list = ['bj', 'cd', 'cq', 'cs', 'dg', 'dl',
                      'fs', 'gz', 'hz', 'hf', 'jn', 'nj',
                      'qd', 'sh', 'sz', 'su', 'sy', 'tj',
                      'wh', 'xm', 'yt']

    # 建立数据库连接
    print("Connect to MySQL...")
    mysql_conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='mysql', db=target_db, charset='utf8')
    myqsl_cursor = mysql_conn.cursor()

    try:
        for city_code in city_code_list:
            file_dir = os.path.join("/home/nick/house/lianjia-spider/data/xiaoqu/", city_code, today_str())
            file_list = os.listdir(file_dir)

            for filename in file_list:
                file_path = os.path.join(file_dir, filename)
                print(file_path)
                deal_area_data(file_path, city_code)
    finally:
        myqsl_cursor.close()
        mysql_conn.close()
    print("Close MySQL connection...")
