import sqlite3
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from fake_useragent import UserAgent
import pandas as pd

ua = UserAgent()
proxies = {'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'}


def save_html_to_database(index, url):
    try:
        print("Getting {0}: {1}".format(index, url))
        user_agent = ua.random
        headers = {
            'User-Agent': user_agent
        }
        response = requests.get(url=url, proxies=proxies, timeout=10)
        html = response.text
        status_code = response.status_code
        conn = sqlite3.connect('./data/html/html_data_33_substr.db')
        cursor = conn.cursor()
        cursor.execute("INSERT INTO html_data (url, status_code, html) VALUES (?, ?, ?)",
                       (url, status_code, html))
        conn.commit()
        conn.close()
    except requests.RequestException:
        # 处理请求异常
        pass


if __name__ == '__main__':
    # 创建数据库连接
    conn = sqlite3.connect('./data/html/html_data_33_substr.db')
    cursor = conn.cursor()
    # 创建表格
    cursor.execute("CREATE TABLE IF NOT EXISTS html_data (url TEXT, status_code INT,html TEXT)")
    # 关闭连接
    conn.close()

    df = pd.read_csv('./data/substring google search/results-33substr.csv')
    urls = list(set(df.link.to_list()))

    # with open('./data/search_results_link_125substr.txt', 'r') as file:
    #     urls = [line.strip() for line in file]

    # 并发访问URL并保存HTML源码到数据库
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(save_html_to_database, index, url) for index, url in enumerate(urls)]
        for future in as_completed(futures):
            # print(future)
            pass
