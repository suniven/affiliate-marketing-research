import sqlite3
import re
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

if __name__ == '__main__':
    # 连接到数据库
    conn = sqlite3.connect('./data/html/html_data_33_substr.db')
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM html_data WHERE status_code = 200")
    results = cursor.fetchall()
    conn.close()

    url_regex = r'https?:\/\/[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]'
    query = "SELECT html FROM html_data WHERE url = ?"
    data = []
    for row in tqdm(results):
        link = row[0]
        # 连接到数据库
        conn = sqlite3.connect('./data/html/html_data_33_substr.db')
        cursor = conn.cursor()
        # 执行查询
        cursor.execute(query, (link, ))
        result = cursor.fetchall()
        conn.close()

        html = result[0][0]
        extracted_urls = re.findall(url_regex, html, re.MULTILINE)

        # 将提取到的URL添加到列表中
        for url in extracted_urls:
            data.append({'page': link, 'url': url})
        # break

    # 将数据列表转换为DataFrame
    df = pd.DataFrame(data)
    df.to_feather('./data/urls in webpage/urls-in-html-33substr.feather')
