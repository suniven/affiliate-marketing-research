import sqlite3
import re
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

if __name__ == '__main__':
    # 连接到数据库
    conn = sqlite3.connect('./data/html/html_data_27_substr.db')
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM html_data WHERE status_code = 200")
    results = cursor.fetchall()
    conn.close()

    url_regex = r'https?://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]'
    # 定义查询语句
    query = "SELECT html FROM html_data WHERE url = ?"
    data = []
    for row in tqdm(results):
        link = row[0]
        # print(link)
        # 连接到数据库
        conn = sqlite3.connect('./data/html/html_data_27_substr.db')
        cursor = conn.cursor()

        # 执行查询
        cursor.execute(query, (link,))
        result = cursor.fetchall()
        conn.close()

        html = result[0][0]
        soup = BeautifulSoup(html, "html.parser")
        text = soup.text
        extracted_urls = re.findall(url_regex, text, re.MULTILINE)

        # 将提取到的URL添加到列表中
        for url in extracted_urls:
            data.append({'page': link, 'url': url})

    # 将数据列表转换为DataFrame
    df = pd.DataFrame(data)
    df.to_feather('./data/urls in webpage/urls-in-text-27substr.feather')
