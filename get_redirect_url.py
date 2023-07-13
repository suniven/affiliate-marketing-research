import time
import pandas as pd
import requests
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

proxies = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}
ua = UserAgent()
df = pd.read_csv('./data/urls-to-be-redirected/youtube.csv')
df = df.reindex(columns=df.columns.to_list() + ['redirect_urls', 'final_url'])
df[['redirect_urls', 'final_url']] = df[['redirect_urls', 'final_url']].astype('object')

# 创建锁
lock = Lock()

# 设置计时器初始值为 0
timer = 0


def process_url(i):
    global timer
    url = df.at[i, 'original_url']
    # user_agent = ua.random
    # headers = {'User-Agent': user_agent}
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
    try:
        response = requests.get(url=url, proxies=proxies, headers=headers, timeout=10)
    except:
        return

    if response.history:
        redirect_urls = []
        for redirect_cnt, history in enumerate(response.history):
            redirect_urls.append(history.url)
        df.at[i, 'redirect_urls'] = redirect_urls
    df.at[i, 'final_url'] = response.url

    print("URL index:", i)

    # 增加计时器值
    timer += 1

    # 每处理 1000条数据 保存一次 DataFrame
    if timer >= 1000:
        print('Saving DataFrame...')
        with lock:
            df.to_feather('./data/urls-with-redirection/youtube-search.feather')

        # 重置计时器
        timer = 0


# 创建线程池
executor = ThreadPoolExecutor(max_workers=12)

# 提交任务到线程池
futures = []
for i in range(len(df)):
    future = executor.submit(process_url, i)
    futures.append(future)

# 等待所有任务完成
for future in futures:
    future.result()

# 保存最终的 DataFrame
with lock:
    df.to_feather('./data/urls-with-redirection/youtube-search.feather')
