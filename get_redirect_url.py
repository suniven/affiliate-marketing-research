import pandas as pd
import requests
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor

proxies = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}
ua = UserAgent()
df = pd.read_feather('./data/urls in webpage/urls-in-html-unique-filtered.feather')


# aff = list(set(df[df['is_affiliate'] == 1].url.to_list()))
# df_aff = pd.DataFrame(data=aff, columns=['url'])


def process_url(i):
    url = df.at[i, 'url']
    user_agent = ua.random
    headers = {'User-Agent': user_agent}
    try:
        response = requests.get(url=url, proxies=proxies, timeout=10)
    except:
        return
    if response.history:
        for redirect_cnt, history in enumerate(response.history):
            column_name = 'redirect_' + str(redirect_cnt + 1)
            df.at[i, column_name] = history.url
    df.at[i, 'final_page'] = response.url

    print("URL index:", i)


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

df.to_feather('./data/urls in webpage/urls-in-html-uf-with-redirection.feather', index=False)

# import pandas as pd
# import requests

# from fake_useragent import UserAgent
# import asyncio

# proxies = {
#     'http': 'http://127.0.0.1:10809',
#     'https': 'http://127.0.0.1:10809',
# }
# ua = UserAgent()
# df = pd.read_feather(
#     './data/urls in webpage/urls-in-text-125substr-labeled.feather')
# aff = list(set(df[df['is_affiliate'] == 1].url.to_list()))
# df_aff = pd.DataFrame(data=aff, columns=['url'])


# def get_next_url(url: str) -> str:
#     user_agent = ua.random
#     headers = {'User-Agent': user_agent}
#     try:
#         response = requests.get(url=url, proxies=proxies,
#                                 headers=headers, timeout=10)
#         return response.url
#     except:
#         return ""


# async def process_url(i, semaphore):
#     async with semaphore:
#         pre = df_aff.at[i, 'url']
#         redirect_cnt = 0
#         res = get_next_url(pre)
#         while res:
#             if res == pre:  # 已经到达 final page
#                 df_aff.at[i, 'final_page'] = res
#                 break
#             else:
#                 redirect_cnt += 1
#                 column_name = 'redirect_' + str(redirect_cnt)
#                 df_aff.at[i, column_name] = res
#                 pre = res
#                 res = get_next_url(pre)

#         # 输出处理结果
#         print("URL index:", i)

# # 创建事件循环
# loop = asyncio.get_event_loop()

# # 创建协程任务列表
# tasks = []

# # 创建信号量以限制最大并发协程数量
# semaphore = asyncio.Semaphore(1)

# # 协程处理每个URL
# for i in range(len(df_aff)):
#     task = asyncio.ensure_future(process_url(i, semaphore))
#     tasks.append(task)

# # 执行协程任务
# loop.run_until_complete(asyncio.wait(tasks))

# # 关闭事件循环
# loop.close()

# df_aff.to_csv(
#     './data/urls in webpage/affiliate-urls-with-redirection-125substr.csv', index=False)
