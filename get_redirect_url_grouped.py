import pandas as pd
import requests
from fake_useragent import UserAgent
from concurrent.futures import ProcessPoolExecutor
import math

proxies = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}
ua = UserAgent()
df = pd.read_csv('./data/urls-to-be-redirected/youtube.csv')

# 将DataFrame划分为每10000条URL一组
num_groups = math.ceil(len(df) / 10000)
url_groups = [df[i * 10000:(i + 1) * 10000] for i in range(num_groups)]
start_indices = [i * 10000 for i in range(num_groups)]
end_indices = [min((i + 1) * 10000, len(df)) for i in range(num_groups)]


def process_url_group(df_group, start_index, end_index):
    df_group = df_group.reindex(columns=df_group.columns.to_list() + ['redirect_urls', 'final_url'])
    df_group.redirect_urls = None
    for i in range(len(df_group)):
        url = df_group.at[i, 'original_url']
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        try:
            response = requests.get(url=url, proxies=proxies, headers=headers, timeout=10)
        except:
            continue
        if response.history:
            redirect_urls = []
            for redirect_cnt, history in enumerate(response.history):
                redirect_urls.append(history.url)
            df_group.at[i, 'redirect_urls'] = redirect_urls
        df_group.at[i, 'final_page'] = response.url

        print("URL index:", start_index + i)

    # 将处理后的结果写入文件
    file_name = f'./data/urls-with-redirection/youtube-search-{start_index}_{end_index}.csv'
    df_group.to_csv(file_name, index=False)


# 创建进程池
executor = ProcessPoolExecutor(max_workers=8)

# 提交任务到进程池
results = executor.map(process_url_group, url_groups, start_indices, end_indices)

# 检查是否有任何错误或异常
for result in results:
    try:
        result.result()
    except Exception as e:
        print(f"Error processing URL group: {str(e)}")

# import pandas as pd
# import requests
# from fake_useragent import UserAgent
# from concurrent.futures import ThreadPoolExecutor
# import math
#
# proxies = {
#     'http': 'http://127.0.0.1:7890',
#     'https': 'http://127.0.0.1:7890',
# }
# ua = UserAgent()
# df = pd.read_csv('./data/urls-to-be-redirected/youtube.csv')
#
# # 将DataFrame划分为每10000条URL一组
# num_groups = math.ceil(len(df) / 10000)
# url_groups = [df[i * 10000:(i + 1) * 10000] for i in range(num_groups)]
#
#
# def process_url_group(df_group, start_index, end_index):
#     df_group = df_group.reindex(columns=df_group.columns.to_list() + ['redirect_urls', 'final_url'])
#     df_group.redirect_urls = None
#     for i in range(len(df_group)):
#         url = df_group.at[i, 'original_url']
#         user_agent = ua.random
#         headers = {'User-Agent': user_agent}
#         try:
#             response = requests.get(url=url, proxies=proxies, timeout=10)
#         except:
#             continue
#         if response.history:
#             redirect_urls = []
#             for redirect_cnt, history in enumerate(response.history):
#                 redirect_urls.append(history.url)
#                 # column_name = 'redirect_' + str(redirect_cnt + 1)
#                 # df_group.at[i, column_name] = history.url
#         df_group.at[i, 'redirect_urls'] = redirect_urls
#         df_group.at[i, 'final_page'] = response.url
#
#         print("URL index:", start_index + i)
#
#     # 将处理后的结果写入文件
#     file_name = f'./data/urls-with-redirection/youtube-search-{start_index}_{end_index}.csv'
#     df_group.to_csv(file_name, index=False)
#
#
# executor = ThreadPoolExecutor(max_workers=10)
#
# futures = []
# for i in range(num_groups):
#     start_index = i * 10000
#     end_index = min((i + 1) * 10000, len(df))
#     df_group = url_groups[i]
#     future = executor.submit(process_url_group, df_group, start_index, end_index)
#     futures.append(future)
#
# # 等待所有任务完成
# for future in futures:
#     future.result()
