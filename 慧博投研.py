import requests
from bs4 import BeautifulSoup
import pandas as pd
import pymongo
import json
import time
import schedule

client = pymongo.MongoClient(host='localhost',port=27017)
db = client['hibor_report']
collection_report = db["report_data"]

report = {}
def spider_detail_urls(url):
    title_list = []
    detail_url_list = []
    id = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 UBrowser/6.2.4098.3 Safari/537.36',
               'Host': 'www.hibor.com.cn'}
    response = requests.get(url, headers=headers)
    text = response.content.decode()
    bs = BeautifulSoup(text,"html5lib")
    div = bs.find("div",class_="leftn2")
    table = div.find("table",class_="tab_ltnew")
    trs = table.find_all("tr")
    for tr in trs[::4]:
        span = tr.find("span",class_="tab_lta")
        title =  list(span.stripped_strings)[0]
        title_list.append(title)
        _url = span.find("a").get("href")
        detail_url = "http://www.hibor.com.cn"+_url
        detail_url_list.append(detail_url)
        _id = _url.split("_")[-1].split(".")[0]
        id.append(_id)
    report["id"] = id
    report["连接"] = detail_url_list
    report["标题"] = title_list
    return detail_url_list

def spider_abstract(detail_url_list):
    abstract = []
    headers = {'Accept-Language': 'zh-CN,zh;q=0.9',
               'Connection': 'keep-alive',
               'Cookie': 'c=; safedog-flow-item=E452688EA9408CDB488598E819CA5CAE; UM_distinctid=17221e00e0a26-02018c653952da-d373666-144000-17221e00e0b56; did=67A671BFE; ASPSESSIONIDCABDDAQR=CNAHPHPCOHJBHKGHKGAGLOND; Hm_lvt_d554f0f6d738d9e505c72769d450253d=1589706231,1590147502,1590713899,1592128454; ASPSESSIONIDAQSSSRDS=KKGBDNADKNMBKOAAJFLPBJED; CNZZDATA1752123=cnzz_eid%3D1486449273-1589705206-https%253A%252F%252Fwww.baidu.com%252F%26ntime%3D1592138092; robih=OWvVuXvWjVoWKY9WdWsU; MBpermission=0; MBname=Curtis%5FLau; Hm_lpvt_d554f0f6d738d9e505c72769d450253d=1592140743',
               'Host': 'www.hibor.com.cn',
               'Referer': 'http://www.hibor.com.cn/docdetail_2937262.html',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
               'X-Requested-With': 'XMLHttpRequest'}
    for url in detail_url_list:
        response = requests.get(url=url, headers=headers)
        text = response.content.decode("gbk")
        bs = BeautifulSoup(text, "html5lib")
        div = bs.find("div", class_="neir")
        span = div.find("span")
        txt = list(span.stripped_strings)
        sentence = ""
        for i in txt:
            if "http://www.hibor.com.cn" not in i:
                sentence += i
        abstract.append(sentence)
    report["摘要"] = abstract
    report_data = pd.DataFrame(report)
    return report_data

def update_mongodb(report_data):
    if len(pd.DataFrame(collection_report.find()))==0:
        new_l = report_data.sort_values(by='id', ascending=True)
        collection_report.insert_many(json.loads(new_l.T.to_json()).values())
    else:
        last_id = pd.DataFrame(collection_report.find()).sort_values(by="id", ascending=False)["id"].max()
        in_list = report_data[report_data['id'] > last_id]
        new_l = in_list.sort_values(by='id', ascending=True)
        collection_report.insert_many(json.loads(new_l.T.to_json()).values())
    print('{} items has been update Successed on {}'.format(len(new_l), time.strftime('%Y-%m-%d %H:%M:%S')))

def run_time():
    page = list(range(10, 0, -1))
    for i in page:
        url = "http://www.hibor.com.cn/microns_1_{}.html".format(i)
        detail_urls = spider_detail_urls(url)
        report_data = spider_abstract(detail_urls)
        update_mongodb(report_data)

schedule.every(4).to(6).days.do(run_time)

if __name__ == '__main__':
    while True:
        schedule.run_pending()

