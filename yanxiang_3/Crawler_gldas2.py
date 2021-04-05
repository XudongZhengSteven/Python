# code: utf-8
# author: "Xudong Zheng" 
# email: Z786909151@163.com
import os
import re
import requests

# general set
# root = "E:/"
# home = os.path.join(root, "GLDAS_Noah_3hourly")
home = "G:/GLDAS_NOAH"
URL = os.path.join(home, "subset_GLDAS_NOAH025_3H_2.0_20210328_114227.txt")

# open url file and read url in urls
with open(URL, 'r') as file:
    urls = file.read()
    urls = urls.split("\n")

# distinguish url of pdf and nc4 file
url_pdf = urls[0]
urls = urls[1:]
pdf_name = 'README_GLDAS2.pdf'
pdf_name = os.path.join(home, pdf_name)
file_name = [re.search(r"LABEL.*\d\.nc4", url)[0][6:] for url in urls]
file_name = [os.path.join(home, file) for file in file_name]

# input start date and find the index in urls/file_name
# start = input("input the start date, such as 19480101.0300")
start = "19480425.1500"

if start == "":
    index = 0
    print("start: ", re.search(r"\d{8}\.\d{4}", file_name[0])[0])
else:
    index = [start in name for name in file_name].index(True)
    print("start: ", start)
urls = urls[index:]
file_name = file_name[index:]

# fail download links
fail_url = []


# download function
def download(url, filename):
    '''
    input:
        url: list
        filename: list
    '''
    print(f"start download {filename}")
    try:
        response = requests.get(url)
        f = open(filename, 'wb')
        f.write(response.content)
        f.close()
        print('contents of URL written to ' + filename)
        return ""
    except:
        print('Error to connect' + filename)
        return urls


# download pdf file
# download(url_pdf, pdf_name)


# download nc file
def serial_download():
    for i in range(len(urls)):
        fail_url.append(download(urls[i], file_name[i]))

# download nc file by multiprocessing
# urls = urls[:10]
# file_name = file_name[:10]
from multiprocessing import Pool


def mp_download():
    po = Pool(4)  # pool
    for i in range(len(urls)):
        res = po.apply_async(download, (urls[i], file_name[i]))
        fail_url.append(res.get())

    po.close()
    po.join()


if __name__ == "__main__":
    mp_download()
