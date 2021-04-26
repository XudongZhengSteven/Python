# 研享天下
welcome to here!这是一个python的代码分享仓库，更多信息请关注"研享天下"公众号


也欢迎进行批评指正，如有问题请私信或issure/pull request！

---
## 代码组织方式
yanxiang_number(内容期数)，如下
* yanxiang_1_2 为第一期和第二期内容
* yanxiang_3 为第三期内容

---
## 下面为内容目录（更新中...）：
* yanxiang_1_2：第一二期，nc文件的区域提取
  * 脚本：分别为单进程（extract_nc.py）和多进程脚本（extract_np_mp.py）
  * 数据：GIS（shp数据），gldas（10个gldas的nc4文件）
  * 结果：result为提取的结果文件，以Snowf_tavg变量为例


* yanxiang_3：第三期，GLDAS数据爬虫
  * 脚本：Crawler_gldas2/3.py分别对应两种爬取方式（.netrc, cookie）
