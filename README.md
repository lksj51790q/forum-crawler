# Forum Crawler
各論壇的爬蟲<br>
需求模組：requests、bs4、lxml、dateutil
```
get_board                 取得所有看板名稱
Crawler                   爬蟲物件
    article_id_generate   回傳一個Generator遍歷所有符合條件的文章網址
    set_target_article    將爬蟲目標設置為特定文章
    get_article           取得目標文章資訊
    get_reply             取得目標文章回覆訊息
```
