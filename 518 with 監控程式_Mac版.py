#!/usr/bin/env python
# coding: utf-8
import sys
import traceback
import os
import datetime
import pymysql
import re
import pandas as pd
from monitor import monitor
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup as bs
from selenium.webdriver.chrome.options import Options

'''設定logfile位置'''
errlogfilename = '/Users/afrithero/Desktop/AI:Big Data 講義/Python with monitor/err.csv'
logfilename = '/Users/afrithero/Desktop/AI:Big Data 講義/Python with monitor/monitor.csv'
serialnumfile = '/Users/afrithero/Desktop/AI:Big Data 講義/Python with monitor/serialnum.csv'
codenum = 1

'''log變數'''
monitor = monitor() #建立 class monitor 物件
#調用 monitor 方法
timeS = monitor.timing() 
daytime = monitor.daytime() 
ip = monitor.ip() 
absFilePath = os.path.abspath(__file__)
path, filename = os.path.split(absFilePath)
processnum = 0
sSQL = 0

'''Chrome 瀏覽器設定 '''
# caps = DesiredCapabilities().CHROME 
# 解決 Selenium 頁面加載慢的問題 => 將預設值 "Normal" 變更為 "eager", 表示網頁加載完 Document 就會停止動作，不會額外加載我們不需要的東西
# caps["pageLoadStrategy"] = "eager"
# 對 Chrome 瀏覽器進行設定
# chrome_options = Options()
# 以最高權限運行
# chrome_options.add_argument('--no-sandbox')
# 變更預設記憶體運行空間 => 預設的空間為 dev/shm 這對於 chrome 來說太小，不 disable 這個設定的話可能會導致瀏覽器崩潰
# chrome_options.add_argument('--disable-dev-shm-usage')
# headless => 不提供可視化頁面, linux 系統如果不支持可視化不加這條會啟動失敗
# chrome_options.add_argument('--headless')
# 規避 bug 
# chrome_options.add_argument('--disable-gpu')

# keyword_list = ["數據分析", "data analytic", "數據工程", "data engineer", "數據處理", "data process", "大數據", "big data",
#                 "資料探勘", "data mining", "前端工程師", "front end engineer", "後端工程師", "back end engineer", "全端工程師",
#                 "full stack engineer", "人工智慧", "artificial intelligence", "機器學習", "machine learning",
#                 "Python", "Hadoop", "Spark", "ELK", "Tableau", "Qlik", "Splunk", "MySQL", "MongoDB", "InfluxDB", "etl", "linux"]

keyword_list = ["數據分析","前端工程師"]


#主程式
#-------------------------------------------------------------
# 啟動 Chrome
# driver = webdriver.Chrome('/usr/local/share/chromedriver',desired_capabilities=caps, chrome_options=chrome_options)
driver = webdriver.Chrome('/Users/afrithero/Desktop/AI:Big Data 講義/Python with monitor/chromedriver')
driver.implicitly_wait(15) # 隱性等待 15 秒，在 chrome_driver 的生命週期中，每當執行尋找元素的動作，都會預設等待 15 秒，一旦找到則進行下一步
driver.get('https://www.518.com.tw/')


for keyword in keyword_list:
#process1
    processnum = 1
    try:
        search_engine = driver.find_element_by_id('kwds')
        search_engine.send_keys(keyword)
        driver.find_element_by_id('gtm_search_job_web').click()
        state = "success"
        errnum = 0
        print('輸入關鍵字成功,現在正在爬的關鍵字是：',keyword)

    except Exception as e:
        state = "fail"
        errnum = 1
        serialnum = monitor.serialnum(serialnumfile,codenum,errnum) 
        monitor.toErrFile(errlogfilename,daytime,filename,e,serialnum,processnum,keyword)

    try: 
        driver.find_element_by_css_selector('#linkpage > span.sum > em')

    except:
        url_counts = 0
        print('應進入行數:',url_counts)
    
    else:
        url_counts = int(driver.find_element_by_css_selector('#linkpage > span.sum > em').text.replace(',',''))
        print('應進入行數:',url_counts)

    link_url = []
    job_details_all = []
#process2
    try:
        while True:
            for i in driver.find_elements_by_css_selector('li.title > a'):
                if 'kw' in i.get_attribute('href'):
                    link_url.append(i.get_attribute('href'))
            try:
                next_page = driver.find_element_by_css_selector('a.goNext')     
            except:
                break
            else:
                next_page.location_once_scrolled_into_view
                next_page.click()
        state = "success"
        errnum = 0
        sSQL += len(link_url)

    except Exception as e:
        processnum = 2
        state = "fail"
        errnum = 1
        serialnum = monitor.serialnum(serialnumfile,codenum,errnum) 
        monitor.toErrFile(errlogfilename , daytime,filename,e, serialnum, processnum,keyword)

    for j in link_url:    
        driver.get(j)
        html_source = driver.page_source
        soup=bs(html_source,"html.parser")
        job_details = {}
#process3
        try:
            #----- vv
            job_details['公司名稱'] = soup.select('div.path_location > a')[4].text
            job_details['產業類別'] = soup.select('div.path_location > a')[3].text
            #----- ^^
            job_details['工作內容'] = soup.select('div.JobDescription > p')[0].text.strip().replace('\n',' ')
            job_details['職務名稱'] = soup.select('h1.job-title')[0].text.strip()
            # -----
            job_details['職務類別'] = soup.select('div.jobItem > ul > li > span')[6].text.strip()
            job_details['工作待遇'] = soup.select('div.jobItem > ul > li > span')[0].text.strip()
            job_details['工作地點'] = soup.select('div.jobItem > ul > li > span')[1].text.strip()
            # -----
            job_details['職缺網址'] = j
            job_details['關鍵字'] = keyword
#更新日期有的寫 yyyy-mm-dd，有的寫 x 天前，如果是前者直接爬取，後者則需要轉成 yyyy-mm-dd 的格式
            # -----
            if len(soup.select('#content > .job-detail-box > h4 > span > time')[0].text.strip('更新日期:')) == 10:
                job_details['更新日期'] = soup.select('#content > .job-detail-box > h4 > span > time')[0].text.strip('更新日期:')
            else:
                str_time = soup.select('#content > .job-detail-box > h4 > span > time')[0].text.strip('職務更新日期:天前')
                int_time = int(str_time)      #利用 datetime.now 得到當前時間，再扣掉該職缺未更新天數，即得到最後一次更新時間
                job_details['更新日期'] = (datetime.datetime.now()-datetime.timedelta(days = int_time)).strftime("%Y-%m-%d")
            # -----
            state = "success"
            errnum = 0

        except Exception as e:
            processnum = 3
            state = "fail"
            errnum = 1
            serialnum = monitor.serialnum(serialnumfile,codenum,errnum) 
            monitor.toErrFile(errlogfilename,daytime,filename,e,serialnum,processnum,keyword)
            continue
#process4
        try:
            job_key = [k.text[:4] for k in soup.select('#content > .job-detail-box > .condition > ul > li')]
            job_value = [l.text.replace(' ','')[4:] for l in soup.select('#content > .job-detail-box > .condition > ul > li')]
            raw_job_details = dict(zip(job_key,job_value))
            job_details['工作經歷'] = raw_job_details.setdefault('工作經驗', 'NULL')
            job_details['學歷要求'] = raw_job_details.setdefault('學歷要求', 'NULL')
            job_details['科系要求'] = raw_job_details.setdefault('科系限制', 'NULL')
            job_details['工作技能'] = raw_job_details.setdefault('工作技能', 'NULL')
            job_details['電腦專長'] = raw_job_details.setdefault('擅長工具', 'NULL')
            job_details['其他條件'] = raw_job_details.setdefault('其他條件', 'NULL')
            job_details['工作性質'] = raw_job_details.setdefault('工作性質', 'NULL')
            
            contact_key = [n.text.strip('：').replace('\u3000','') for n in soup.select('#content > div.job-detail-box.show > dl > dt')] #title
            contact_value = [o.text for o in soup.select('#content > div.job-detail-box.show > dl > dd')]
            contact_details = dict(zip(contact_key,contact_value))
            job_details['聯絡人員'] = contact_details.setdefault('職務聯絡人', 'NULL')
            job_details['電子郵件'] = contact_details.setdefault('聯絡Email', 'NULL')
            job_details['手機聯絡'] = contact_details.setdefault('手機', 'NULL')
            job_details['市話聯絡'] = contact_details.setdefault('電洽', 'NULL')
            job_details['其他方式'] = contact_details.setdefault('其他應徵方式及備註', 'NULL')

        except Exception as e:
            processnum = 4
            state = "fail"
            errnum = 1
            serialnum = monitor.serialnum(serialnumfile,codenum,errnum) 
            monitor.toErrFile(errlogfilename,daytime,filename,e,serialnum,processnum,keyword)
            continue

        company_button = driver.find_element_by_css_selector('h3 > a')
        company_button.click()
#process5
        try:
            driver.find_element_by_css_selector('div.companyProfile')
            html_source = driver.page_source
            soup = bs(html_source,"html.parser")
            # ----
            job_details['人數規模'] = soup.select('div.companyProfile > ul > li')[0].text[4:]
            # ----

        except Exception as e:
            processnum = 5
            state = "fail"
            errnum = 1
            serialnum = monitor.serialnum(serialnumfile,codenum,errnum) 
            monitor.toErrFile(errlogfilename,daytime,filename,e,serialnum,processnum,keword)
            continue

        for m in job_details:
            if job_details[m] == '不拘' or job_details[m] == '未填寫' or job_details[m] == '請先登入後查看' or job_details[m] == '暫不提供' or job_details[m] == '無經驗可':
                job_details[m] = 'NULL'
        job_details_all.append(job_details)

    for x in job_details_all:
        db = pymysql.connect('127.0.0.1','root','root','518')
        cursor = db.cursor(pymysql.cursors.DictCursor)
        sql = """
            Insert into `518`.`518_test`(update_time, corp_name, corp_cat, corp_employee, job_desc, job_title, job_cat, job_salary, work_place, work_exp, degree_req, department_req, job_skill, compute_exp, other_req, job_type, contact_name, contact_email, contact_cell, contact_phone, contact_other,job_url,keyword)
            Values('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
        """.format(x["更新日期"], x["公司名稱"], x["產業類別"], x["人數規模"], x["工作內容"], x["職務名稱"], x["職務類別"], x["工作待遇"], x["工作地點"], x["工作經歷"], x["學歷要求"], x["科系要求"], x["工作技能"], x["電腦專長"], x["其他條件"], x["工作性質"], x["聯絡人員"], x["電子郵件"], x["手機聯絡"],
                x["市話聯絡"], x["其他方式"],x['職缺網址'],x['關鍵字'])
        cursor.execute(sql)
        db.commit()
        db.close()
    driver.get('https://www.518.com.tw/')
    driver.find_element_by_css_selector('li.remove-all > a').click()
#......
#-------------------------------------------------------------    
#success serialnum
serialnum = monitor.serialnum(serialnumfile,codenum,errnum) 

#sql驗證(請修改sSQL & sql兩個變數)
#-------------------------------------------------------------
try:
    #請select出此次執行已進入SQL行數
    sql = "select count(*) as cou from `518`.`518_test` where `time` between DATE_ADD(NOW(), INTERVAL -1 DAY) and NOW();"
    db, cursor ,accounts = monitor.pymysqlcon("127.0.0.1", 'root', 'root', "518" ,sql)
    aSQL = accounts[0]['cou']
except:
    sSQL = 0
    aSQL = 99
#-------------------------------------------------------------    
print('應進入行數:',sSQL)
print('已進入 SQL 行數：', aSQL)
timeE = monitor.timing() 
timeSP = timeE -timeS
monitor.toFile(logfilename,daytime, timeS, timeE,ip ,filename ,state ,sSQL ,aSQL ,serialnum)
missSQL = sSQL - aSQL
#請先建立好 SQL table
#log to SQL 請輸入 ip account password databases query
sql = "INSERT INTO `518`.`logtest` VALUES ('{}', {:.2f}, '{}', '{}', '{}', {}, {}, {}, {});".format(daytime,timeSP,ip,filename,state,sSQL,aSQL,missSQL,serialnum)
monitor.pymysqlcon("127.0.0.1", 'root', 'root', "518" ,sql)
driver.close()