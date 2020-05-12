#!/usr/bin/env python
# coding: utf-8

import time
import socket
import os
import random
import sys
import traceback
import pymysql
import pandas as pd
#使用說明：
# 請自行定義相關參數
# 程式全程以try execpt 建立，必須跑完完整程式碼

# 事前準備 建立SQL Table
# CREATE TABLE `yourdb`.`table` ( `daytime` datetime NOT NULL,  `timeSP` float NOT NULL,  `ip` text NOT NULL,  `filename` text NOT NULL,  `state` text NOT NULL,  `sSQL` int(11) NOT NULL,  `aSQL` int(11) NOT NULL,  `missSQL` int(11) NOT NULL,  `serialnum` text NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
# ex:CREATE TABLE `logTest`.`logtest2` ( `daytime` datetime NOT NULL,  `timeSP` float NOT NULL,  `ip` text NOT NULL,  `filename` text NOT NULL,  `state` text NOT NULL,  `sSQL` int(11) NOT NULL,  `aSQL` int(11) NOT NULL,  `missSQL` int(11) NOT NULL,  `serialnum` text NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

# 1 程式中需自行定義參數
# state = "success" or "fail+result" (fail要加進度與原因並以空白分隔) 
# (state需自行在程式中建立檢核點 ex : "70% 入庫前清洗發生錯誤")
# sSQL = 242 sql 輸入行數
# sql = "請輸入此次已入行數語法" 帶出一個int
# logfilename = 'logfile 絕對路徑' 請給定名稱跟路徑以用來創立
# errlogfilename = 'errlogfile 絕對路徑' 請給定名稱跟路徑以用來創立
# serialnumfile = '流水號紀錄檔絕對路徑' 請給定名稱跟路徑以用來創立
# codenum = 程式代碼 int 0~99 每隻程式不重複

# 2 在主程式區段編寫程式
# 依照流程區分 process1 process2 process3......

# 3 程式說明 ：
# def timing(): 計時的時間最後會相減
# def daytime():目前日期與時間
# def ip():取得當前ip如有多張網卡要另外做確認並註記
# def filename():取得當前檔案名稱
# state 請自行建立狀態代碼 str ex: 成功：success 失敗：fail,原因（原因用空白分隔
# sSQL = should be imported to SQL (int) 到SQL資料庫query
# aSQL = alreadly imported to SQL (int) 到SQL資料庫query
# def serialnum(): 
    # serialnumfile : file location 流水號紀錄位置
    # codenum : 固定機器編號 machine code
    # errnum : 增量值
    # 1 create file # create file and column names
    # 2 read csv file to df
    # 3 add new machine
    # if input new machine number
    # given all values 0 
    # and return serial number
    # 4 update old number
    # if input old machine number
    # donum +1
    # newerrnum + errnum
    # and return serial number
# def add60(num): add 0 to 6 digits
# def add30(num): add 0 to 3 digits
# def toFile(logfilename , daytime, timeS, timeE ,ip ,filename ,state ,sSQL ,aSQL ,serialnum):
# 將所有資料輸出到file
# def toErrFile(errlogfilename , daytime,filename,e, serialnum):
# 將錯誤訊息統一輸出到另外一個errlogfilename
# def pymysqlcon(ip, user, pw, db ,sql): 將toFile的訊息轉到sql

class monitor:
    def __init__(self,):
        pass
    def timing(self):
        timenow = time.time() #返回當前的時間戳記（從 1970/1/1 00:00:00 開始按秒計算的偏移量）
        return timenow
    def daytime(self):
        daytime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) #依照指定格式將 struct_time 時間資料轉換成文字輸出 
        return daytime
    def ip(self):
        hostname = socket.gethostname() #返回當前主機名稱
        ipAddr = socket.gethostbyname(hostname) #將主機名稱翻譯為 IPv4 位址
        return ipAddr
    def filename(self):
        absFilePath = os.path.abspath(__file__) #__file__ 為當前顯示文件的位址, abspath 表示返回絕對路徑
        path, filename = os.path.split(absFilePath) #把路徑分割成 dirname 和 basename, 並返回一個元組
        return filename

    def serialnum(self,serialnumfile,codenum,errnum):
        if os.path.isfile(serialnumfile) != True: #isfile -> 判斷路徑是否為文件
            with open(serialnumfile,'a') as file:
                file.write('donum,errnum,codenum\n') # 如果沒有這個檔案, 先創一個, 然後寫入三個欄位
        with open(serialnumfile,'r') as file: 
            csvfile = file.read() 
        df = pd.read_csv(serialnumfile) # 讀取 csv, 並以 dataframe 的形式讀取
        if codenum not in list(df["codenum"]): # 如果 codenum 欄位沒有任何帶入的代號（參數）則執行以下動作
            donum = 1 
            df = df.append({'donum':donum,'errnum':errnum,'codenum':codenum}, ignore_index=True)
            df.to_csv(serialnumfile, index=False) # 將 dataframe 寫入 csv
            donumS = self.add60(int(donum))
            errnumS = self.add60(int(errnum))
            codenumS = self.add20(int(codenum))
            serialnum = donumS + errnumS + codenumS
            return serialnum
        elif codenum in list(df["codenum"]): # 如果有的話增加次數
            oldrow = df[df['codenum']==codenum]
            df = df.drop([df[df['codenum']==codenum].index[0]])
            newdonum = list(oldrow['donum'])[0] + 1
            newerrnum = list(oldrow['errnum'])[0] + errnum
            df = df.append({'donum':newdonum,'errnum':newerrnum,'codenum':codenum}, ignore_index=True)
            df.to_csv(serialnumfile, index=False)
            donumS = self.add60(newdonum)
            errnumS = self.add60(newerrnum)
            codenumS = self.add20(codenum)
            serialnum = donumS + errnumS + codenumS
            return serialnum
    def add60(self,num):
        if num < 10:
            numS = '0' + '0' + '0' + '0' + '0' + str(num)
        elif num < 100:
            numS = '0' + '0' + '0' + '0' + str(num)
        elif num < 1000:
            numS = '0' + '0' + '0' + str(num)
        elif num < 10000:
            numS = '0' + '0' + str(num)
        elif num < 100000:
            numS = '0' + str(num)
        else:
            numS = str(num)
        return numS
    def add20(self, num):
        if num < 10:
            numS = '0' + str(num)
        else :
            numS = str(num)
        return numS
    def toFile(self,logfilename , daytime, timeS, timeE ,ip ,filename ,state ,sSQL ,aSQL , serialnum):
        if os.path.isfile(logfilename) != True:
            with open(logfilename,'a') as file:
                timeSP = timeE - timeS
                missSQL = sSQL - aSQL
                file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n'%("daytime", "timeSP" ,"ip" ,"filename" ,"state" ,"sSQL" ,"aSQL" ,"missSQL","serialnum"))

        with open(logfilename,'a') as file:
            timeSP = timeE - timeS
            missSQL = sSQL - aSQL
            file.write('%s,%.2f,%s,%s,%s,%s,%s,%s,%s\n'%(daytime, timeSP ,ip ,filename ,state ,sSQL ,aSQL ,missSQL,serialnum))
    def toErrFile(self,errlogfilename , daytime, filename, e, serialnum ,processnum,keyword=None):
        error_class = e.__class__.__name__ #取得錯誤類型
        detail = str(e.args[0].replace("\n", "\t")).replace(',','|')
        # detail = detail.replace(",","|") #取得詳細內容, 取出來會有分行, 記得要換行
        cl, exc, tb = sys.exc_info() #取得Call Stack
        lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
        lineNum = lastCallStack[1] #取得發生的行號
        funcName = lastCallStack[2] #取得發生的函數名稱
        err = "File \"{}\" line {} in {} : [{}] {}".format(filename, lineNum, funcName, error_class, detail)
        if os.path.isfile(errlogfilename) != True:
            with open(errlogfilename,'a') as file:
                file.write('%s,%s,%s,%s,%s,%s\n'%("daytime","filename","processnum","keyword","err","serialnum" ))

        with open(errlogfilename,'a') as file:
            file.write('%s,%s,%s,%s,%s,%s\n'%(daytime,filename,processnum,keyword,err,serialnum))
    def pymysqlcon(self,ip, user, pw, db ,sql):
        db = pymysql.connect(ip, user, pw, db)
        cursor = db.cursor(pymysql.cursors.DictCursor)#拿到dict
        cursor.execute(sql)
        db.commit()#commit是把查詢語句提交到資料庫內
        accounts = cursor.fetchall()
        db.close()
        return db, cursor ,accounts

