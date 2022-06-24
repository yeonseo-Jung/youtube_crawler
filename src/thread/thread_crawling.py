import os
import re
import sys
import time
import pickle
import pandas as pd
from tqdm.auto import tqdm
from datetime import datetime

import numpy as np
from PyQt5 import QtCore

import warnings
warnings.filterwarnings("ignore")

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
sys.path.append(root)
sys.path.append(src)

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    conn_path = os.path.join(base_path, 'conn.txt')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = root + '/tbl_cache'
    conn_path = os.path.join(src, 'gui/conn.txt')
    
from crawling.crawler_youtube import CrawlingYoutube
from database.access_db import AccessDataBase

class ThreadCrawlingYoutubeUrl(QtCore.QThread, QtCore.QObject):   
    
    def __init__(self):
        super().__init__()
        self.power = True
        self.check = 0
        
        self.crw = CrawlingYoutube()
        
        # path
        self.path_input = os.path.join(tbl_cache, 'words.txt')
        self.path_output = os.path.join(tbl_cache, 'urls.txt')
        self.error = os.path.join(tbl_cache, 'error.txt')
        
        # today (regist date)
        today = datetime.today()
        year = str(today.year)
        month = str(today.month)
        day = str(today.day)
        if len(month) == 1:
            month = "0" + month
        if len(day) == 1:
            day = "0" + day
        self.date = year + "-" + month + "-" + day
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
        # # ** 검색어 추가 필요 시 여기에 추가하세요 ** 
        # self.makeup = ['sephora makeup', 'ulta makeup', 'sephora makeup tutorial', 'ulta makeup tutorial']
        # self.info = ['sephora haul', 'sephora sale', 'ulta haul', 'ulta sale', 'sephora review', 'ulta review']
        # self.skincare = ['sephora skincare', 'ulta skincare']
        # self.fragrance = ['sephora fragrance', 'ulta fragrance']        

    def _upload_db(self):
        
        self.urls = list(set(self.urls))
        print(f'\n url counts: {len(self.urls)}')
        df = pd.DataFrame(self.urls, columns=['url'])
        try:
            self.db.engine_upload(df, 'youtube_urls_temp', 'replace')
        
        except Exception as e:
            # db 연결 끊김: 인터넷(와이파이) 재연결 필요
            print(e)
            if self.power:
                self.stop()
            self.check = 2
            status = -1
    
    progress = QtCore.pyqtSignal(object)
    def run(self):
        with open(self.path_input, 'rb') as f:
            searching_words = pickle.load(f)
            
        if os.path.isfile(self.path_output):
            
            with open(self.path_output, 'rb') as f:
                self.urls = pickle.load(f)
        else:
            self.urls = []
            
        if os.path.isfile(self.error):
            with open(self.error, 'rb') as f:
                error = pickle.load(f)
        else:
            error = []
            
        t = tqdm(searching_words)
        i = 0
        for searching_word in t:
            
            if self.power:
                self.check = 0
                self.progress.emit(t)
                url, status = self.crw.crawling_url(searching_word)
                
                if status == 1:
                    self.urls += url
                else:
                    error.append(searching_word)
                    
                i += 1
            else:
                break
            
        if i == len(searching_words):
            if len(error) != 0:
                # Recrawl due to error occurrence
                t = tqdm(error)
                for searching_word in t:
                    self.check = 0
                    self.progress.emit(t)
                    url, status = self.crw.crawling_url(searching_word)
                    self.urls += url
            self._upload_db()
            
        # save file in cache dir
        with open(self.path_input, 'wb') as f:
            pickle.dump(searching_words[i:], f)
        with open(self.path_output, 'wb') as f:
            pickle.dump(self.urls, f)
            
        self.progress.emit(t)
        self.power = False
        
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)
        
class ThreadCrawlingYoutubeScript(QtCore.QThread, QtCore.QObject):   
    
    def __init__(self):
        super().__init__()
        self.power = False
        self.check = 0
        
        self.crw = CrawlingYoutube()
        
        # path
        self.path_input = os.path.join(tbl_cache, 'input.txt')
        self.path_output = os.path.join(tbl_cache, 'output.txt')
        self.path_output_df = os.path.join(tbl_cache, 'scrapes.csv')
        self.error = os.path.join(tbl_cache, 'error.txt')
        
        # today (regist date)
        today = datetime.today()
        year = str(today.year)
        month = str(today.month)
        day = str(today.day)
        self.date = year + "-" + month + "-" + day
        
        self.crw = CrawlingYoutube()
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
    def _get_tbl(self):
        df = self.db.get_tbl('youtube_urls_temp')
        _df = self.db.get_tbl('youtube_video_collect', ['url'])
        if df is None:
            status = 0
            urls = []
        elif _df is None:
            status = 1
            urls = list(set(df.url.unique().tolist()))
        else:
            if len(_df) == 0:
                status = 1
                urls = list(set(df.url.unique().tolist()))
            else:
                # dup check
                urls = list(set(df.url.unique().tolist() + _df.url.unique().tolist()))
                if len(urls) == 0:
                    status = 0
                else:
                    status = 1
                    with open(self.path_input, 'wb') as f:
                        pickle.dump(urls, f)
                
        return status, urls
    
    def preprocessing(self, scrape_df):
        ''' Script Preprocessing '''

        _scrape_df = scrape_df[scrape_df.video_title.notnull() & scrape_df.script.notnull()].reset_index(drop=True)
        _scrape_df_null = scrape_df[scrape_df.video_title.notnull() & scrape_df.script.isnull()].reset_index(drop=True)

        # 영문, 특수기호(,.!?')만 추출
        reg_eng = re.compile('[^a-zA-Z0-9\.\,\?\!\']')
        _scrape_df.loc[:, 'script'] = _scrape_df.script.str.replace(reg_eng, ' ').str.replace(' +', ' ')

        # 특수기호와 영문 빈도 계산 및 비율 비교를 통한 필터링
        _scrape_df_copy = _scrape_df.copy()
        _scrape_df_copy.loc[:, 'en'] = _scrape_df_copy.script.str.count('[a-zA-Z]')
        _scrape_df_copy.loc[:, 'num'] = _scrape_df_copy.script.str.count('[0-9]')
        _scrape_df_copy.loc[:, 'sp_0'] = _scrape_df_copy.script.str.count('\.')
        _scrape_df_copy.loc[:, 'sp_1'] = _scrape_df_copy.script.str.count('\,')
        _scrape_df_copy.loc[:, 'sp_2'] = _scrape_df_copy.script.str.count('\?')
        _scrape_df_copy.loc[:, 'sp_3'] = _scrape_df_copy.script.str.count('\!')

        drop_index = []
        for i in range(len(_scrape_df_copy)):
            counts = _scrape_df_copy.iloc[i, -6:].tolist()
            
            if str(counts[0]) == 'nan':
                pass
            else:
                if counts.index(max(counts)) != 0:
                    drop_index.append(i)
                elif counts[0] <= sum(counts[1:]) * 13:
                    drop_index.append(i)
                else:
                    pass

        _scrape_df_copy_ = _scrape_df_copy.drop(drop_index)
        _scrape_df_copy_concat = pd.concat([_scrape_df_copy_, _scrape_df_null]).sort_values('youtuber').reset_index(drop=True)

        # Upload table into Database
        columns = ['url', 'video_title', 'video_id','thumbnail', 'youtuber', 'youtuber_profile', 'script', 'video_type']
        upload_df =  _scrape_df_copy_concat.loc[:, columns]
        
        return upload_df
    
    def _upload_db(self, comp=False):
        
        scrape_df = pd.DataFrame(self.scrapes, columns=['url', 'video_title', 'video_id', 'youtuber', 'script', 'thumbnail', 'youtuber_profile', 'status', 'video_type'])
        upload_df = self.preprocessing(scrape_df)
        upload_df.loc[:, 'regist_date'] = pd.Timestamp(self.date)
        
        try:
            table_name = "youtube_video_collect"
            if comp:
                if table_name in self.db.get_tbl_name():
                    self.db.engine_upload(upload_df, table_name, if_exists_option='append')
                else:
                    self.db.create_table(upload_df, table_name)
                
                # Drop temporary table
                self.db.drop_table('youtube_urls_temp')
                self.db.drop_table('youtube_video_collect_temp')
                print(f'\n new data counts: {len(upload_df)}')
            else:
                self.db.engine_upload(upload_df, 'youtube_video_collect_temp', 'replace')
        except Exception as e:
            # db 연결 끊김: 인터넷(와이파이) 재연결 필요
            print(e)
            if self.power:
                self.stop()
            self.check = 2
            status = -1
        
    progress = QtCore.pyqtSignal(object)
    def run(self):

        if os.path.isfile(self.path_input):
            with open(self.path_input, 'rb') as f:
                urls = pickle.load(f)
        else:
            status, urls = self._get_tbl()
            
            
        if os.path.isfile(self.path_output):
            with open(self.path_output, 'rb') as f:
                self.scrapes = pickle.load(f)
        else:
            self.scrapes = []
            
        if os.path.isfile(self.error):
            with open(self.error, 'rb') as f:
                error = pickle.load(f)
        else:
            error = []

        t = tqdm(urls)
        i = 0
        for url in t:
            ''' status
                    * 200: url pasing successful
                    * 404: url pasing failed
                    * 403: url pasing failed
                    *   1: shorts
                    *  -1: scraping failed
            '''
            if self.power:
                self.check = 0
                self.progress.emit(t)
                
                # scraping transcription
                if 'shorts' in url:
                    status, video_id, script = 1, url.replace('https://www.youtube.com/shorts/', ''), np.nan
                    video_type = 'shorts'
                elif 'watch?v=' in url:
                    status, video_id, script = self.crw.scrape_transcripts(url)
                    video_type = 'youtube'
                else:
                    video_id, script = np.nan, np.nan
                    video_type = np.nan
                    
                # scraping video info
                status, title, channel_name, thumbnail_img, profile_img = self.crw.parsing_url(url)
                
                if (status == 404) | (status == 403) | (status == -1):
                    error.append(url)
                else:
                    self.scrapes.append([url, title, video_id, channel_name, script, thumbnail_img, profile_img, status, video_type])
                i += 1
            else:
                break
            
        if i == len(urls):
            if len(error) != 0:
                # Recrawl due to error occurrence
                t = tqdm(error)
                for url in t:
                    self.check = 0
                    self.progress.emit(t)
                    
                    # scraping transcription
                    if 'shorts' in url:
                        status, video_id, script = 1, url.replace('https://www.youtube.com/shorts/', ''), np.nan
                        video_type = 'shorts'
                    elif 'watch?v=' in url:
                        status, video_id, script = self.crw.scrape_transcripts(url)
                        video_type = 'youtube'
                    else:
                        video_id, script = np.nan, np.nan
                        video_type = np.nan
                        
                    # scraping video info
                    status, title, channel_name, thumbnail_img, profile_img = self.crw.parsing_url(url)
                    
                    self.scrapes.append([url, title, video_id, channel_name, script, thumbnail_img, profile_img, status, video_type])
            self._upload_db(comp=True)
        else:
            self._upload_db(comp=False)

        # save file in cache dir
        with open(self.path_input, 'wb') as f:
            pickle.dump(urls[i:], f)
        with open(self.path_output, 'wb') as f:
            pickle.dump(self.scrapes, f)
        scrape_df = pd.DataFrame(self.scrapes, columns=['url', 'video_title', 'video_id', 'youtuber', 'script', 'thumbnail', 'youtuber_profile', 'status', 'video_type'])
        scrape_df.to_csv(self.path_output_df, index=False)
        
        self.progress.emit(t)    
        self.power = False
        
    def stop(self):
        ''' Stop Thread '''
        
        self.power = False
        self.quit()
        self.wait(3000)