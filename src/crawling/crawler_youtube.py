import os
import re
import sys
import time
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from user_agent import generate_user_agent
from selenium.webdriver.support.ui import WebDriverWait

from selenium.webdriver.common.by import By
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

import requests
from requests.exceptions import ConnectionError
from bs4 import BeautifulSoup
from user_agent import generate_user_agent

from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api import TranscriptsDisabled

import socket
import warnings
warnings.filterwarnings("ignore")

# current directory
cur_dir = os.path.dirname(os.path.realpath(__file__))

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    conn_path = os.path.join(base_path, 'conn.txt')
    
else:
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(root)
    sys.path.append(src)
    tbl_cache = os.path.join(root, 'tbl_cache')
    conn_path = os.path.join(src, 'gui', 'conn.txt')

from crawling.crawler import get_url, scroll_down

class CrawlingYoutube:
    
    def crawling_url(self, searching_word):
        searching_word = searching_word.replace(' ', '+')
        filter_query = 'sp=EgYIBBABKAE%253D'
        search_url = f'https://www.youtube.com/results?search_query={searching_word}&{filter_query}'
        wd = get_url(search_url, window=False, image=False)

        try:
            xpath_filter = '//*[@id="container"]/ytd-toggle-button-renderer/a'
            WebDriverWait(wd, 15).until(EC.element_to_be_clickable((By.XPATH, xpath_filter)))
        except TimeoutException:
            wd.quit()
            wd = None
        except Exception as e:
            print(e)
            wd = None
            
        if wd == None:
            urls = []
            status = 0
        else:
            # page scroll down
            wd = scroll_down(wd, sleep_time=2.5, check_count=25)
            
            # scraping video id (href)
            soup = BeautifulSoup(wd.page_source, 'lxml')
            url_a = soup.find_all('a', 'yt-simple-endpoint style-scope ytd-video-renderer')
            urls = []
            for a in url_a:
                _id = a['href']
                url = f'https://www.youtube.com{_id}'
                urls.append(url)
            status = 1
        return urls, status
    
    def scrape_transcripts(self, url: str) -> tuple:
        ''' Scraping Youtube Script
        
        :: input data
            * url: youtube link
            
        :: output data
            * script: youtube script
        '''
        
        video_id = url[url.find('v=')+2:]
        try:
            userAgent = generate_user_agent(os=('mac', 'linux'), navigator='chrome', device_type='desktop')
            headers = {'user-agent': userAgent}
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id, proxies=headers)
            status = 200
            
            for transcript in transcript_list:
                # fetch the actual transcript data
                transcripts = transcript.fetch()
                
                script = ""
                for transcript in transcripts:
                    script += transcript['text'] + " "
                    
            script = re.sub(' +', ' ', script).strip()

            if script == "":
                status = -1
                script = np.nan
                
        except TranscriptsDisabled:
            # 재생 불가 동영상 or 스크립트 누락 동영상
            status = -1
            script = np.nan
            
        except ConnectionError as e:
            # ConnectionError
            status = 403
            print(e, status, '\ntime sleep: 300sec')
            script = np.nan
            time.sleep(300)
            
        return status, video_id, script
        
    def parsing_url(self, url: str) -> tuple:
        ''' Parsing url & scraping tite, channel_name'''
        
        try:
            # url pasing & user agent
            userAgent = generate_user_agent(os=('mac', 'linux'), navigator='chrome', device_type='desktop')
            headers = {'user-agent': userAgent}
            html = requests.get(url, headers=headers)
            status = html.status_code
        except ConnectionError as e:
            status = 404
            print(e, status, '\ntime sleep: 300sec')
            title, channel_name = np.nan, np.nan
            time.sleep(300)
        
        if status == 200:
            try:
                # scraping title & channel_name
                soup = BeautifulSoup(html.text, 'lxml')
                title = soup.find('title').text.replace('- YouTube', '').strip()
                channel_name = soup.find('div', 'watch-main-col').find('link', {'itemprop': 'name'})['content']   
                
                # thumbnail image
                if soup.find('link', {'rel': 'image_src'}) == None:
                    thumbnail_img = np.nan
                else:
                    thumbnail_img = soup.find('link', {'rel': 'image_src'})['href']

                # youtuber profile image
                reg = re.compile(r'https\:\/\/yt3\.ggpht\.com\/[a-zA-Z0-9\-\_\/]+\=s48\-c\-k\-c0x00ffffff\-no\-rj')
                if re.search(reg, html.text) == None:
                    profile_img = np.nan
                else:
                    profile_img = re.search(reg, html.text).group(0)
                    
            except AttributeError:
                # scraping failed
                status = -1
                title, channel_name, thumbnail_img, profile_img = np.nan, np.nan, np.nan, np.nan
                
        else:
            title, channel_name, thumbnail_img, profile_img = np.nan, np.nan, np.nan, np.nan
            
        return status, title, channel_name, thumbnail_img, profile_img