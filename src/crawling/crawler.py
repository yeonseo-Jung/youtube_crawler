import os
import re
import sys
import time
import pickle
import numpy as np
import pandas as pd

import socket
import warnings
warnings.filterwarnings("ignore")

# Scrapping
from selenium import webdriver
from user_agent import generate_user_agent
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.alert import Alert

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

def get_url(url, window=True, image=True):
    ''' Set up webdriver, useragent & Get url '''
    
    wd = None
    socket.setdefaulttimeout(30)
    error = []
    attempts = 0 # url parsing 시도횟수
    # 10번 이상 parsing 실패시 pass
    while attempts < 10:
        try:  
            attempts += 1
            # user agent
            options = Options() 
            userAgent = generate_user_agent(os=('mac', 'linux'), navigator='chrome', device_type='desktop')
            options.add_argument('window-size=1920x1080')
            options.add_argument("--disable-gpu")
            options.add_argument('--disable-extensions')
            if not window:
                options.add_argument('headless')
            if not image:
                options.add_argument('--blink-settings=imagesEnabled=false')
            options.add_argument(f'user-agent={userAgent}')

            # web driver 
            wd = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
            wd.get(url)
            wd.implicitly_wait(5)
            break

        # 예외처리
        except Exception as e:
            print(f'\n\nError: {str(e)}\n\n')
            time.sleep(300)
            try:
                wd.quit()
            except:
                pass
            wd = None
    return wd
        
def scroll_down(wd, sleep_time, check_count):
    ''' page scroll down '''
    
    cnt = 0
    while True:
        height = wd.execute_script("return document.body.scrollHeight")
        wd.find_element_by_tag_name('body').send_keys(Keys.END)
        time.sleep(sleep_time)
        if int(height) == 0:
            cnt += 1
            if cnt == check_count:
                break        
    return wd