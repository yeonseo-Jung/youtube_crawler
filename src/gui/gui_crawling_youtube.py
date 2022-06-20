import os
import re
import sys
import pickle
import pandas as pd
from tqdm.auto import tqdm

from PyQt5 import uic
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog, QListWidgetItem

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src = os.path.abspath(os.path.join(cur_dir, os.pardir))
    sys.path.append(root)
    sys.path.append(src)
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = os.path.join(root, 'tbl_cache')
    
conn_path = os.path.join(base_path, 'conn.txt')
form_path = os.path.join(base_path, 'form/gui_crawling_youtube.ui')

from database.access_db import AccessDataBase
from thread.thread_crawling import ThreadCrawlingYoutubeUrl, ThreadCrawlingYoutubeScript
from gui.table_view import TableViewer

form = uic.loadUiType(form_path)[0]
class CrawlingYoutubeWindow(QMainWindow, form):
    ''' Product Status, Store Crawling Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Crawling Youtube Data')
        self.viewer = None
        self.power_url = True
        self.power_crw = True
        self.check_url = False
        
        # connect thread class 
        self.thread_url = ThreadCrawlingYoutubeUrl()
        self.thread_crw = ThreadCrawlingYoutubeScript()
        self.thread_url.progress.connect(self._update_progress)
        self.thread_crw.progress.connect(self.update_progress)
        
        # cache file path
        self.path_words = os.path.join(tbl_cache, 'words.txt')
        self.path_scrape_df = os.path.join(tbl_cache, 'scrapes.csv')
        self.path_prg = os.path.join(tbl_cache, 'prg_dict.txt')
        
        # Searching Words List
        makeup = ['sephora makeup', 'ulta makeup', 'sephora makeup tutorial', 'ulta makeup tutorial']
        info = ['sephora haul', 'sephora sale', 'ulta haul', 'ulta sale', 'sephora review', 'ulta review']
        skincare = ['sephora skincare', 'ulta skincare']
        fragrance = ['sephora fragrance', 'ulta fragrance']        
        searching_words = makeup + info + skincare + fragrance
        for word in searching_words:
            item = QListWidgetItem(word)
            self.SearchWordsList.addItem(item)
            
        # connect button with function
        self.Add.clicked.connect(self._add_word)
        self.Delete.clicked.connect(self._delete_word)
        
        self.Run_url.clicked.connect(self._run_url)
        self.Run.clicked.connect(self._run_crw)
        self.Pause.clicked.connect(self.thread_crw.stop)
        self.View.clicked.connect(self.tbl_viewer)
        self.Save.clicked.connect(self.save_file)
        
        
    def _update_progress(self, progress):
    
        if os.path.isfile(self.path_prg):
            with open(self.path_prg, 'rb') as f:
                prg_dict_ = pickle.load(f)
            itm_ = prg_dict_['n'] 
            elapsed_ = round(prg_dict_['elapsed'], 0)
            
        else:
            itm_, elapsed_ = 0, 0
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] + itm_
        tot = prg_dict['total'] + itm_ 
        per = round((itm / tot) * 100, 0)
        elapsed = round(prg_dict['elapsed'], 0) + elapsed_
        prg_dict_ = {
            'n': itm,
            'elapsed': elapsed,
        }
                
        if itm >= 1:
            remain_time = round((elapsed * tot / itm) - elapsed, 0)
        else:
            remain_time = 0
        
        self.progressBar_url.setValue(per)
        
        elapsed_h = int(elapsed // 3600)
        elapsed_m = int((elapsed % 3600) // 60)
        elapsed_s = int(elapsed - (elapsed_h * 3600 + elapsed_m * 60))
        
        remain_h = int(remain_time // 3600)
        remain_m = int((remain_time % 3600) // 60)
        remain_s = int(remain_time - (remain_h * 3600 + remain_m * 60))
        
        message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s}"
        self.statusbar.showMessage(message)
        
        # pause 시에 현재까지 진행률 저장
        if not self.thread_url.power:
            with open(self.path_prg, 'wb') as f:
                pickle.dump(prg_dict_, f)
            
            if itm == tot:
                message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **Complete**"
                os.remove(self.path_prg)
            else:
                message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **PAUSE**"
            self.statusbar.showMessage(message)
            
            self.power_url = True
        
        # crawling url complete
        if itm == tot:
            self.check_url = True
        
        # ip 차단 및 db 연결 끊김 대응
        if self.thread_url.check == 1:
            msg = QMessageBox()
            msg.setText("\n    ** ip 차단됨 **\n\n - VPN 나라변경 필요\n - wifi 재연결 필요")
            msg.exec_()
            self.thread_crw.check = 0
            
        elif self.thread_url.check == 2:
            msg = QMessageBox()
            msg.setText("\n    ** db 연결 끊김 **\n\n - VPN, wifi 재연결 필요\n\n - Upload 버튼 클릭 후 re-Run")
            msg.exec_()
            self.thread_crw.check = 0
        
    def update_progress(self, progress):
        
        if os.path.isfile(self.path_prg):
            with open(self.path_prg, 'rb') as f:
                prg_dict_ = pickle.load(f)
            itm_ = prg_dict_['n'] 
            elapsed_ = round(prg_dict_['elapsed'], 0)
            
        else:
            itm_, elapsed_ = 0, 0
        
        prg_dict = progress.format_dict
        itm = prg_dict['n'] + itm_
        tot = prg_dict['total'] + itm_ 
        per = round((itm / tot) * 100, 0)
        elapsed = round(prg_dict['elapsed'], 0) + elapsed_
        prg_dict_ = {
            'n': itm,
            'elapsed': elapsed,
        }
                
        if itm >= 1:
            remain_time = round((elapsed * tot / itm) - elapsed, 0)
        else:
            remain_time = 0
        
        self.progressBar.setValue(per)
        
        elapsed_h = int(elapsed // 3600)
        elapsed_m = int((elapsed % 3600) // 60)
        elapsed_s = int(elapsed - (elapsed_h * 3600 + elapsed_m * 60))
        
        remain_h = int(remain_time // 3600)
        remain_m = int((remain_time % 3600) // 60)
        remain_s = int(remain_time - (remain_h * 3600 + remain_m * 60))
        
        message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s}"
        self.statusbar.showMessage(message)
        
        # pause 시에 현재까지 진행률 저장
        if not self.thread_crw.power:
            with open(self.path_prg, 'wb') as f:
                pickle.dump(prg_dict_, f)
            
            if itm == tot:
                message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **Complete**"
                os.remove(self.path_prg)
            else:
                message = f"{int(per)}% | Progress item: {itm}  Total: {tot} | Elapsed time: {elapsed_h}:{elapsed_m}:{elapsed_s} < Remain time: {remain_h}:{remain_m}:{remain_s} **PAUSE**"
            self.statusbar.showMessage(message)
            
            self.power_crw = True
        
        # ip 차단 및 db 연결 끊김 대응
        if self.thread_crw.check == 1:
            msg = QMessageBox()
            msg.setText("\n    ** ip 차단됨 **\n\n - VPN 나라변경 필요\n - wifi 재연결 필요")
            msg.exec_()
            self.thread_crw.check = 0
        elif self.thread_crw.check == 2:
            msg = QMessageBox()
            msg.setText("\n    ** db 연결 끊김 **\n\n - VPN, wifi 재연결 필요\n\n - Upload 버튼 클릭 후 re-Run")
            msg.exec_()
            self.thread_crw.check = 0
            
    def _add_word(self):
        
        words = []
        for idx in range(self.SearchWordsList.count()):
            words.append(self.SearchWordsList.item(idx).text())
            
        word = self.SearchWord.text().strip() 
        if word == "":
            message = "Enter the word to search"
        elif word in words:
            message = "Words that already exist"
        else:
            message = None
            item = QListWidgetItem(word)
            self.SearchWordsList.addItem(item)
        
        if message is None:
            pass
        else:  
            msg = QMessageBox()
            msg.setText(message)
            msg.exec_()
            
    def _delete_word(self):
        
        words = []
        for idx in range(self.SearchWordsList.count()):
            words.append(self.SearchWordsList.item(idx).text())
            
        word = self.SearchWord.text().strip()
        if word == "":
            message = "Enter the word to delete"
        elif word in words:
            message = None
            idx = words.index(word)
            self.SearchWordsList.takeItem(idx)
        else:
            message = "Words that doesn't exist"
        
        if message is None:
            pass
        else:  
            msg = QMessageBox()
            msg.setText(message)
            msg.exec_()
            
    def _run_url(self):
        words = []
        for idx in range(self.SearchWordsList.count()):
            words.append(self.SearchWordsList.item(idx).text())
        
        # save searching words
        with open(self.path_words, 'wb') as f:
            pickle.dump(words, f)
            
        if (len(words) == 0) & (os.path.isfile(self.path_words)):
            msg = QMessageBox()
            msg.setText("More than one search term is required")
            msg.exec_()
            
        else:
            if self.power_url:
                msg = QMessageBox()
                msg.setText("- 인터넷 연결 확인 \n- VPN 연결 확인 \n- mac 자동 잠금 해제 확인 \n ** 1시간 이상 소요 예정 **")
                msg.exec_()
                self.power_url = False
                self.thread_url.power = True
                self.thread_url.start()
            else:
                msg = QMessageBox()
                msg.setText("It is already running")
                msg.exec_()
                
    def _run_crw(self):
        
        if self.check_url:
            
            if self.power_crw:
                msg = QMessageBox()
                msg.setText("- 인터넷 연결 확인 \n- VPN 연결 확인 \n- mac 자동 잠금 해제 확인")
                msg.exec_()
                self.power_crw = False
                self.thread_crw.power = True
                self.thread_crw.start()
            else:
                msg = QMessageBox()
                msg.setText("It is already running")
                msg.exec_()
    
        else:
            msg = QMessageBox()
            msg.setText("Try after collecting url")
            msg.exec_()
            
    def save_file(self):
        ''' save csv file '''
        
        # 캐시에 해당 파일이 존재할 때 저장
        if os.path.isfile(self.path_scrape_df):
            df = pd.read_csv(self.path_scrape_df)
            file_save = QFileDialog.getSaveFileName(self, "Save File", "", "csv file (*.csv)")
            
            if file_save[0] != "":
                df.to_csv(file_save[0], index=False)
        else:
            msg = QMessageBox()
            msg.setText('Please pause and try again')
            msg.exec_()
            
    def tbl_viewer(self):
        ''' table viewer '''
        
        # 캐시에 테이블이 존재할 때 open table viewer
        if os.path.isfile(self.path_scrape_df):
            if self.viewer is None:
                self.viewer = TableViewer()
            else:
                self.viewer.close()
                self.viewer = TableViewer()
                
            self.viewer.show()
            self.viewer._loadFile('scrapes.csv')
        else:
            msg = QMessageBox()
            msg.setText('Please pause and try again')
            msg.exec_()