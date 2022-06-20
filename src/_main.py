import os
import sys
from gui.gui_main import MainWidget
from PyQt5.QtWidgets import QApplication


# pyinstaller에 의한 패키징이 여부에 따른 경로 설정 
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    base_path = sys._MEIPASS
    tbl_cache_dir = os.path.join(base_path, 'tbl_cache_')
    
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    base_path = os.path.abspath(os.path.join(cur_dir, os.pardir))
    tbl_cache_dir = os.path.join(base_path, 'tbl_cache')
    
# 테이블 캐시 데이터 저장 디렉토리
if not os.path.exists(tbl_cache_dir):
    os.makedirs(tbl_cache_dir)
    
# base_path
print(f'\n\nroot: {base_path}\n\n')

''' main gui execution '''
app = QApplication(sys.argv)
form = MainWidget()
form.show()
sys.exit(app.exec_())