import os
import sys
import pickle

from PyQt5.QtWidgets import QWidget, QGridLayout, QLabel, QLineEdit, QComboBox, QPushButton, QMessageBox

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
sys.path.append(root)
sys.path.append(src)

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = os.path.join(root, 'tbl_cache')

from database.access_db import AccessDataBase
from gui.gui_get_table import GetTableWindow
from gui.gui_crawling_youtube import CrawlingYoutubeWindow


conn_path = os.path.join(base_path, 'conn.txt')
        
class MainWidget(QWidget):
    ''' Database Connect Form '''
    
    def __init__(self):
        super().__init__()
        self.version = "1.0.0"
        self.w0 = None
        self.w1 = None
        # self.w2 = None
        # self.w3 = None
        # self.w4 = None
        # self.w5 = None
        # self.w6 = None
        self.setWindowTitle('Connect Database')
        self.resize(475, 250)

        layout = QGridLayout()

        #
        label_name = QLabel('<font size="4"> Username </font>')
        self.lineEdit_username = QLineEdit()
        self.lineEdit_username.setPlaceholderText('Please enter your username')
        layout.addWidget(label_name, 0, 0)
        layout.addWidget(self.lineEdit_username, 0, 1)

        #
        label_password = QLabel('<font size="4"> Password </font>')
        self.lineEdit_password = QLineEdit()
        self.lineEdit_password.setPlaceholderText('Please enter your password')
        layout.addWidget(label_password, 1, 0)
        layout.addWidget(self.lineEdit_password, 1, 1)

        #
        label_database = QLabel('<font size="4"> Database </font>')
        self.lineEdit_database = QLineEdit()
        self.lineEdit_database.setPlaceholderText('Please enter database')
        layout.addWidget(label_database, 2, 0)
        layout.addWidget(self.lineEdit_database, 2, 1)
        
        #
        self.comb_menu = QComboBox()
        layout.addWidget(self.comb_menu, 3, 1)
        self.comb_menu.addItem('Crawling Youtube Data')
        self.comb_menu.addItem('Get Table from Database')
        self.comb_menu.move(50, 50)
        
        #
        btn_conn = QPushButton('Connect')
        btn_conn.clicked.connect(self.check_info)
        layout.addWidget(btn_conn, 5, 0, 1, 2)
        layout.setRowMinimumHeight(3, 75)
        
        version = QLabel(f'<font size="2"> version {self.version} </font>')
        layout.addWidget(version, 6, 0)

        self.setLayout(layout)
        
    # Select Menu 
    def select_menu(self):
        ''' 메뉴 선택 함수 '''
        
        menu_index = self.comb_menu.currentIndex()
        return menu_index
        
    def check_info(self):
        ''' db 연결 정보 확인 함수 '''
        
        msg = QMessageBox()
        conn = {
            0: self.lineEdit_username.text(),
            1: self.lineEdit_password.text(),
            2: self.lineEdit_database.text(),
        }
        db = AccessDataBase(conn[0], conn[1], conn[2])
        try:
            curs = db.db_connect()
            msg.setText(f'Database Connection Successful \n - {self.lineEdit_database.text()}')
            msg.exec_()
            curs.close()
            
            # save connect info 
            with open(conn_path, 'wb') as f:
                pickle.dump(conn, f)
            
            menu_index = self.select_menu()
            if menu_index == 0:
                if self.w0 is None:
                    self.w0 = CrawlingYoutubeWindow()
                    self.w0.show()
                else:
                    pass
                
            elif menu_index == 1:
                if self.w1 is None:
                    self.w1 = GetTableWindow()
                    self.w1.show()
                else:
                    pass
                
            # elif menu_index == 2:
            #     if self.w2 is None:
            #         self.w2 = CrawlingGlWindow()
            #         self.w2.show()
            #     else:
            #         pass
                
            # elif menu_index == 3:
            #     if self.w3 is None:
            #         self.w3 = CrawlingNvStatus()
            #         self.w3.show()
            #     else:
            #         pass
                
            # elif menu_index == 4:
            #     if self.w4 is None:
            #         self.w4 = CrawlingNvRevWindow()
            #         self.w4.show()
            #     else:
            #         pass
            
            # elif menu_index == 5:
            #     if self.w5 is None:
            #         self.w5 = GetTableWindow()
            #         self.w5.show()
            #     else:
            #         pass
                
            # elif menu_index == 6:
            #     if self.w6 is None:
            #         self.w6 = ReviewWindow()
            #         self.w6.show()
            #     else:
            #         pass
                
        except Exception as e:
            msg.setText(f'{e}\n\n** VPN 연결 해제 후 로그인 **')
            msg.exec_()