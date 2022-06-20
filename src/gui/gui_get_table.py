import os
import sys
import pickle
import pandas as pd

cur_dir = os.path.dirname(os.path.realpath(__file__))
root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
src = os.path.abspath(os.path.join(cur_dir, os.pardir))
sys.path.append(root)
sys.path.append(src)

from database.access_db import AccessDataBase
from gui.table_view import TableViewer

from PyQt5 import uic
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog, QListWidgetItem
from PyQt5.QtCore import Qt

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    base_path = sys._MEIPASS
    tbl_cache = os.path.join(base_path, 'tbl_cache_')
    
else:
    base_path = os.path.dirname(os.path.realpath(__file__))
    tbl_cache = os.path.join(root, 'tbl_cache')

conn_path = os.path.join(base_path, 'conn.txt')
form_path = os.path.join(base_path, 'form/get_table_db.ui')

mapping_form = uic.loadUiType(form_path)[0]

class GetTableWindow(QMainWindow, mapping_form):
    ''' Product Mapping Window '''
    
    def __init__(self):
        super().__init__()    
        self.setupUi(self)
        self.setWindowTitle('Get Table From Database')
        self.viewer = None
        
        # db 연결
        with open(conn_path, 'rb') as f:
            conn = pickle.load(f)
        self.db = AccessDataBase(conn[0], conn[1], conn[2])
        
        # db에 존재하는 모든 테이블 이름 출력
        for table in self.db.get_tbl_name():
            self.tables.addItem(table)
        
        # 테이블 선택
        self.Select.clicked.connect(self._get_columns)
        
        # 선택된 컬럼에 대한 테이블 출력 
        self.Import.clicked.connect(self._get_table)
        
        # table view & save
        self.View.clicked.connect(self._viewer)
        self.Save.clicked.connect(self._save)
        
    def _select_table(self):
        ''' 선택한 테이블명 리턴 '''
        
        table_name = str(self.tables.currentText())
        return table_name
    
    def _get_columns(self):
        ''' 선택한 테이블의 모든 컬럼 출력 '''
        
        self.columns.clear() # init
        table_name = self._select_table()
        column_list = self.db.get_tbl_columns(table_name)
        for column in column_list:
            item = QListWidgetItem(column)
            item.setCheckState(Qt.Unchecked)
            self.columns.addItem(item)
    
    def _select_columns(self):
        ''' 필요한 컬럼만 선택해서 리스트에 할당 후 리턴 '''
        
        column_list = []
        for idx in range(self.columns.count()):
            if self.columns.item(idx).checkState() == Qt.Checked:
                column_list.append(self.columns.item(idx).text())
                
        return column_list
        
    def _get_table(self):
        ''' 선택한 테이블, 컬럼 db에서 가져오기 '''
        
        column_list = self._select_columns()
        
        
        """
        test
        """
        print(f"\n\n{column_list}\n\n")
        
        # 컬럼을 선택하지 않으면 모든 컬럼이 선택되거나 import를 취소
        if len(column_list) == 0:
            msg = QMessageBox.question(
                self, "Question", "테이블의 모든 컬럼을 가져오겠습니까?",
                QMessageBox.Cancel | QMessageBox.Ok,
                QMessageBox.Cancel
            )
            if msg == QMessageBox.Ok:
                table_name = self._select_table()
                column_list = "all"
                df = self.db.get_tbl(table_name, column_list)
                df.to_csv(tbl_cache + f"/{table_name}.csv", index=False)
                msg = QMessageBox()
                msg.setText(f'Table import success')
                msg.exec_()
            else:
                pass
            
        else:
            table_name = self._select_table()
            df = self.db.get_tbl(table_name, column_list)
            df.to_csv(tbl_cache + f"/{table_name}.csv", index=False)
            msg = QMessageBox()
            msg.setText(f'Table import success')
            msg.exec_()
        
    def save_file(self, table_name):
        ''' save csv file '''
        file_name = table_name + ".csv"
        file_path = os.path.join(tbl_cache, file_name)
        # 캐시에 해당 파일이 존재할 때 저장
        if os.path.isfile(file_path):
            df = pd.read_csv(file_path, lineterminator='\n')
            file_save = QFileDialog.getSaveFileName(self, "Save File", table_name, "csv file (*.csv)")
            
            if file_save[0] != "":
                df.to_csv(file_save[0], index=False)
        else:
            msg = QMessageBox()
            msg.setText('테이블 가져오기 완료 후 시도하세요')
            msg.exec_()
            
    def tbl_viewer(self, file_name):
        ''' table viewer '''
        
        # 캐시에 테이블이 존재할 때 open table viewer 
        file_path = os.path.join(tbl_cache, file_name)
        if os.path.isfile(file_path):
            if self.viewer is None:
                self.viewer = TableViewer()
            else:
                self.viewer.close()
                self.viewer = TableViewer()
                
            self.viewer.show()
            self.viewer._loadFile(file_name)
        else:
            msg = QMessageBox()
            msg.setText('테이블 가져오기 완료 후 시도하세요')
            msg.exec_()
        
    def _save(self):
        table_name = self._select_table()
        self.save_file(table_name)
        
    def _viewer(self):
        file_name = self._select_table() + ".csv"
        self.tbl_viewer(file_name)
