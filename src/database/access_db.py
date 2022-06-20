# necessary
import pandas as pd
import time
from datetime import datetime

# db connection 
import pymysql
import sqlalchemy

class AccessDataBase:
    
    def __init__(self, user_name, password, db_name):
        # user info & db
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
    
        # today 
        today = datetime.today()
        year = str(today.year)
        month = str(today.month)
        day = str(today.day)
        if len(month) == 1:
            month = "0" + month
        if len(day) == 1:
            day = "0" + day    
        self.date = year[2:4] + month + day
        self.regist_date = year + "-" + month + "-" + day
        
    def db_connect(self):
        ''' db connect '''

        host_url = "db.ds.mycelebs.com"
        port_num = 3306
        conn = pymysql.connect(host=host_url, user=self.user_name, passwd=self.password, port=port_num, db=self.db_name, charset='utf8')
        curs = conn.cursor(pymysql.cursors.DictCursor)
        return curs

    def get_tbl_name(self):
        ''' db에 존재하는 모든 테이블 이름 가져오기 '''

        curs = self.db_connect()

        # get table name list
        query = "SHOW TABLES;"
        curs.execute(query)
        tables = curs.fetchall()

        table_list = []
        for table in tables:
            tbl = list(table.values())[0]
            table_list.append(tbl)
        curs.close()
        
        return table_list

    def get_tbl_columns(self, table_name):
        ''' 선택한 테이블 컬럼 가져오기 '''
        
        curs = self.db_connect()

        # get table columns 
        query = f"SHOW FULL COLUMNS FROM {table_name};"
        curs.execute(query)
        columns = curs.fetchall()

        column_list = []
        for column in columns:
            field = column['Field']
            column_list.append(field)
        curs.close()
        
        return column_list

    def get_tbl(self, table_name, columns='all'):
        ''' db에서 원하는 테이블, 컬럼 pd.DataFrame에 할당 '''
        
        if table_name in self.get_tbl_name():
            st = time.time()
            curs = self.db_connect()
            
            if columns == 'all':
                query = f'SELECT * FROM {table_name};'
            else:
                # SELECT columns
                query = 'SELECT '
                i = 0
                for col in columns:
                    if i == 0:
                        query += f"`{col}`"
                    else:
                        query += ', ' + f"`{col}`"
                    i += 1

                # FROM table_name
                query += f' FROM {table_name};'
            curs.execute(query)
            tbl = curs.fetchall()
            df = pd.DataFrame(tbl)
            curs.close()
            
            ed = time.time()
            print(f'`{table_name}` Import Time: {round(ed-st, 1)}sec\n\n')
        else:
            df = None
            print(f'\n\n{table_name} does not exist in db')
        
        return df
    
    def integ_tbl(self, table_name_list, columns):
        ''' 
        db에서 컬럼이 같은 여러개 테이블 가져오기
        db에서 테이블 가져온 후 데이터 프레임 통합 (concat)
        '''

        df = pd.DataFrame()
        for tbl in table_name_list:
            df_ = self.get_tbl(tbl, columns)
            df_.loc[:, 'table_name'] = tbl
            df = pd.concat([df, df_])
        df = df.reset_index(drop=True)
        return df

    def sqlcol(self, dfparam):    
        ''' Convert DataFrame data type to sql data type '''
        
        dtypedict = {}
        for i,j in zip(dfparam.columns, dfparam.dtypes):
            
            if "object" in str(j):
                dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})
                                    
            if "datetime" in str(j):
                dtypedict.update({i: sqlalchemy.types.DateTime()})

            if "float" in str(j):
                dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

            if "int" in str(j):
                dtypedict.update({i: sqlalchemy.types.INT()})

        return dtypedict

    def engine_upload(self, upload_df, table_name, if_exists_option, pk=None):
        ''' Create Table '''
        
        # engine
        host_url = "db.ds.mycelebs.com"
        port_num = 3306
        engine = sqlalchemy.create_engine(f'mysql+pymysql://{self.user_name}:{self.password}@{host_url}:{port_num}/{self.db_name}?charset=utf8mb4')
        
        # Create table or Replace table 
        upload_df.to_sql(table_name, engine, if_exists=if_exists_option, index=False)
        
        # Setting pk 
        if pk != None:
            engine.execute(f'ALTER TABLE {table_name} ADD PRIMARY KEY (`{pk}`);')
        else:
            pass
        engine.dispose()
        print(f'\nTable Upload Success: {table_name}')
        
    def drop_table(self, table_name):
        ''' Drop Table '''
        
        if table_name in self.get_tbl_name:
            curs = self.db_connect()
            curs.execute(f'DROP TABLE {table_name};')
            curs.close()
        else:
            pass

    def table_update(self, table_name, pk, df):
        ''' Table Update from DB
        
        table_name: table name from db
        pk: primary key
        df: dataframe to update 
        
        '''
        try:
            # get table from db
            _df = self.get_tbl(table_name, 'all')
                    
            # 기존에 존재하는 status값 update
            df_update = _df.loc[:, [pk]].merge(df, on=pk, how='inner')

            # 새로운 status값 append
            df_dedup = pd.concat([_df, df]).drop_duplicates(subset=pk, keep=False)
            df_append = pd.concat([df_update, df_dedup]).sort_values(by=pk).reset_index(drop=True)
            
            self.engine_upload(df_append, table_name, "replace", pk=pk)
            
        except Exception as e:
            # 신규 테이블 업로드
            print(e)
            df = df.sort_values(by=pk).reset_index(drop=True)
            self.engine_upload(df, table_name, "replace", pk=pk)
            
    def table_backup(self, table_name):
        
        curs = self.db_connect()
        
        table_list = self.get_tbl_name()
        if table_name in table_list:
            new_table_name = f'{table_name}_backup_{self.date}'
            
            # 백업 테이블이 이미 존재하는경우 replace(drop -> insert)
            if new_table_name in table_list:
                curs.execute(f'DROP TABLE {new_table_name};')
                
            query = f'ALTER TABLE {table_name} RENAME {new_table_name};'
            curs.execute(query)
        else:
            pass
        curs.close()
        
    def create_table(self, upload_df, table_name):
        ''' Create table '''
        
        if 'info_all' in table_name:
            category = table_name.replace('beauty_kr_', '').replace('_info_all', '')
        elif 'reviews_all' in table_name:
            category = table_name.replace('beauty_kr_', '').replace('_reviews_all', '')
        else:
            category = ""
            
        query_dict = {
            'youtube_video_collect': f"CREATE TABLE `youtube_video_collect` (\
                                    `video_pk` int(11) NOT NULL AUTO_INCREMENT,\
                                    `url` varchar(255) DEFAULT NULL,\
                                    `video_title` varchar(255) DEFAULT NULL,\
                                    `video_id` varchar(100) DEFAULT NULL,\
                                    `thumbnail` varchar(255) DEFAULT NULL,\
                                    `youtuber` varchar(255) DEFAULT NULL,\
                                    `youtuber_profile` varchar(255) DEFAULT NULL,\
                                    `script` text,\
                                    `regist_date` datetime DEFAULT NULL COMMENT '데이터 수집 일자',\
                                    PRIMARY KEY (`video_pk`)\
                                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        }
        
        if 'info_all' in table_name:
            query = query_dict[f'beauty_kr_{category}_info_all']
        elif 'reviews_all' in table_name:
            query = query_dict[f'beauty_kr_{category}_reviews_all']
        else:
            if table_name in list(query_dict.keys()):
                query = query_dict[table_name]
            else:
                query = None
        
        if query == None:
            print('query is None')
        else:
            # backup table
            self.table_backup(table_name)
            
            # create table
            curs = self.db_connect()
            curs.execute(query)
            
            # upload table
            self.engine_upload(upload_df, table_name, if_exists_option='append')
            
            # drop temporary table
            table_list = self.get_tbl_name()
            if  f'{table_name}_temp' in table_list:
                curs.execute(f'DROP TABLE {table_name}_temp;')
            
            # close cursor
            curs.close()