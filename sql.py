import sqlite3
import psycopg2
import csv
from os import remove
from os.path import getsize, exists

class Instance():
    
    def __init__(self, conn):
        self.conn = conn
        self.curs = self.conn.cursor()


    def __call__(self, q, *args, **kwargs):
        return self.execute(q, *args, **kwargs)
        

    def close(self):
        self.conn.commit()
        self.conn.close()


    def execute(self, q, *args, unpack=False,  p=False):
        if 'select' in q.lower()[:10]:
            self.curs.execute(q, args)
            r = self.curs.fetchall()
            if unpack:
                r = [r[0] for r in r]
            if p:
                print(r)
            return r
        else:
            self.curs.execute(q, args)
            self.conn.commit()


    def executemany(self, q, *args, p=False):
        self.curs.execute(q, args)
        self.conn.commit()



class notInstance():

    def __init__(self, conn_method, db_path=None, **kwargs):
        """ 
        conn_method is the connect method for the class. ie: sqlite3.connect 
        if using sqlite, set db_path to a relative or absolute path to your database file 
        else pass all keyword arguments implimented for other connect methods
        """
        self.conn_method =  conn_method
        self.db_path = db_path
        self.__dict__.update(kwargs)
        self.attrs = {k:v for k,v in kwargs.items()}
        


    def __call__(self, q, *args, **kwargs):
        return self.execute(q, *args, **kwargs)
        

    def open(self):
        if self.db_path:
            self.conn = self.conn_method(self.db_path)
        else:
            self.conn = self.conn_method(**self.attrs)
        self.curs = self.conn.cursor()


    def close(self):
        self.conn.commit()
        self.conn.close()


    def execute(self, q, *args, p=False, unpack=False):
        self.open()
        if 'select' in q.lower()[:10]:
            self.curs.execute(q, *args)
            r = self.curs.fetchall()
            if unpack:
                r = [r[0] for r in r]
            if p:
                print(r)
            self.close()
            return r
        else:
            self.curs.execute(q, *args)
            self.close()


    def executemany(self, q, *args):
        self.open()
        self.curs.executemany(q, *args)
        self.close()





class SQL:

    def dictionary_dump(self, d:dict, table:str, rows:list):
        """ 
        d is a dictionary representation of data you are trying to store 
        d keys will be used to determine column names and # of columns in table
        all columns will be TEXT datatype
        rows should be a executemany ready list of data tuples to populate table with
        """
        self.execute(f'CREATE TABLE IF NOT EXISTS data {tuple(d.keys())}')
        self.executemany(f'INSERT INTO data VALUES ({",".join(["?"] * len(list(d.keys())))})',
                            rows)


    def tables(self):
        return self.execute("SELECT name FROM sqlite_master WHERE type='table';", unpack=True)


    def columns(self, table):
        return self.execute(f"SELECT name FROM PRAGMA_TABLE_INFO('{table}');", unpack=True)


    def create_table_from_dict(self, table, d, sort=False, unique=[]):
        def parse(obj):
            cls = str(obj.__class__)
            if 'int' in cls or 'bool' in cls:
                return 'INTEGER'
            return 'TEXT'

        keys = d.keys()
        if sort:
            keys = sorted(keys)

        query = f'CREATE TABLE IF NOT EXISTS {table} ('
        s = ''
        for k in keys:
            dtype = parse(d[k])
            col = f"{k} {dtype}"
            if k in unique:
                col += ' UNIQUE'
            s += f"{col}, "
        query += s[:-2] + ');'
        try:
            self.execute(query)
        except Exception as e:
            print(query)
            raise e



class PSQL():

    def dictionary_dump(self, d:dict, table:str, rows:list):
        """ 
        d is a dictionary representation of data you are trying to store 
        d keys will be used to determine column names and # of columns in table
        all columns will be TEXT datatype
        rows should be a executemany ready list of data tuples to populate table with
        """
        self.execute(f'CREATE TABLE IF NOT EXISTS data {tuple(d.keys())}')
        self.executemany(f'INSERT INTO data VALUES ({",".join(["%s"] * len(list(d.keys())))})',
                            rows)


    def text_insert(self, data, table, delim='`'):
        """
        data should be a list of tuples representing rows
        table will be used for filename. 
        """
        table = table.replace('-','_')
        file_name = os.path.join(r'C:\Users\Public\Temp', f"{table}.txt")
        with open(file_name, 'w+') as f:
            for d in data:
                f.write(delim.join(d) + '\n')


    def text_dump(self, table, delim='`'):
        """
        table will be used for filename. should mirror table used for text_dump function
        will delete text file after insertion
        """
        table = table.replace('-','_')
        file_name = os.path.join(r'C:\Users\Public\Temp', f"{table}.txt")
        if exists(file_name):
            if getsize(file_name) > 1e+9: # 1gb max before to db. ensures large files can insert
                self.csv_dump(table, cols)

        self.execute(f"COPY {table} FROM '{file_name}' (FORMAT TEXT, DELIMITER('{delim}') )")
        remove(file_name)


    def csv_insert(self, data:list, table:str, cols=[], delim='`', quotechar='"'):
        """ 
        will append data to csv file used in csv_dump function 
        csv_dump function will then perform bulk insert for efficient data insertion
        'data' argument should be a list of rows ie: [(a,1),(b,2),(c,3)]
        row data should be in default insertion format for that table 
        table is used for filename, must mirror table used for csv_dump function
        """
        table = table.replace('-','_')
        file_name = os.path.join(r'C:\Users\Public\Temp', f"{table}.txt")
        if exists(file_name):
            if getsize(file_name) > 1e+9: # 1gb max before to db. ensures large files can insert
                self.csv_dump(table, cols)

        with open(file_name, 'a+', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=delim,
                                    quotechar=quotechar, quoting=csv.QUOTE_MINIMAL)
            for d in data:
                writer.writerow(d)


    def csv_dump(self, table, col_defs=[], delim='`', quotechar='"'):
        """
        table will be used for filename. should mirror table used for text_dump function
        will delete text file after insertion
        
        create table before performing using this function 
        OR
        provied col_defs as a flat list of the columns and their data_types seperated by a space
            ie: col_defs  = ('author TEXT', 'body TEXT', 'created_utc INTEGER')
        """
        table = table.replace('-','_')
        file_name = os.path.join(r'C:\Users\Public\Temp', f"{table}.csv")
        if col_defs:
            self.execute(f"""CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)});""")
        self.execute(f"""COPY {table} FROM '{file_name}' (FORMAT CSV, DELIMITER('{delim}'), QUOTE('{quotechar}'), ENCODING('UTF-8') )""")
        remove(f"{file_name}")
            

    def tables(self):
        return self.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';", unpack=True)


    def columns(self, table):
        return self.execute("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'some_table';", unpack=True)


    def drop_all_tables(self):
        return self.execute("SELECT 'DROP TABLE IF EXISTS "' || tablename || '" CASCADE;' from pg_tables WHERE schemaname = 'public'; ")


    def create_table_from_dict(self, table, d, sort=False, unique=[]):
        def parse(obj):
            cls = str(obj.__class__)
            if 'int' in cls or 'bool' in cls:
                return 'INTEGER'
            return 'TEXT'

        keys = d.keys()
        if sort:
            keys = sorted(keys)

        query = f'CREATE TABLE IF NOT EXISTS {table} ('
        s = ''
        for k in keys:
            dtype = parse(d[k])
            col = f"{k} {dtype}"
            if k in unique:
                col += ' UNIQUE'
            s += f"{col}, "
        query += s[:-2] + ');'
        try:
            self.execute(query)
        except Exception as e:
            print(query)
            raise e


    def mogrify(self, data:list) -> str:
        # data is list of rows
        return ','.join(self.curs.mogrify('%s', row).decode('utf-8') for row in data)




    
class psqlInstance(PSQL, Instance):

    def __init__(self, *args, **kwargs):
        super().__init__(psycopg2.connect(*args, **kwargs))


class psql(PSQL, notInstance):

    def __init__(self, *args, **kwargs):
        super().__init__(psycopg2.connect, *args, **kwargs)


class sqlInstance(SQL, Instance):

    def __init__(self, db_path, *args, **kwargs):
        super().__init__(sqlite3.connect(db_path), *args, **kwargs)


class sql(SQL, notInstance):

    def __init__(self, *args, **kwargs):
        super().__init__(sqlite3.connect, *args, **kwargs)