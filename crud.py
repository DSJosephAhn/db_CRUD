from CRUD.db_load import db_connection

class TmpDB():
    def  __init__(self):
        tmp_db= 'DB_LOCAL'
        tmp_conn= db_connection(tmp_db)

        self.db= tmp_conn
        self.cursor= self.db.cursor()

    def __del__(self):
        self.db.close()
        self.cursor.close()

    def execute(self,query,args={}):
        self.cursor.execute(query,args)
        row = self.cursor.fetchall()
        return row

    def commit(self):
        self.cursor.commit()

class myCRUD(TmpDB):
    # Create
    def createTable(self,schema,table,colum):
        sql = " CREATE TABLE IF NOT EXISTS {schema}.{table} ({colum}) ;".format(schema=schema,table=table,colum=colum)
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(" create Table ",e)
    
    # INSERT
    def insertDB(self,schema,table,colum,data):
        sql = " INSERT INTO {schema}.{table}({colum}) VALUES ('{data}');".format(schema=schema,table=table,colum=colum,data=data)
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(" insert DB ",e)

    # Read(SELECT)
    def readDB(self,schema,table,colum):
        sql = " SELECT {colum} from {schema}.{table}".format(colum=colum,schema=schema,table=table)
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
        except Exception as e :
            result = (" read DB err",e)

        return result

    # Update
    def updateDB(self,schema,table,colum,value,condition):
        sql = " UPDATE {schema}.{table} SET {colum}='{value}' WHERE {colum}='{condition}' ".format(schema=schema, table=table , colum=colum ,value=value,condition=condition )
        try :
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(" update DB err",e)

    # Delete
    def deleteDB(self,schema,table,condition):
        sql = " delete from {schema}.{table} where {condition} ;".format(schema=schema,table=table, condition=condition)
        try :
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print( "delete DB err", e)

    def countTableRow(self,schema,table):
        self.cursor.execute('SELECT COUNT(*) FROM {schema}.{table}')
        self.db.commit()
        return self.cursor.fetchone()[0]