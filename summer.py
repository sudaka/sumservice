from fileinput import filename
import re
import pandas as pd
import numpy as np
import hashlib
import time
import os.path
import json
#python -m pip install flask

class DataConnector():
    def __init__(self) -> None:
        self.result = {"result": False, "description": "Initial state don't change"}

    def check_localfilename(self, fname):
        try:
            if not os.path.isfile(fname):
                raise
            return True
        except:
            self.result["result"] = False
            self.result["description"] = "Bad file name."
            return False

    def loadlocalfile(self, fname):
        #формируем список номеров столбцов для считывания
        try:
            colslist = []
            with open(fname, encoding="UTF-8") as myfile:
                firstline = myfile.readline()
                cols = firstline.split(',')
                if len(cols)>9:
                    colslist=[i for i in range(0,len(cols)+1,10)]
                colslist.remove(0)
            raw = pd.read_csv(fname, encoding="UTF-8", quoting=3, usecols=colslist)
            #raw = pd.read_csv(fname, encoding="UTF-8", quoting=3) # Вышибло при загрузке файла более 3,86 ГБ
            raw.rename(columns=lambda x: x.replace('"', ''), inplace=True)
            self.data = raw.applymap(lambda x: x.replace('"', ''))
            self.data = self.data.replace('', np.nan)
            self.data = self.data.fillna(0)
            self.data=self.data.astype('float', errors='raise')
            #print(self.data.dtypes)
            self.result["summ"] = json.loads(self.data.sum(axis=0).to_json(orient='index'))
            self.result["result"] = True
            self.result["description"] = "ok"
            #print(self.summ['col9'])
            #print(self.result)
        except:
            self.result["description"] = "Bad in data summing."

    def loadfile(self, fname):
        self.result["filename"] = fname
        hash_object = hashlib.md5(fname.encode())
        self.result["jobid"] = hash_object.hexdigest()
        if fname.startswith('http'):
            #функция сохранения файла в локальное хранилище и передачи 
            print(hash_object.hexdigest()+'.csv')
        else:
            if self.check_localfilename(fname):
                self.loadlocalfile(fname)
            else:
                return False
        
        
        return True

if __name__ == '__main__':
    tr = DataConnector()
    tr.loadfile('data.csv')
    #print(tr.data.)
    #print(tr.data(1))

