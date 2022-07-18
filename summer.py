from fileinput import filename
import pandas as pd
import numpy as np

class DataConnector():
    def __init__(self) -> None:
        pass

    def loadlocalfile(self, fname):
        #формируем список номеров столбцов для считывания
        colslist = []
        with open(fname, encoding="UTF-8") as myfile:
            firstline = myfile.readline()
            cols = firstline.split(',')
            if len(cols)>9:
                colslist=[i for i in range(0,len(cols)+1,10)]
            colslist.remove(0)
        raw = pd.read_csv(fname, encoding="UTF-8", quoting=3, usecols=colslist, na_values=['0'])
        raw.rename(columns=lambda x: x.replace('"', ''), inplace=True)
        
        self.data = raw.applymap(lambda x: x.replace('"', ''))
        self.data = self.data.replace(r'', np.nan, regex=True)
        self.data = self.data.fillna(0)
        self.data=self.data.astype('float', errors='raise')
        print(self.data.dtypes)
        self.summ = self.data.sum(axis=0)
        print(self.summ)

if __name__ == '__main__':
    tr = DataConnector()
    tr.loadlocalfile('data.csv')
    print(tr.data.head())
    #print(tr.data.)
    #print(tr.data(1))

