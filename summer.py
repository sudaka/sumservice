import pandas as pd
import numpy as np
import hashlib
import time
import os
import os.path
import json
from dnslib import DNSRecord, DNSBuffer, DNSHeader, DNSQuestion, RR
import requests

#python -m pip install flask

class DataConnector():
    def __init__(self) -> None:
        self.result = {"result": "Initial state don't change"}
        with open("settings.json") as f:
            settings = json.load(f)
            for (name, value) in settings.items():
                self.__setattr__(name, value)

    def getaddr(self, fdns,dnsserver):
        q = DNSRecord.question(fdns,"A")
        q.pack()
        packet = q.send(dnsserver)
        #a = DNSRecord.parse()
        rr = []
        iptable = []
        buffer = DNSBuffer(packet)
        try:
            header = DNSHeader.parse(buffer)
            for _ in range(header.q):
                    DNSQuestion.parse(buffer)
            for _ in range(header.a):
                rr.append(RR.parse(buffer))
            for info in rr:
                if info.rname == fdns + '.':
                    iptable.append(info.rdata.toZone())
            return iptable
        except:
            return []
    
    def loadremotejobscount(self):
        addrlist = self.getaddr(self.domainname, self.dnsserver)
        for tmpip in ['127.0.0.1', self.myip]:
            if tmpip in addrlist:
                addrlist.remove(tmpip)
        jobscount = 0
        for addr in addrlist:
            res = requests.get(f'http://{addr}:{self.apiport}/getjobcount?local=yes')
            try:
                tmpjobscount = json.loads(res.text)
                jobscount += int(tmpjobscount["result"])
            except:
                pass
        return jobscount

    def loadremotejobs(self):
        addrlist = self.getaddr(self.domainname, self.dnsserver)
        for tmpip in ['127.0.0.1', self.myip]:
            if tmpip in addrlist:
                addrlist.remove(tmpip)
        jobid = self.result["id"]
        for addr in addrlist:
            try:
                res = requests.get(f'http://{addr}:{self.apiport}/getjobinfo?id={jobid}&local=yes')
                jobinfo = json.loads(res.text)
                if jobinfo[jobid]["result"] != "Job not in list":
                    self.result = jobinfo[jobid]
                    return True
            except:
                pass
        return False

    def loadjobs(self):
        try:
            with open(self.jobsfile) as jobsfile:
                jobs = json.load(jobsfile)
                return jobs
        except:
            return dict()

    def savejobs(self, jobs):
        try:
            with open(self.jobsfile, 'w') as f:
                json.dump(jobs, f)
            return True
        except:
            return False

    def createjob(self):
        jobs = self.loadjobs()
        self.result["id"] = hashlib.md5(self.fname.encode()).hexdigest()
        if self.result["id"] in jobs.keys():
            self.result["result"] = 'Job in list'
            return True    
        remjobs = self.loadremotejobs()
        if self.result["id"] in remjobs.keys():
            self.result["result"] = 'Job in list'
            return True
        jobs[self.result["id"]] = {"server":self.myip, "fname": self.fname, "result":"Job in list"}
        if self.savejobs(jobs):
            self.result["result"] = 'Job in list'
            self.result["filename"] = self.fname
            return True
        else:
            return False
    
    def getjobinfo(self, local=False):
        jobs = self.loadjobs()
        if self.result["id"] in jobs.keys():
            self.result = jobs[self.result["id"]]
            return True
        if not local:
            remjobs = self.loadremotejobs()
            if remjobs:
                return True
        return False 

    def getjobcount(self, local=False):
        count = -1
        jobs = self.loadjobs()
        count = len(jobs)
        if not local:
            remjobs = self.loadremotejobscount()
            count += remjobs
        self.result["result"] = count
        if count > -1 :
            return True
        return False

    def check_localfilename(self, fname):
        try:
            if not os.path.isfile(fname):
                raise
            return True
        except:
            self.result["result"] = "Bad file name."
            return False

    def summlocalfile(self):
        #формируем список номеров столбцов для считывания
        try:
            colslist = []
            with open(self.fname, encoding="UTF-8") as myfile:
                firstline = myfile.readline()
                cols = firstline.split(',')
                if len(cols)>9:
                    colslist=[i for i in range(0,len(cols)+1,10)]
                colslist.remove(0)
            raw = pd.read_csv(self.fname, encoding="UTF-8", quoting=3, usecols=colslist)
            #raw = pd.read_csv(fname, encoding="UTF-8", quoting=3) # Вышибло при загрузке файла более 3,86 ГБ
            raw.rename(columns=lambda x: x.replace('"', ''), inplace=True)
            self.data = raw.applymap(lambda x: x.replace('"', ''))
            self.data = self.data.replace('', np.nan)
            self.data = self.data.fillna(0)
            self.data=self.data.astype('float', errors='raise')
            #print(self.data.dtypes)
            self.result["summ"] = json.loads(self.data.sum(axis=0).to_json(orient='index'))
            self.result["result"] = "ok"
            #print(self.summ['col9'])
            #print(self.result)
        except:
            self.result["result"] = "Bad in data summing."

    def loadfile(self):
        if self.fname.startswith('http'):
            #функция сохранения файла в локальное хранилище и передачи 
            pass #print(hash_object.hexdigest()+'.csv')
        else:
            if self.check_localfilename(self.fname):
                copycmd = f'cp {self.fname} {self.tmpdir}{self.result["id"]}'
                if os.name == "nt":
                    copycmd = f'copy {self.fname} {self.tmpdir}{self.result["id"]}'
                i=0
                while i<3:
                    try:
                        os.system(copycmd)
                    except:
                        i += 1
                if i == 3:
                    self.result["result"] = "Bad in file copying."
                    return False
                self.result["result"] = "File loaded."
                self.summlocalfile(self.fname)
            else:
                return False
        return True

if __name__ == '__main__':
    tr = DataConnector()
    #tr.loadfile()
    #print(tr.data.)
    #print(tr.data(1))

