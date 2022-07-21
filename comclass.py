import pandas as pd
import numpy as np
import hashlib
import os
import os.path
import json
from dnslib import DNSRecord, DNSBuffer, DNSHeader, DNSQuestion, RR
import requests
import csv

#python -m pip install flask

class Settings:
    def __init__(self, myobj) -> None:
        try:
            with open("settings.json") as f:
                settings = json.load(f)
                for (name, value) in settings.items():
                    myobj.__setattr__(name, value)
            myobj.errorloadsettings = False
        except:
            myobj.errorloadsettings = True

class Jobs():
    def __init__(self) -> None:
        self.result = {"result": "Initial state don't change"}
        Settings(self)

    def getaddr(self, fdns,dnsserver):
        q = DNSRecord.question(fdns,"A")
        q.pack()
        packet = q.send(dnsserver)
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
        if self.loadremotejobs():
            self.result["result"] = 'Job in list'
            return True
        jobs[self.result["id"]] = {"server":self.myip, "fname": self.fname, "result":"Job in list","state":"0"}
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
        count = 0
        jobs = self.loadjobs()
        for jobid, job in jobs.items():
            if int(job["state"]) < 9:
                count += 1
        if not local:
            remjobs = self.loadremotejobscount()
            count += remjobs
        self.result["result"] = count
        return True

class Summ():
    def __init__(self, fname = '', id = '') -> None:
        Settings(self)
        self.fname = fname
        self.jobid = id

    def getpiece(self, start_index):
        with open(self.fname, 'r') as infile:
            reader = csv.reader(infile)
            num = 0 
            for idx, row in enumerate(reader):
                if idx >= start_index-1:
                    if num >= int(self.linesreadcount):
                        return
                    else:
                        yield row 
                        num += 1

    def getfirstline(self):
        colslist = []
        firstline = ''
        with open(self.fname, encoding="UTF-8") as myfile:
            firstline = myfile.readline()
            cols = firstline.split(',')
            if len(cols)>9:
                colslist=[i for i in range(0,len(cols)+1,10)]
            colslist.remove(0)
        jobs = Jobs().loadjobs()
        jobs[self.jobid]["firstline"] = firstline
        jobs[self.jobid]["colslist"] = colslist
        jobs[self.jobid]["start_index"] = 1
        Jobs().savejobs(jobs)
        return (1, firstline, colslist)

    def getfirstlinepd(self):
        ind = ''
        with open(self.fname, encoding="UTF-8") as myfile:
            firstline = myfile.readline().replace('"', '')
            ind = pd.Series(firstline.split(','))
            ind=ind[ind.index%10==0]
            ind=ind[ind.index>0]
        jobs = Jobs().loadjobs()
        jobs[self.jobid]["firstline"] = ind.to_dict()
        jobs[self.jobid]["start_index"] = 2
        Jobs().savejobs(jobs)
        return (2, ind)

    def sumbyparts(self):
        try:
            jobs = Jobs().loadjobs()
            (start_index, firstline) = (0,'')
            if "start_index" not in jobs[self.jobid].keys():
                (start_index, firstline) = self.getfirstlinepd()
            else:
                firstline = jobs[self.jobid]["firstline"]
                start_index = jobs[self.jobid]["start_index"]
            ind = pd.Series(firstline)
            df = pd.DataFrame(columns=ind)
            isnext = False
            for line in self.getpiece(start_index):
                isnext = True
                s1 = pd.Series(line[0].split(','))
                s1=s1[s1.index%10==0]
                s1=s1[s1.index>0]
                s1 = s1.apply(lambda x: x.replace('"', ''))
                s1 = s1.replace('', np.nan)
                s1 = s1.fillna(0)
                s1 = s1.astype('float', errors='raise')
                df.loc[len(df)] = s1.values
            #summ = json.loads(df.sum(axis=0).to_json(orient='index'))
            summ = df.sum(axis=0).to_dict()
            jobs = Jobs().loadjobs()
            if start_index == 2:
                jobs[self.jobid]["result"] = summ
            else:
                psumm = pd.DataFrame(columns=ind)
                lastsumm = pd.Series(jobs[self.jobid]["result"])
                cursumm = pd.Series(summ)
                psumm.loc[len(psumm)] = lastsumm.values
                psumm.loc[len(psumm)] = cursumm.values
                summ = psumm.sum(axis=0).to_dict()
            jobs[self.jobid]["start_index"] = start_index + int(self.linesreadcount)
            jobs[self.jobid]["result"] = summ
            Jobs().savejobs(jobs)
            return {"summ":summ, "error":False, "iterable":isnext}
        except:
            return {"error":True, "iterable":False}
        


    def sumallfile(self):
        #формируем список номеров столбцов для считывания
        try:
            (_, colslist) = self.getfirstline()
            raw = pd.read_csv(self.fname, encoding="UTF-8", quoting=3, usecols=colslist)
            raw.rename(columns=lambda x: x.replace('"', ''), inplace=True)
            self.data = raw.applymap(lambda x: x.replace('"', ''))
            self.data = self.data.replace('', np.nan)
            self.data = self.data.fillna(0)
            self.data=self.data.astype('float', errors='raise')
            summ = json.loads(self.data.sum(axis=0).to_json(orient='index'))
            return {"summ":summ, "error":False, "iterable":False}
        except:
            return {"error":True, "iterable":False}

class Downloader(): 
    def __init__(self, fname = '') -> None:
        Settings(self)
        self.fname = fname

    def check_localfilename(self):
        if os.path.isfile(self.fname):
           return True
        return False  

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
            else:
                return False
        return True

    def loadfile2(self):
        if self.fname.startswith('http'):
            #функция сохранения файла в локальное хранилище и передачи 
            return False
        if self.check_localfilename():
            return True
        return False
        


if __name__ == '__main__':
    tr = Summ(fname='data.csv',id="b87775cb83cbf0511096cfb67074662a")
    tr.sumbyparts()
    '''
    ind = ''
    with open(tr.fname, encoding="UTF-8") as myfile:
        firstline = myfile.readline().replace('"', '')
        ind = pd.Series(firstline.split(','))
        ind=ind[ind.index%10==0]
        ind=ind[ind.index>0]
    df = pd.DataFrame(columns=ind)
    print(df.head)
    for line in tr.getpiece(2):
        print("=====================================================================================================")
        s1 = pd.Series(line[0].split(','))
        s1=s1[s1.index%10==0]
        s1 = s1.apply(lambda x: x.replace('"', ''))
        s1=s1[s1.index>0]
        s1 = s1.replace('', np.nan)
        s1 = s1.fillna(0)
        s1=s1.astype('float', errors='raise')
        df.loc[len(df)] = s1.values
    print(df.head)
    '''
    #tr.loadfile()
    #print(tr.data.)
    #print(tr.data(1))

