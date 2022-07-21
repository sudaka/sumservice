import time, threading
from comclass import Settings, Jobs, Summ

class Sumthread():
    def __init__(self) -> None:
        Settings(self)

    def getsummalg(self, fname = '', id = ''):
        if self.summ_algoritm == 'sumallfile':
            self.summing = Summ(fname, id).sumallfile
            return True
        self.summing = Summ(fname, id).sumbyparts
        return True

    def savesummresults(self, jobid, summ):
        jobs1 = Jobs().loadjobs()
        jobs1[jobid]["state"] = "10"
        jobs1[jobid]["result"] = summ
        Jobs().savejobs(jobs1)
        print(f'File with id {jobid} calculated.')

    def workerth(self):
        while True:
            jobs = Jobs().loadjobs()
            for jobid, job in jobs.items():
                self.repcount = 0
                if job["state"] in ["2","5"]:
                    jobs1 = Jobs().loadjobs()
                    jobs1[jobid]["state"] = "4"
                    Jobs().savejobs(jobs1)
                    print(f'File with id {jobid} summing.')
                    self.getsummalg(job["fname"], jobid)
                    sumres = self.summing()
                    print(sumres)
                    if not sumres["error"]:
                        if sumres["iterable"]:
                            jobs1 = Jobs().loadjobs()
                            jobs1[jobid]["state"] = "5"
                            Jobs().savejobs(jobs1)
                            print(f'File with id {jobid} await.')
                        else:
                            self.savesummresults(jobid, sumres["summ"])
                            
                    time.sleep(1)
                    

    def start(self):
        self.hw_thread = threading.Thread(target = self.workerth)
        self.hw_thread.daemon = True
        self.hw_thread.start()

summ = Sumthread()
summ.start()
print("Summ service started.")

iscont = True
while iscont:
    try:
        time.sleep(2)
    except KeyboardInterrupt:
        quest = input("Break sum files (y/n)?")
        if quest == 'y':
            iscont = False
        else:
            print('Summing continue.')
print('Summing stopped.')