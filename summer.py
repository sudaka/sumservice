import time, threading
from comclass import Settings, Jobs, Summ

class Sumthread():
    def __init__(self) -> None:
        Settings(self)

    def getsummalg(self, fname = ''):
        if self.summ_algoritm == 'sumallfile':
            self.summing = Summ(fname).sumallfile
            return True
        self.summing = Summ(fname).sumallfile
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
                if job["state"] == "2":
                    jobs1 = Jobs().loadjobs()
                    jobs1[jobid]["state"] = "3"
                    Jobs().savejobs(jobs1)
                    print(f'File with id {jobid} summing.')
                    self.getsummalg(job["fname"])
                    sumres = self.summing()
                    if not sumres["error"]:
                        if not sumres["iterable"]:
                            self.savesummresults(jobid, sumres["summ"])
                        else:
                            pass
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