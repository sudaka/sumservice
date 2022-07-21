import time, threading
from comclass import Jobs, Downloader

class Dthread():
    def __init__(self, jobid = '', fname = '') -> None:
        self.jobid = jobid
        self.fname = fname
        self.repcount = 0

    def workerth(self):
        dwn = Downloader(self.fname)
        jobs = Jobs().loadjobs()
        if self.jobid in jobs.keys(): 
            jobs[self.jobid]["state"] = "1"
            print(f'File with id {self.jobid} loading.')
        Jobs().savejobs(jobs)
        while self.repcount<3:
            if dwn.loadfile2():
                if self.jobid in jobs.keys():
                    jobs[self.jobid]["state"] = "2"
                    jobs[self.jobid]["result"] = "File loaded."
                    Jobs().savejobs(jobs)
                    print(f'File with id {self.jobid} loaded.')
                break
            self.repcount += 1
            time.sleep(3)
        jobs = Jobs().loadjobs()
        if self.jobid in jobs.keys():
            if jobs[self.jobid]["state"] == "1":
                jobs[self.jobid]["state"] = "9"
                jobs[self.jobid]["result"] = "Bad in file copying."
                Jobs().savejobs(jobs)

    def start(self):
        self.hw_thread = threading.Thread(target = self.workerth)
        self.hw_thread.daemon = True
        self.hw_thread.start()

print('Downloading server started.')
iscont = True
while iscont:
    try:
        jobs = Jobs().loadjobs()
        for jobid, job in jobs.items():
            if job["state"] == "0":
                tmpthread = Dthread(jobid = jobid, fname = job["fname"])
                tmpthread.start()           
        time.sleep(2)
    except KeyboardInterrupt:
        quest = input("Break dowloading files (y/n)?")
        if quest == 'y':
            iscont = False
        else:
            print('Downloading continue.')
print('Downloading stopped.')
