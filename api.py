from flask import Flask, request
from comclass import Jobs

app = Flask(__name__)

@app.get("/calculate")
def get_calculate():
    fname = request.args.get('filename')
    if fname != None:
        tst = Jobs()
        tst.fname = fname
        if tst.createjob():
            return tst.result   
    return '{"result":"Unrecognized error"}' 

@app.get("/getjobinfo")
def getjobinfo():
    jobid = request.args.get('id')
    local = request.args.get('local')
    localreq = False
    if local == 'yes':
        localreq = True
    if jobid != None:
        tst = Jobs()
        tst.result["id"] = jobid
        if tst.getjobinfo(local=localreq):
            return {jobid:tst.result}
    return {jobid: {"result": "Job not in list"}}

@app.get("/getjobcount")
def getjobcount():
    local = request.args.get('local')
    localreq = False
    if local == 'yes':
        localreq = True
    tst = Jobs()
    if tst.getjobcount(local=localreq):
        return tst.result
    return '{"result":"Unrecognized error"}' 