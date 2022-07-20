from flask import Flask, request, jsonify
from summer import DataConnector

app = Flask(__name__)

@app.get("/calculate")
def get_calculate():
    fname = request.args.get('filename')
    if fname != None:
        tst = DataConnector()
        tst.fname = fname
        if tst.createjob():
            return tst.result   
    return '{"result":"Unrecognized error"}' 

@app.get("/getjobinfo")
def getjobinfo():
    jobid = request.args.get('id')
    if jobid != None:
        tst = DataConnector()
        tst.result["id"] = jobid
        if tst.getjobinfo():
            return {jobid:tst.result}
    return '{"result":"Unrecognized error"}' 

@app.get("/getjobcount")
def getjobcount():
    local = request.args.get('local')
    localreq = False
    if local == 'yes':
        localreq = True
    tst = DataConnector()
    if tst.getjobcount(local=localreq):
        return tst.result
    return '{"result":"Unrecognized error"}' 

@app.post("/countries")
def add_country():
    if request.is_json:
        country = request.get_json()
        return country, 201
    return {"error": "Request must be JSON"}, 415