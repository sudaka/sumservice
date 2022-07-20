from flask import Flask, request, jsonify
from summer import DataConnector
import socket
import os

app = Flask(__name__)

hostname = socket.gethostname()
myip = socket.gethostbyname(hostname)

@app.get("/calculate")
def get_calculate():
    fname = request.args.get('filename')
    if fname != None:
        tst = DataConnector()
        tst.fname = fname
        tst.myip = myip
        if tst.createjob():
            return tst.result
    return '{"result":"Unrecognized error"}' 

@app.get("/getjobinfo")
def getjobinfo():
    jobid = request.args.get('id')
    if jobid != None:
        tst = DataConnector()
        tst.myip = myip
        tst.result["id"] = jobid
        if tst.getjobinfo():
            return {jobid:tst.result}
    return '{"result":"Unrecognized error"}' 

@app.get("/getjobcount")
def getjobcount():
    tst = DataConnector()
    tst.myip = myip
    if tst.getjobcount():
        return tst.result
    return '{"result":"Unrecognized error"}' 

@app.post("/countries")
def add_country():
    if request.is_json:
        country = request.get_json()
        return country, 201
    return {"error": "Request must be JSON"}, 415