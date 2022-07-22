import json
from requests import request
from comclass import Jobs, Summ
import time
import requests
from dnslib import *

def test_first_summ():
    tst = Summ()
    tst.loadfile('data.csv')
    assert tst.result["summ"]['col9'] == 81.1310216823

def test_goodlocalname():
    res = requests.get('http://127.0.0.1:5000/calculate?filename=data.csv')
    try:
        resjson = res.json()
        assert resjson["result"] == True
    except:
        assert False
    
def test_badlocalname():
    res = requests.get('http://127.0.0.1:5000/calculate?filename=data1.csv')
    try:
        resjson = res.json()
        assert resjson["result"] == False
    except:
        assert False

def est_remotename():
    res = requests.get('http://127.0.0.1:5000/calculate?filename=http://google.com/first?one=oisjdfasdrkjdsa')
    print(res)

def test_dns_round_robin():
    tst = Jobs()
    fdns = "summer.example.com"
    dnsserver = '127.0.0.1'
    firstreceive = tst.getaddr(fdns,dnsserver)
    secondreceive = tst.getaddr(fdns,dnsserver)
    for i in range(len(firstreceive)):
        j = i-1
        if i == 0:
            j = len(secondreceive)-1
        assert firstreceive[i] == secondreceive[j]

def est_mid_summ():
    tst = Summ()
    tst.loadfile('data.csv_mid.csv')
    assert tst.summ['col9'] == 81131.02168233921

def est_big_summ():
    tst = Summ()
    tst.loadfile('data.csv_big.csv')
    assert tst.summ['col9'] == 4705599.257575668

def est_time():
    start = time.time()
    tst = Summ()
    tst.loadfile('data.csv_big.csv')
    end = time.time()
    timedelta = int(end-start)
    print('Время на расчет: '+str(timedelta))
    print(tst.summ['col9'])

def createbigfile(fname):
    #repcount = 58000
    repcount = 1000
    strlist = []
    with open(fname, 'r') as f:
        strlist = f.readlines()
        firstline = strlist[0]
        others = strlist[1:]
        with open(fname+'_big.csv', 'a') as bf:
            bf.write(firstline)
            for _ in range(repcount):
                for tstr in others:
                    bf.write(tstr)


if __name__ == '__main__':
    #createbigfile('data.csv')
    #test_time()
    #est_big_summ()
    test_dns_round_robin()