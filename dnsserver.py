import datetime
import sys
import time
import threading
import traceback
import socketserver
from dnslib import *
import codecs

class DomainName(str):
    def __getattr__(self, item):
        return DomainName(item + '.' + self)

D = DomainName('example.com')
IP = '127.0.0.1'
TTL = 5
PORT = 53
service = [D.summer]

soa_record = SOA(
    mname=D.ns1,  # primary name server
    rname=D.summer,  # email of the domain administrator
    times=(
        202207200,  # serial number
        60 * 60 * 1,  # refresh
        60 * 60 * 3,  # retry
        60 * 60 * 24,  # expire
        60 * 60 * 1,  # minimum
    )
)

ns_records = [NS(D.ns1), NS(D.ns2)]
records = {
    D: [A(IP), MX(D.mail), soa_record] + ns_records,
    D.ns1: [A(IP)],  # MX and NS records must never point to a CNAME alias (RFC 2181 section 10.3)
    D.ns2: [A(IP)],
    D.summer: [A('192.168.51.174')],
}

qtypemapper = {
    'A' : 1,
    'AAAA': 28,
    'MX': 15,
    'SOA': 6,
    'NS': 2,
}

def balance_round_robin(curservice, currecords):
    dd = currecords[curservice][0]
    currecords[curservice].remove(dd)
    currecords[curservice].append(dd)

def balance_by_jobcount():
    pass

def dns_response(data):
    balance_round_robin(service[0], records)
    request = DNSRecord.parse(data)
    #print(request)
    reply = DNSRecord(DNSHeader(id=request.header.id, qr=1, aa=1, ra=1), q=request.q)
    qname = request.q.qname
    qn = str(qname)
    qtype = request.q.qtype
    qt = QTYPE[qtype]
    spqn = qn.split('.')
    gqn = '.'.join(spqn)
    DN = str(D) + '.'
    if qn == DN or qn.endswith('.' + DN):
        for name, rrs in records.items():
            if name == qn[:-1]:
                for rdata in rrs:
                    rqtstr = rdata.__class__.__name__
                    rqt = qtypemapper[rqtstr]
                    if qt in ['*', rqtstr]:
                        reply.add_answer(RR(rname=qname, rtype=rqt, rclass=1, ttl=TTL, rdata=rdata))
        for rdata in ns_records:
            reply.add_answer(RR(rname=D, rtype=QTYPE.NS, rclass=1, ttl=TTL, rdata=rdata))
        reply.add_answer(RR(rname=D, rtype=QTYPE.SOA, rclass=1, ttl=TTL, rdata=soa_record))
    #print("---- Reply:\n", reply)
    return reply.pack()

class BaseRequestHandler(socketserver.BaseRequestHandler):
    def get_data(self):
        raise NotImplementedError

    def send_data(self, data):
        raise NotImplementedError

    def to_bytes(self, s):
         if type(s) is bytes:
            return s
         elif type(s) is str or (sys.version_info[0] < 3 and type(s) is unicode):
             return codecs.encode(s, 'utf-8')
         else:
             raise TypeError("Expected bytes or string, but got %s." % type(s))

    def handle(self):
        now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        #print ("\n\n%s request %s (%s %s):" % (self.__class__.__name__[:3], now, self.client_address[0],
        #                                       self.client_address[1]))
        try:
            data = self.get_data()
            #print(len(data), self.to_bytes(data))  # repr(data).replace('\\x', '')[1:-1]
            self.send_data(dns_response(data))

        except Exception:
            #traceback.print_exc(file=sys.stderr)
            pass

class TCPRequestHandler(BaseRequestHandler):
    def get_data(self):
        data = self.request.recv(8192).strip()
        sz = int(data[:2].encode('hex'), 16)
        if sz < len(data) - 2:
            raise Exception("Wrong size of TCP packet")
        elif sz > len(data) - 2:
            raise Exception("Too big TCP packet")
        return data[2:]

    def send_data(self, data):
        sz = hex(len(data))[2:].zfill(4).decode('hex')
        return self.request.sendall(sz + data)

class UDPRequestHandler(BaseRequestHandler):
    def get_data(self):
        return self.request[0].strip()

    def send_data(self, data):
        return self.request[1].sendto(data, self.client_address)

if __name__ == '__main__':
    print("Starting nameserver...")
    servers = [
        socketserver.ThreadingUDPServer(('', PORT), UDPRequestHandler),
        socketserver.ThreadingTCPServer(('', PORT), TCPRequestHandler),
    ]

    for s in servers:
        thread = threading.Thread(target=s.serve_forever)  # that thread will start one more thread for each request
        thread.daemon = True  # exit the server thread when the main thread terminates
        thread.start()
        print("%s server loop running in thread: %s" % (s.RequestHandlerClass.__name__[:3], thread.name))
    try:
        while 1:
            time.sleep(1)
            sys.stderr.flush()
            sys.stdout.flush()
    except KeyboardInterrupt:
        pass
    finally:
        for s in servers:
            s.shutdown()