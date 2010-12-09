# takes in username and password of the local homegroup user

#if create:
#    Initialize ppgrh and ppsec
#    publish the homegroup credentials
#
#if join:
#
#    Initialize ppgrh and ppsec
#    Connect to the other peer
#    publish the homegroup credentials
#    
#now setup a named pipe
#wait on the pipe until a command comes in
#    BROWSE - get all the homegroup credentials from the ppgrh and extract the computer names
#    CONNECT (name) - get all the homegroup credentials from the ppgrh, find the matching one and return IP, username, password
#    QUIT - stop the server threads and exit
#repeat


import server
import ppsec
import sys
import socket
import uuid
import time
import os
import xml.dom.minidom as md

    
myPPSEC = ppsec.nullPPSEC()

myServer = server.PPGraph(3587)
myServer.daemon = True
myServer.start()

if len(sys.argv) < 4:
    raise Exception("Need parameters: create|join local_username local_password")

hostname = socket.gethostname()


def getOtherComputer():
    global hostname
    
    if hostname == "kbogert-laptop":
        return "fe80::68a6:77ff:fe55:53a1%wlan0"
    else:
        return "fe80::221:6aff:fe46:27a%eth0"

def getMyComputer():
    global hostname
    
    if hostname == "kbogert-laptop":
#        return "fe80::221:6aff:fe46:27a%wlan0"
        return "192.168.1.100"
    else:
#        return "fe80::68a6:77ff:fe55:53a1%eth0"
        return "192.168.1.4"

def getCredentialXML():
    global hostname
    global credType
    
    impl = md.getDOMImplementation()
    doc = impl.createDocument(None, "HOMEGROUP_RECORD", None)
    top_element = doc.documentElement
    
    newnode = doc.createElement("VERSION")
    text = doc.createTextNode("1")
    newnode.appendChild(text)
    top_element.appendChild(newnode)
    
    newnode = doc.createElement("RECORDSOURCE")
    text = doc.createTextNode("{" + str(credType) + "}")
    newnode.appendChild(text)
    top_element.appendChild(newnode)
    
    newnode = doc.createElement("RECORDID")
    text = doc.createTextNode("{00000000-0000-0000-0000-000000000000}")
    newnode.appendChild(text)
    top_element.appendChild(newnode)
    
    newnode = doc.createElement("EVENTTYPE")
    text = doc.createTextNode("0")
    newnode.appendChild(text)
    top_element.appendChild(newnode)
            
    newnode = doc.createElement("FLAGS")
    text = doc.createTextNode("0")
    newnode.appendChild(text)
    top_element.appendChild(newnode)
    
    newnode = doc.createElement("SOURCEOS")
    text = doc.createTextNode("100728832")
    newnode.appendChild(text)
    top_element.appendChild(newnode)
    
    newnode = doc.createElement("PERSIST")
    text = doc.createTextNode("1")
    newnode.appendChild(text)
    top_element.appendChild(newnode)
    
    newnode = doc.createElement("MACHINE")
    text = doc.createTextNode(hostname)
    newnode.appendChild(text)
    top_element.appendChild(newnode)
    
    newnode = doc.createElement("PEERID")
    text = doc.createTextNode(getMyComputer())
    newnode.appendChild(text)
    top_element.appendChild(newnode)

    hgnode = doc.createElement("HOMEGROUP_DATA")
    top_element.appendChild(hgnode)
        
    newnode = doc.createElement("USERNAME")
    text = doc.createTextNode(sys.argv[2])
    newnode.appendChild(text)
    hgnode.appendChild(newnode)

    newnode = doc.createElement("PASSWORD")
    text = doc.createTextNode(sys.argv[3])
    newnode.appendChild(text)
    hgnode.appendChild(newnode)

    newnode = doc.createElement("ACCOUNTCREATED")
    text = doc.createTextNode(str(int(time.time() * 1000)))
    newnode.appendChild(text)
    hgnode.appendChild(newnode)

    returnval = doc.toxml()
    doc.unlink()
    
    return returnval
    

if sys.argv[1] == "create":
    myServer.createGraph("HomeGroup", hostname, myPPSEC, "Home Group")
else:
    myServer.joinGraph("HomeGroup", hostname, getOtherComputer(), 3587, myPPSEC, "Home Group")
    
# create homegroup credentials record


credType = uuid.UUID('{929CB323-C5EA-48E7-A6D0-193DD432E769}')

myServer.publish("HomeGroup", credType, getCredentialXML())

pipefile = "/tmp/homegroup"

if not os.path.exists(pipefile):
    os.mkfifo(pipefile)

exitCondition = False

try:
    while not exitCondition:
        
        pipe = open(pipefile, "r")
        input = pipe.read()
        pipe.close()
        
        input = str.strip(input)
        
        if input == "QUIT":
            exitCondition = True
        elif input == "BROWSE":
            returnval = []
            
            allRecords = myServer.getAll("HomeGroup", credType)
            
            for record in allRecords:
                doc = md.parseString(record.data)
                returnval.append(doc.getElementsByTagName("MACHINE")[0].childNodes[0].data)
            
            pipe = open(pipefile, "w")
            for name in returnval:
                pipe.write(name)
                pipe.write("\n")
            pipe.close()
        else:
            computername = str.split(input)[1]
            
            returnval = []
            
            allRecords = myServer.getAll("HomeGroup", credType)
            
            for record in allRecords:
                doc = md.parseString(record.data)
                if unicode.upper(doc.getElementsByTagName("MACHINE")[0].childNodes[0].data) == str.upper(computername):
                    returnval.append(doc.getElementsByTagName("PEERID")[0].childNodes[0].data)
                    returnval.append(doc.getElementsByTagName("USERNAME")[0].childNodes[0].data)
                    returnval.append(doc.getElementsByTagName("PASSWORD")[0].childNodes[0].data)
                    break
            
            pipe = open(pipefile, "w")
            if len(returnval) == 0:
                pipe.write("None\n")
            else:
                for name in returnval:
                    pipe.write(name)
                    pipe.write("\n")
            pipe.close()        

finally:

    myServer.leaveGraph("HomeGroup")

    os.unlink(pipefile)