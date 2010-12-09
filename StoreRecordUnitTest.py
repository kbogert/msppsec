import time
import server
import random
import uuid
import ppsec
import pprint

# create two PPGraph's with two different ports in the first, create the graph
# then use the second to connect to it

sec = ppsec.nullPPSEC()

port1 = int(random.uniform(1025, 65535))
port2 = int(random.uniform(1025, 65535))
graphGUID = "The Graph's ID"
creatorPeerID = "I'm the Creator"
joinerPeerID = "I'm the Joiner"
record1TypeGUID = uuid.uuid4()

creator = server.PPGraph( port1 )
creator.daemon = True
creator.start()

creator.createGraph(graphGUID, creatorPeerID, sec, "Test Graph")

time.sleep(4)

joiner = server.PPGraph( port2 )
joiner.daemon = True
joiner.start()

joiner.joinGraph(graphGUID, joinerPeerID, "localhost", port1, sec, "Ken's Laptop")

record1GUID = joiner.publish(graphGUID, record1TypeGUID, "This is record 1")

pp = pprint.PrettyPrinter()

print("Retrieve by ID")
ans = None
while ans == None:
    ans = joiner.get(graphGUID, record1GUID)
    time.sleep(1)

print("Joining Node Record:")
pp.pprint(ans.__dict__)


ans = None
while ans == None:
    ans = creator.get(graphGUID, record1GUID)
    time.sleep(1)
    
print("Creating Node Record:")
pp.pprint(ans.__dict__)


print("Retrieve all for a node.")
ans = None
while ans == None:
    ans = joiner.getAllFor(graphGUID, joinerPeerID)
    time.sleep(1)

print("Joining Node Record:")
for record in ans:
    pp.pprint(record.__dict__)


ans = None
while ans == None:
    ans = creator.getAllFor(graphGUID, joinerPeerID)
    time.sleep(1)
    
print("Creating Node Record:")
for record in ans:
    pp.pprint(record.__dict__)

joiner.leaveGraph(graphGUID)
