import time
import server
import random

# create two PPGraph's with two different ports in the first, create the graph
# then use the second to connect to it


port1 = int(random.uniform(1025, 65535))
port2 = int(random.uniform(1025, 65535))

creator = server.PPGraph( port1 )
creator.start()

creator.createGraph("The Graph's ID", "I'm the Creator", None, "Test Graph")

time.sleep(4)

joiner = server.PPGraph( port2 )
joiner.start()

joiner.joinGraph("The Graph's ID", "I'm the Joiner", "localhost", port1, None)