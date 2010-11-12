import threading
import uuid
import time

class Graph:

	def __init__(self):
		self.scope = 0
		self.creatorID = ""
		self.friendlyName = ""
		self.comment = ""
		self.presenceLifetime = 0
		self.maxPresenceRecords = 0
		self.maxRecordSize = 0;
		self.signatureRecord = ""
		self.nodes = {}
		self.records = {}
		self.recordTypes = {}
		self.attributes = ""
		self.contacts = {}
		self.secProvider = 0
	
class Record:
	def __init__(self):
		self.guid = ""
		self.typeID = ""
		self.creator = ""
		self.timestamp = 0
		self.createdAt = 0
		self.data = ""
		self.attributes = ""
	
class Node:
	def __init__(self):
		self.guid = ""
		self.addresses = []
		self.attributes = ""
		self.records = {}
		self.recordTypes = {}


class PPGraphContact(Thread):
	
	
	def __init__(self, ppgraph, socket, newConnection = False):
		self.ppgraph = ppgraph
		self.socket = socket
		self.lock = threading.Lock()
		self.rcvBuffer = ""
		self.rcvPacketSize = 0
		self.sendBuffer = ""
		self.isNewConnection = newConnection
		self.recordsToPublish = []
		self.peerId = ""
		
	def run(self):
		
		# if it's a new connection then perform handshaking and
		# update the PPGraph's data structures correctly with the new
		# contact
		
		if self.isNewConnection:
			pass
		
		
		while(True):
			pass
				# check for new data to publish
				
				# TODO check for data to update
				
				# check for incoming messages from peers
				
				# if nothing for a while ping
				
				# if no response to ping, destroy connection
				

	def publish(self, recordStruct):
		self.lock.acquire()
		try:
			self.recordsToPublish.append(recordStruct)
		finally:
			self.lock.release()
			
			
	def sendAuthInfo():
	
	def sendConnect():
		
	def sendWelcome():
	
	def sendRefuse():
	
	def sendDisconnect():
		
	def sendSolictNew():
		
	def sendSolictTime():
		
	def sendSolictHash():
		
	def sendAdvertise():
		
	def sendRequest():
	
	def sendFlood():
		
	def sendSyncEnd():
		
	def sendPT2PT():
		
	def sendACK():
		
		
	def rcvAuthInfo():
	
	def rcvConnect():
		
	def rcvWelcome():
	
	def rcvRefuse():
	
	def rcvDisconnect():
		
	def rcvSolictNew():
		
	def rcvSolictTime():
		
	def rcvSolictHash():
		
	def rcvAdvertise():
		
	def rcvRequest():
	
	def rcvFlood():
		
	def rcvSyncEnd():
		
	def rcvPT2PT():
		
	def rcvACK():

# This is not the correct way to handle blocking sockets! this architecture should
# either be non-blocking, or we kickoff a manager thread when a new socket is
# established with a peer for sending/receiving data
class PPGraph(Thread):
	
	
	def __init__(self, secProvider, listenPort):
		self.listenPort = listenPort
		self.lock = threading.Lock()
		self.serverThread = 0
		self.peerId = ""
		
		self.graphs = {}
	
	def run(self):
		
		
		self.serverThread = Server(self)
		self.serverThread.start()
		
		
		# begin loop
		while True:
			time.sleep(600)			
			self.lock.acquire()
			try:
				pass
				# perform maintenenance (publish presence and contact records)
				# remove all data for nodes with expired presence records
				# remove all expired records
				# check if our number of nodes should be increased/decreased
				#	If so, connect to new nodes/disconnect
				# check for partition of network
				
			finally:
				self.lock.release()
				

		
	# adds a new socket, we don't know which graph it's for yet
	def addConnection(self, socket):
		
		newconn = PPGraphContact(self, socket, True)
		newconn.start()
		
	
	# join an existing graph, returns the GUID of the local node on success
	def joinGraph(self, graphGUID, address, port, secProvider):
		# can't fire off a thread until after successful connection and
		# authentication
		
		pass
	
	# create a new graph, returns the graph GUID, and local node Id
	def createGraph(self, secProvider, name):
		graphGUID = self.genGUID()
		nodeID = self.genGUID()
		
		graph = Graph()
		graph.secProvider = secProvider
		graph.friendlyName = name
		graph.creatorID = nodeID
		graph.scope = 0x00000003 
		graph.presenceLifetime = 600
		graph.signatureRecord = nodeID
		graph.maxRecordSize = 4096
		
		self.lock.acquire()
		try:
			self.graphs[graphGUID] = graph
		finally:
			self.lock.release()
			
			
		return (graphGUID, nodeID)
		
	
	# publish data of type recordTypeId to all members of the graph, returns
	# the record's GUID
	def publish(self, graphId, recordTypeId, data):
		guid = self.genGUID()
		
		newRecord = Record()
		newRecord.attributes = ""
		newRecord.createdAt = time.gmtime()
		newRecord.creator = self.peerId
		newRecord.data = data
		newRecord.guid = guid
		newRecord.timestamp = 0
		newRecord.typeID = recordTypeId
		
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return nil
			
			graphstruct = self.graphs.get(graphId)
			graphstruct.records[guid] = newRecord
			graphstruct.recordTypes[recordTypeId].append(newRecord)
			
			if self.peerId in graphstruct.contacts:
				graphstruct.contacts[self.peerId].records[guid] = newRecord				
				graphstruct.contacts[self.peerId].recordTypes[recordTypeId].append(newRecord)
			
			for (peerId, contactStruct) in graphstruct.contacts:
				contactStruct.publish(newRecord)
				
			return guid
		finally:
			self.lock.release()
		return nil
	
	# retrieve the given record from the given graph
	def get(self, graphId, recordId):
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return nil
			
			if not self.graphs.get(graphId).records.has_key(recordId):
				return nil
			
			return self.graphs.get(graphId).records.get(recordId)
		finally:
			self.lock.release()

	# retrieve all records of type recordTypeId from the given graph
	def getAll(self, graphId, recordTypeId):
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return nil
			
			if not self.graphs.get(graphId).recordTypes.has_key(recordId):
				return []
			
			return self.graphs.get(graphId).recordTypes.get(recordTypeId)
		finally:
			self.lock.release()
	
	# returns all record types for the given peer in the given graph
	def getRecordTypesFor(self, graphId, peerId):
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return nil
			
			if not self.graphs.get(graphId).nodes.has_key(peerId):
				return nil
			
			return self.graphs.get(graphId).nodes.get(peerId).recordTypes.keys()
		finally:
			self.lock.release()
	
	# get all records from a peer for a given graph
	def getAllFor(self, graphId, peerId):
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return nil
			
			if not self.graphs.get(graphId).nodes.has_key(peerId):
				return nil
			
			return self.graphs.get(graphId).nodes.get(peerId).records
		finally:
			self.lock.release()
	
	# list members of the graph that we know about with their peerIds
	def listMembers(self, graphId):
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return nil
			
			return self.graphs.get(graphId).nodes.keys()
		finally:
			self.lock.release()
		
	# return a 
	def genGUID(self):
		return UUID().bytes

class Server(Thread):
	
	def __init__(self, ppgraph, port):
		self.myppGraph = ppgraph
		self.port = port
	
	def run(self):
		#create an INET, STREAMing socket
		serversocket = socket.socket(
			socket.AF_INET, socket.SOCK_STREAM)
		#bind the socket to a public host,
		# and a well-known port
		serversocket.bind((socket.gethostname(), self.port))
		#become a server socket
		serversocket.listen(5)

		while True:
			#accept connections from outside
			(clientsocket, address) = serversocket.accept()
			#now do something with the clientsocket
			#in this case, we'll pretend this is a threaded server
			self.myppGraph.addConnection(clientsocket)
