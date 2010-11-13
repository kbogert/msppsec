import threading
import uuid
import time
import struct

MAX_FRAMESIZE = 16384
MAX_MESSAGESIZE = 63914560

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
		self.myGUID = 0
	
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

class ConnectionClosedException(exception):
	def __init__(self):
		pass
	

class InvalidVersionException(exception):
	def __init__(self):
		pass
	

class PPGraphContact(Thread):
	
	
	def __init__(self, ppgraph, socket, incoming = False):
		self.ppgraph = ppgraph
		self.socket = socket
		self.lock = threading.Lock()
		
		self.frameBuffer = ""
		self.frameSize = -1	
		self.messageBuffer = ""
		self.messageSize = -1
		
		self.sendBuffer = [] # messages broken up into frame sized chunks, need lock before accessing
		self.sendFrameBuffer = "" # how much is remaining to send of a frame
		self.incoming = incoming
		self.recordsToPublish = [] # need lock before accessing
		self.peerId = ""
		
		self.socket.setTimeout(2)
		
		self.recvMap = {1 : self.rcvAuthInfo,
				2 : self.rcvConnect,
				3 : self.rcvWelcome,
				4 : self.rcvRefuse,
				5 : self.rcvDisconnect,
				6 : self.rcvSolicitNew,
				7 : self.rcvSolicitTime,
				8 : self.rcvSolicitHash,
				9 : self.rcvAdvertise,
				10 :self.rcvRequest,
				11 :self.rcvFlood,
				12 :self.rcvSyncEnd,
				13 :self.rcvPT2PT,
				14 :self.rcvACK}
		
	def run(self):
		
		# Perform handshaking and
		# update the PPGraph's data structures correctly with the new
		# contact
		
		if self.incoming:
			pass
		else:
			pass
		
		try:
			while(True):
				try:
					self.recv()
				except (socket.timeout):
					pass
				
				try:
					self.send()
				except (socket.timeout):
					pass
				
				# check for new data to publish
				performFlood = false
				self.lock.acquire()
				try:
					if len(self.recordsToPublish) > 0:
						performFlood = True
				finally:
					self.lock.release()
				if performFlood:
					self.sendFlood()
				
				# TODO check for data to update	
					
				# TODO if nothing for a while ping
				
					
				# if no response to ping, destroy connection
				
		except (Exception):
			# unknown exception, close the socket, remove myself from the graph
			pass

	def publish(self, recordStruct):
		self.lock.acquire()
		try:
			self.recordsToPublish.append(recordStruct)
		finally:
			self.lock.release()
			
	# receives a chunk of data from the peer and adds it to our buffer
	def recv(self):
		# NOTE in the event of a timeout, the main loop handles the exception
		global MAX_FRAMESIZE
		global MAX_MESSAGESIZE
		
		if self.frameSize == -1:
			x = self.socket.recv(4 - len(self.frameBuffer))
			
			if len(x) == 0:
				raise ConnectionClosedException()
			self.frameBuffer += x
			
			if len(self.frameBuffer) == 4:
				self.frameSize, = struct.unpack("i", self.frameBuffer)

				if self.frameSize < 0 or self.frameSize > MAX_FRAMESIZE:
					raise ConnectionClosedException()
		else:
			x = self.socket.recv(self.frameSize - len(self.frameBuffer))
			
			if len(x) == 0:
				raise ConnectionClosedException()
			self.frameBuffer += x
			
			if len(self.frameBuffer) == self.frameSize:
				self.messageBuffer += self.frameBuffer[4 : ]
				self.frameSize = -1
				self.frameBuffer = ""
				
				if self.messageSize == -1:
					self.messageSize, = struct.unpack("i", self.message[0 : 4])
					
					if self.messageSize < 0 or self.messageSize > MAX_MESSAGESIZE:
						raise ConnectionClosedException()
					
				if self.messageSize <= len(self.messageBuffer):
					self.parseMessage()
					self.messageSize = -1
					self.messageBuffer = ""

	def parseMessage(self):
		
		# first determine if the version matches, then get the message type
		messageVersion, messagetype = struct.unpack("BB", self.messageBuffer[4:8])
		
		if messageVersion != 16:
			raise InvalidVersionException()
		
		# now call the appropriate receive function to handle the message
		
		func = self.recvMap(messagetype)
		func()
	
	
	# sends a chunk of data to the peer and subtracts it from our buffer
	def send(self):
		if len(self.sendFrameBuffer) > 0:
			x = self.socket.send(self.sendFrameBuffer)
			
			if x == 0:
				raise ConnectionClosedException()
			
			self.sendFrameBuffer = self.sendFrameBuffer[x : ]
			
		if len(self.sendFrameBuffer) == 0:
			self.lock.acquire()
			try:
				if len(self.sendBuffer) > 0:
					# add on the framesize to the front of the frame
					frameSize = len(self.sendBuffer[0])
					frameSize += 4
					
					packStr = "i" + str(len(self.sendBuffer[0])) + "s"
					
					self.sendFrameBuffer = struct.pack(packStr, frameSize, self.sendBuffer[0])
					self.sendBuffer.pop(0)
					
			finally:
				self.lock.release()
	
	def sendAuthInfo():
		pass
	
	def sendConnect():
		pass
		
	def sendWelcome():
		pass
	
	def sendRefuse():
		pass
	
	def sendDisconnect():
		pass
		
	def sendSolictNew():
		pass
		
	def sendSolictTime():
		pass
		
	def sendSolictHash():
		pass
		
	def sendAdvertise():
		pass
		
	def sendRequest():
		pass
	
	def sendFlood():
		pass
		
	def sendSyncEnd():
		pass
		
	def sendPT2PT():
		pass
		
	def sendACK():
		pass
		
		
	def rcvAuthInfo():
		pass
	
	def rcvConnect():
		pass
	
	# also needs to notify the waiting ppsec thread
	def rcvWelcome(self):
		
		# be sure the contact is added to the graph structure before we get here!
		self.lock.acquire()
		try:
			
			self.lock.notify()
		finally:
			self.lock.release()
			
	# also needs to notify the waiting ppsec thread
	def rcvRefuse(self):
		self.lock.acquire()
		try:
			
			self.lock.notify()
		finally:
			self.lock.release()
	
	def rcvDisconnect():
		pass
		
	def rcvSolictNew():
		pass
		
	def rcvSolictTime():
		pass
		
	def rcvSolictHash():
		pass
		
	def rcvAdvertise():
		pass
		
	def rcvRequest():
		pass
	
	def rcvFlood():
		pass
		
	def rcvSyncEnd():
		pass
		
	def rcvPT2PT():
		pass
		
	def rcvACK():
		pass

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
				# resyncronize with connected peers periodically
				
			finally:
				self.lock.release()
				

		
	# adds a new socket, we don't know which graph it's for yet
	def addConnection(self, socket):
		
		newconn = PPGraphContact(self, socket, True)
		newconn.start()
		
	
	# join an existing graph, returns the GUID of the local node on success
	def joinGraph(self, graphGUID, peerGUID, address, port, secProvider):
		# return until after successful connection and
		# authentication
		
		graph = Graph()
		graph.secProvider = secProvider
		graph.scope = 0x00000003 
		nodeID = self.genGUID()
		graph.myGUID = nodeID
		
		socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		socket.connect((address, port))
		
		self.lock.acquire()
		try:
			self.graphs[graphGUID] = graph
		finally:
			self.lock.release()
			
		newconn = PPGraphContact(self, socket)
		newconn.lock.acquire()
		try:
			newconn.start()
			newconn.lock.wait()
		finally:
			newconn.lock.release()
		
		
		self.lock.acquire()
		try:
			if self.graphs[graphGUID].contacts.has_key(peerGUID):
				return nodeID
			else:
				raise Exception()
		finally:
			self.lock.release()		
		
	
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
		graph.myGUID = nodeID
		
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
