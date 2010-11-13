import threading
import uuid
import time
import struct
import random
import socket
import sys

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
		self.maxRecordSize = 0
		self.signatureRecord = ""
		self.nodes = {} # nodeId : Node instace
		self.records = {} # recordGUID : record
		self.recordTypes = {} # recordTypeID : array of records
		self.attributes = ""
		self.contacts = {} # nodeId : PPGraphContact instance
		self.secProvider = 0
		self.myPeerID = "" # supplied by application
		self.myNodeID = 0 # 64 bit random number
	
class Record:
	def __init__(self):
		self.guid = ""
		self.typeID = ""
		self.creator = ""
		self.version = 1
		self.modificationTime = 0
		self.createdAt = 0
		self.expireTime
		self.data = ""
		self.securityData = ""
		self.attributes = ""
	
class Node:
	def __init__(self):
		self.peerId = ""
		self.nodeId = 0
		self.addresses = []
		self.attributes = ""
		self.records = {}
		self.recordTypes = {}

class ConnectionClosedException(Exception):
	def __init__(self):
		pass
	

class InvalidVersionException(Exception):
	def __init__(self):
		pass
	

class PPGraphContact(threading.Thread):
	
	
	def __init__(self, ppgraph, socket, incoming = False):
		self.ppgraph = ppgraph
		self.socket = socket
		self.lock = threading.Condition()
		self.graphGUID = ""
		
		self.frameBuffer = ""
		self.frameSize = -1	
		self.messageBuffer = ""
		self.messageSize = -1
		
		self.sendBuffer = [] # messages broken up into frame sized chunks, need lock before accessing
		self.sendFrameBuffer = "" # how much is remaining to send of a frame
		self.incoming = incoming
		self.recordsToPublish = [] # need lock before accessing
		self.nodeID = -1 # nodeid of this contact
		self.peerID = "" # peerid of this contact
		
		self.socket.settimeout(2)
		
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
		threading.Thread.__init__(self)
		
	def run(self):
		
		# Perform handshaking and
		# update the PPGraph's data structures correctly with the new
		# contact
		
		if not self.incoming:
			self.sendAuthInfo()
			try:
				self.send()
			except (socket.timeout):
				pass
			self.sendConnect()
			try:
				self.send()
			except (socket.timeout):
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
				performFlood = False
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
				
		except:
			if self.incoming:
				print("ReceivingSide Error: ", sys.exc_info()[1])
			else:				
				print("SendingSide Error: ", sys.exc_info()[1])
		
		# close the socket, remove myself from the graph
		self.socket.shutdown(socket.SHUT_RDWR)
		self.socket.close()
			
		if self.nodeID != -1 and self.graphGUID != "":
			self.ppgraph.lock.acquire()
			try:
				del self.ppgraph.graphs[self.graphGUID].contacts[self.nodeID]
			finally:
				self.ppgraph.lock.release()
			
		self.lock.acquire()
		try:
			self.lock.notifyAll()
		finally:
			self.lock.release()

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
				self.frameSize, = struct.unpack("!i", self.frameBuffer)

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
					self.messageSize, = struct.unpack("!i", self.messageBuffer[0 : 4])
					
					if self.messageSize < 0 or self.messageSize > MAX_MESSAGESIZE:
						raise ConnectionClosedException()
					
				if self.messageSize <= len(self.messageBuffer):
					self.parseMessage()
					self.messageSize = -1
					self.messageBuffer = ""


	def parseMessage(self):
		
		# first determine if the version matches, then get the message type
		messageVersion, messagetype = struct.unpack("!BB", self.messageBuffer[4:6])
		
		if messageVersion != 16:
			raise InvalidVersionException()
		
		# now call the appropriate receive function to handle the message
		
		func = self.recvMap.get(messagetype)
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
					
					packStr = "!i" + str(len(self.sendBuffer[0])) + "s"
					
					self.sendFrameBuffer = struct.pack(packStr, frameSize, self.sendBuffer[0])
					self.sendBuffer.pop(0)
					
			finally:
				self.lock.release()
	
	def sendAuthInfo(self):
		# called when trying to connect to a new node, basically to introduce myself
		
		formatStr = "!iBBxxBxHHH"
		
		graphIDstr = self.graphGUID + chr(0)
		sourceIDstr = self.ppgraph.graphs[self.graphGUID].myPeerID + chr(0)
		
		formatStr += str(len(graphIDstr)) + "s" + str(len(sourceIDstr)) + "s"
		
		messageLen = 16 + len(graphIDstr) + len(sourceIDstr)
		
		message = struct.pack(formatStr, messageLen, 0x10, 1, 0x02, 16, 16 + len(graphIDstr), messageLen, graphIDstr, sourceIDstr )
		
		self.lock.acquire()
		try:
			self.sendBuffer.append(message)
		finally:
			self.lock.release()
		
	
	def sendConnect(self):
		pass
		
	def sendWelcome(self):
		pass
	
	def sendRefuse(self):
		pass
	
	def sendDisconnect(self):
		pass
		
	def sendSolicitNew(self):
		pass
		
	def sendSolicitTime(self):
		pass
		
	def sendSolicitHash(self):
		pass
		
	def sendAdvertise(self):
		pass
		
	def sendRequest(self):
		pass
	
	def sendFlood(self):
		pass
		
	def sendSyncEnd(self):
		pass
		
	def sendPT2PT(self):
		pass
		
	def sendACK(self):
		pass
		
		
	# the other node is identifying itself, accept it
	def rcvAuthInfo(self):
		formatStr = "!iBBxxBxHHH"
		(messageLen, version, messageType, connType, graphIDOffset, sourceIDOffset, destIDOffset) = struct.unpack(formatStr, self.messageBuffer[0:16])
		if version != 0x10 or messageType != 0x01:
			self.sendDisconnect()
			return
		
		graphID = self.messageBuffer[graphIDOffset:sourceIDOffset - 1]
		sourceID = self.messageBuffer[sourceIDOffset:destIDOffset - 1]
		
		self.graphGUID = graphID
		if not self.ppgraph.graphs.has_key(graphID):
			self.sendDisconnect()
			return
		
		self.peerID = sourceID
		print("The other peer is :" + self.peerID)
		print("In the graph :" + self.graphGUID)
	
	def rcvConnect(self):
		pass
	
	# also needs to notify the waiting ppsec thread
	def rcvWelcome(self):
		
		# be sure the contact is added to the graph structure before we get here!
		self.lock.acquire()
		try:
			
			self.lock.notifyAll()
		finally:
			self.lock.release()
			
	# also needs to notify the waiting ppsec thread
	def rcvRefuse(self):
		self.lock.acquire()
		try:
			
			self.lock.notifyAll()
		finally:
			self.lock.release()
	
	def rcvDisconnect(self):
		pass
	    
	def rcvSolicitNew(self):
		pass
		
	def rcvSolicitTime(self):
		pass
		
	def rcvSolicitHash(self):
		pass
		
	def rcvAdvertise(self):
		pass
		
	def rcvRequest(self):
		pass
	
	def rcvFlood(self):
		pass
		
	def rcvSyncEnd(self):
		pass
		
	def rcvPT2PT(self):
		pass
		
	def rcvACK(self):
		pass

class PPGraph(threading.Thread):
	
	
	def __init__(self, listenPort):
		self.listenPort = listenPort
		self.lock = threading.Lock()
		self.serverThread = 0
		self.peerId = ""
		
		self.graphs = {}
		
		threading.Thread.__init__(self)
	
	def run(self):
		
		
		self.serverThread = Server(self, self.listenPort)
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
		
	
	# throws exception on failure
	def joinGraph(self, graphGUID, localPeerID, address, port, secProvider):
		# return until after successful connection and
		# authentication
		localNodeID = random.uniform(0, 9223372036854775807) #nodeid is a random 64bit int

		graph = Graph()
		graph.secProvider = secProvider
		graph.scope = 0x00000003 
		graph.myNodeID = localNodeID
		graph.myPeerID = localPeerID
		
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((address, port))
		
		self.lock.acquire()
		try:
			self.graphs[graphGUID] = graph
		finally:
			self.lock.release()
			
		newconn = PPGraphContact(self, s)
		newconn.graphGUID = graphGUID
		newconn.lock.acquire()
		try:
			newconn.start()
			newconn.lock.wait()
		finally:
			newconn.lock.release()
		
		
		self.lock.acquire()
		try:
			if len(self.graphs[graphGUID].contacts.keys()) == 0:
				del self.graphs[graphGUID]
				raise Exception()
		finally:
			self.lock.release()
			
		# TODO start a synchronization of the database
		
	
	def createGraph(self, graphGUID, localPeerID, secProvider, friendlyName):
		localNodeID = random.uniform(0, 9223372036854775807) #nodeid is a random 64bit int
		
		graph = Graph()
		graph.secProvider = secProvider
		graph.friendlyName = friendlyName
		graph.creatorID = localPeerID
		graph.scope = 0x00000003 
		graph.presenceLifetime = 600
		graph.maxRecordSize = 0
		graph.myNodeID = localNodeID
		graph.myPeerID = localPeerID
		
		# TODO create default records for the graph:
		
		# graph info
		# my contact record
		# my presence record
		# graph signature record
		
		self.lock.acquire()
		try:
			self.graphs[graphGUID] = graph
		finally:
			self.lock.release()
			
		
	
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

class Server(threading.Thread):
	
	def __init__(self, ppgraph, port):
		self.myppGraph = ppgraph
		self.port = port
		threading.Thread.__init__(self)

	def run(self):
		#create an INET, STREAMing socket
		serversocket = socket.socket(
			socket.AF_INET, socket.SOCK_STREAM)
		#bind the socket to a public host,
		# and a well-known port
		serversocket.bind((socket.gethostname(), self.port))
		#become a server socket
		serversocket.listen(5)
		print("Waiting for connection on " + str(self.port))
		while True:
			#accept connections from outside
			(clientsocket, address) = serversocket.accept()
			#now do something with the clientsocket
			#in this case, we'll pretend this is a threaded server
			self.myppGraph.addConnection(clientsocket)
