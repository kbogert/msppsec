import threading
import uuid
import time
import struct
import random
import socket
import sys
import traceback
import hashlib

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
		self.nodeRecords = {} # peerId : array of records
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
		self.lastModifiedBy = ""
		self.createdAt = 0
		self.expireTime = 0
		self.data = ""
		self.securityData = ""
		self.attributes = ""
	
class Node:
	def __init__(self):
		self.peerID = ""
		self.nodeID = 0
		self.addresses = []
		self.attributes = ""
		self.records = {}
		self.recordTypes = {}
		self.friendlyname = ""

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
		self.isSyncing = False
		
		self.sendBuffer = [] # messages broken up into frame sized chunks, need lock before accessing
		self.sendFrameBuffer = "" # how much is remaining to send of a frame
		self.incoming = incoming
		self.recordsToPublish = [] # need lock before accessing
		self.nodeID = -1 # nodeid of this contact
		self.peerID = "" # peerid of this contact
		self.connectionState = "UnAuth"
		
		self.socket.settimeout(.2)
		
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
			print("about to send auth info")
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
				
				recordsToFlood = []
				# check for new data to publish
				self.lock.acquire()
				try:
					for record in self.recordsToPublish:
						recordsToFlood.append(record)
						
					self.recordsToPublish = []
				finally:
					self.lock.release()
					
				for record in recordsToFlood:
					self.sendFlood(record)
				
				# TODO check for data to update	
					
				# TODO if nothing for a while ping
				
					
				# if no response to ping, destroy connection
		except ConnectionClosedException:
			pass
		except:
			if self.incoming:
				print("ReceivingSide Error: " + traceback.format_exc())
				
			else:				
				print("SendingSide Error: " + traceback.format_exc())
				
		
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
			x = self.socket.recv(2 - len(self.frameBuffer))
			
			if len(x) == 0:
				raise ConnectionClosedException()
			self.frameBuffer += x
			
			if len(self.frameBuffer) == 2:
				self.frameSize, = struct.unpack("!H", self.frameBuffer)
				self.frameBuffer = ""
				
				if self.frameSize < 0 or self.frameSize > MAX_FRAMESIZE:
					raise ConnectionClosedException()
		else:
			x = self.socket.recv(self.frameSize - len(self.frameBuffer))
			
			if len(x) == 0:
				raise ConnectionClosedException()
			self.frameBuffer += x
			
			if len(self.frameBuffer) == self.frameSize:
				self.messageBuffer += self.frameBuffer
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
		
		if self.connectionState != "UnAuth":
			if self.ppgraph.graphs[self.graphGUID].secProvider != None:
				self.messageBuffer = self.ppgraph.graphs[self.graphGUID].secProvider.decrypt(self.messageBuffer)
		
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
					
					packStr = "!H" + str(len(self.sendBuffer[0])) + "s"
					
					self.sendFrameBuffer = struct.pack(packStr, frameSize, self.sendBuffer[0])
					self.sendBuffer.pop(0)
					
			finally:
				self.lock.release()
	
	def addToSendBuffer(self, message):
		global MAX_FRAMESIZE
		
		if self.connectionState != "UnAuth":
			if self.ppgraph.graphs[self.graphGUID].secProvider != None:
				message = self.ppgraph.graphs[self.graphGUID].secProvider.encrypt(message)
		
		self.lock.acquire()
		try:
			while len(message) > 0:
				self.sendBuffer.append(message[0:MAX_FRAMESIZE- 4])
				message = message[MAX_FRAMESIZE- 4:]
		finally:
			self.lock.release()
		
	
	def sendAuthInfo(self):
		# called when trying to connect to a new node, basically to introduce myself
		
		formatStr = "!iBBxxBxHHH"
		
		graphIDstr = self.graphGUID
		sourceIDstr = self.ppgraph.graphs[self.graphGUID].myPeerID
		
		formatStr += str(len(graphIDstr)) + "s" + str(len(sourceIDstr)) + "s"
		
		messageLen = 16 + len(graphIDstr) + len(sourceIDstr)
		
		message = struct.pack(formatStr, messageLen, 0x10, 1, 0x01, 16, 16 + len(graphIDstr), messageLen, graphIDstr, sourceIDstr )
		
		self.addToSendBuffer(message)
		
		if self.ppgraph.graphs[self.graphGUID].secProvider != None:
			if not self.ppgraph.graphs[self.graphGUID].secProvider.authenticateOutgoing(self.socket):
				raise Exception("Authentication Failed")
				
		self.connectionState = "Auth"
		
	
	def sendConnect(self):
		formatStr = "!iBBxxxBHHxxQ"
		
		friendlyNamestr = self.ppgraph.graphs[self.graphGUID].friendlyName
		
		formatStr += str(len(friendlyNamestr)) + "s"
		
		messageLen = 24 + len(friendlyNamestr)
		
		message = struct.pack(formatStr, messageLen, 0x10, 2, 0, messageLen, 24, self.ppgraph.graphs[self.graphGUID].myNodeID, friendlyNamestr)
		
		self.addToSendBuffer(message)
		
		
		
	def sendWelcome(self):
		
		formatStr = "!iBBxxQQBxHHH"
		
		peerIDstr = self.ppgraph.graphs[self.graphGUID].myPeerID
		
		formatStr += str(len(peerIDstr)) + "s"
		
		messageLen = 32 + len(peerIDstr)
		
		message = struct.pack(formatStr, messageLen, 0x10, 3, self.ppgraph.graphs[self.graphGUID].myNodeID, int(time.time() * 1000), 0, messageLen, 32, messageLen, peerIDstr )
		
		self.addToSendBuffer(message)
				
		self.ppgraph.lock.acquire()
		try:
			self.ppgraph.graphs[self.graphGUID].contacts[self.nodeID] = self
			
		finally:
			self.ppgraph.lock.release()
		
		print("Welcome Sent!")
		
	# whyCodes:
	# 1 = busy
	# 2 = Already Connected on this connection
	# 3 = Duplicate connection to an existing one
	# 4 = Direct connection not allowed
	def sendRefuse(self, whyCode):
		
		formatStr = "!iBBxxBBH"
		
		messageLen = 12
		
		message = struct.pack(formatStr, messageLen, 0x10, 4, whyCode, 0, messageLen )
		
		self.addToSendBuffer(message)
		
		
	# whyCodes:
	# 1 = I'm leaving the graph
	# 2 = I'm performing maintenance, you're the least useful node
	# 3 = Application requested a disconnect
	def sendDisconnect(self, whyCode):
		formatStr = "!iBBxxBBH"
		
		messageLen = 12
		
		message = struct.pack(formatStr, messageLen, 0x10, 5, whyCode, 0, messageLen )
		
		self.addToSendBuffer(message)
		
	
	# synchronize all records for now
	def sendSolicitNew(self):
		formatStr = "!iBBxxBBH"
		
		messageLen = 12
		
		message = struct.pack(formatStr, messageLen, 0x10, 6, 0, 0, messageLen )
		
		self.isSyncing = True
		self.addToSendBuffer(message)
	
	# not implemented
	def sendSolicitTime(self):
		pass
		
	# not implemented
	def sendSolicitHash(self):
		pass
	
	# not implemented
	def sendAdvertise(self):
		pass
	
	# not implemented
	def sendRequest(self):
		pass
	
	def sendFlood(self, record):
		formatStr = "!iBBxxHxx"
		
		recordStr = ""
		
		recordFormat = "!16s16sIxxxxI" + str(len(record.creator)) + "sI" + str(len(record.lastModifiedBy)) + "s"
		recordFormat += "I" + str(len(record.securityData)) + "sQQQI" + str(len(self.graphGUID)) + "sHI" + str(len(record.data)) + "s"
		
		recordStr = struct.pack(recordFormat, record.typeID.bytes, record.guid.bytes, record.version, len(record.creator), record.creator,
					len(record.lastModifiedBy), record.lastModifiedBy, len(record.securityData), record.securityData,
					record.createdAt, record.expireTime, record.modificationTime, len(self.graphGUID), self.graphGUID,
					0x0100, len(record.data), record.data)

		if len(record.attributes) > 0:
			recordFormat = str(len(record.attributes)) + "s"
			recordStr += struct.pack(recordFormat, len(record.attributes), record.attributes)
		else:
			recordStr += chr(0) + chr(0) + chr(0) + chr(0)
		
		
#		formatStr += len(recordStr) + "s"	
		messageLen = 12 + len(recordStr)
		
		message = struct.pack(formatStr, messageLen, 0x10, 0x0B, 12 )
		message += recordStr
		
		self.addToSendBuffer(message)
		
		
	def sendSyncEnd(self):
		formatStr = "!iBBxxBxxx"
		
		messageLen = 12
		
		message = struct.pack(formatStr, messageLen, 0x10, 0x0C, 1)
		
		self.addToSendBuffer(message)
	
	# sends a direct message to this peer, needs a uuid and string payload
	def sendPT2PT(self, dataType, payload):
		
		formatStr = "!iBBxxHxx16s"
		
		formatStr += str(len(payload)) + "s"
		
		messageLen = 28 + len(payload)
		
		message = struct.pack(formatStr, messageLen, 0x10, 0x0D, 28, dataType.bytes, payload )
		
		self.addToSendBuffer(message)
		
	# send an acknowledgement for each record in the array
	def sendACK(self, rcvdRecords):
		formatStr = "!iBBxxHH" + str(24 * len(rcvdRecords)) + "s"
		
		recordStr = ""
		
		for record in rcvdRecords:
			recordStr += record.guid.bytes + chr(0) + chr(0) + chr(0) + chr(1)
		
		
		messageLen = 12 + len(recordStr)
		
		message = struct.pack(formatStr, messageLen, 0x10, 0x0E, len(rcvdRecords), 12, recordStr )
		
		self.addToSendBuffer(message)
		
		
	# the other node is identifying itself, accept it
	def rcvAuthInfo(self):
		formatStr = "!iBBxxBxHHH"
		(messageLen, version, messageType, connType, graphIDOffset, sourceIDOffset, destIDOffset) = struct.unpack(formatStr, self.messageBuffer[0:16])
		if version != 0x10 or messageType != 0x01:
			self.sendDisconnect()
			return
		
		graphID = self.messageBuffer[graphIDOffset:sourceIDOffset]
		sourceID = self.messageBuffer[sourceIDOffset:destIDOffset]
		
		self.graphGUID = graphID
		if not self.ppgraph.graphs.has_key(graphID):
			self.sendDisconnect()
			return
		
		self.peerID = sourceID
		print("The other peer is :" + self.peerID)
		print("In the graph :" + self.graphGUID)
		
				
		if self.ppgraph.graphs[self.graphGUID].secProvider != None:
			if not self.ppgraph.graphs[self.graphGUID].secProvider.authenticateIncoming(sourceID, graphID, self.socket):
				raise Exception("Authentication Failed")
		
		self.connectionState = "Auth"
			
	# dummy implementation, only for testing
	def rcvConnect(self):
		formatStr = "!iBBxxBBHHxx"
		(messageLen, version, messageType, flags, addressCount, addressOffset, friendlyNameOffset) = struct.unpack(formatStr, self.messageBuffer[0:16])
		if version != 0x10 or messageType != 0x02:
			self.sendDisconnect()
			return
		
		(nodeID,) = struct.unpack("Q", self.messageBuffer[16:24])
		friendlyName = self.messageBuffer[friendlyNameOffset:]
		friendlyName = friendlyName[:len(friendlyName)]
		
		self.nodeID = nodeID
		
		print("The other node is :" + str(self.nodeID))
		print("Whose Name is :" + friendlyName)
		
		self.sendWelcome()
	
	
	# also needs to notify the waiting ppsec thread
	def rcvWelcome(self):
		formatStr = "!iBBxxQQBxHHH"
		(messageLen, version, messageType, nodeID, peerTime, addressCount, addressOffset, peerIDoffset, friendlyNameOffset) = struct.unpack(formatStr, self.messageBuffer[0:32])
		if version != 0x10 or messageType != 0x03:
			self.sendDisconnect()
			return
		
		peerID = self.messageBuffer[peerIDoffset:friendlyNameOffset]
		friendlyName = self.messageBuffer[friendlyNameOffset:]
		friendlyName = friendlyName[:len(friendlyName)]
		
		self.nodeID = nodeID
		self.peerID = peerID
		
		self.ppgraph.lock.acquire()
		try:
			self.ppgraph.graphs[self.graphGUID].contacts[self.nodeID] = self
#			self.ppgraph.graphs[self.graphGUID].peerTime = peerTime
			
		finally:
			self.ppgraph.lock.release()
		
		print("The other node is :" + str(self.nodeID))		
		print("Whose PeerID is :" + self.peerID)
		print("Whose Name is :" + friendlyName)		
		
		
		# extract nodeid, peer time, peer id, and friendly name (if available)
		
		# be sure the contact is added to the graph structure before we get here!
		self.lock.acquire()
		try:
			
			self.lock.notifyAll()
		finally:
			self.lock.release()
			
		self.sendSolicitNew()
			
	# also needs to notify the waiting ppsec thread
	def rcvRefuse(self):
		self.lock.acquire()
		try:
			
			self.lock.notifyAll()
		finally:
			self.lock.release()
	
		raise Exception("Received a refuse message")
		
	def rcvDisconnect(self):
		raise ConnectionClosedException()
	
	# loop through all the records we have, flooding the peer
	# dummy implementation, ignores the inclusion and exclusion settings
	def rcvSolicitNew(self):
		self.ppgraph.lock.acquire()
		try:
			for (recordID, record) in self.ppgraph.graphs[self.graphGUID].records.items():
				self.sendFlood(record)
		finally:
			self.ppgraph.lock.release()
			
		self.sendSyncEnd()
	
	# not implemented, causes disconnect
	def rcvSolicitTime(self):
		self.sendDisconnect(3)
	
	# not implemented, causes disconnect
	def rcvSolicitHash(self):
		self.sendDisconnect(3)
	
	# not implemented, causes disconnect	
	def rcvAdvertise(self):
		self.sendDisconnect(3)
	
	# not implemented, causes disconnect
	def rcvRequest(self):
		self.sendDisconnect(3)
	
	# stupid implementation, sends an ack for each flood received
	def rcvFlood(self):
		formatStr = "!iBBxxHxx"
		(messageLen, version, messageType, dataOffset) = struct.unpack(formatStr, self.messageBuffer[0:12])
		if version != 0x10 or messageType != 0x0B:
			self.sendDisconnect()
			return
		
		
		recordStr = self.messageBuffer[dataOffset:]
		
		recordFormat = "!16s16sIxxxxI"
		record = Record()
		
		(recordType, recordId, record.version, creatorLength) = struct.unpack(recordFormat, recordStr[0:44])
		record.typeID = uuid.UUID(bytes=recordType)
		record.guid = uuid.UUID(bytes=recordId)
		record.creator = recordStr[44:44 + creatorLength ]
		
		
		cursor = 44 + creatorLength
		(lastModifiedIDLength,) = struct.unpack("!I", recordStr[cursor: cursor + 4])
		cursor += 4
		if lastModifiedIDLength > 0:
			record.lastModifiedBy = recordStr[cursor:cursor + lastModifiedIDLength]
		
		cursor += lastModifiedIDLength
		(securityDataLength,) = struct.unpack("!I", recordStr[cursor: cursor + 4])
		cursor += 4
		if securityDataLength > 0:
			record.securityData = recordStr[cursor:cursor + securityDataLength]
		
		cursor += securityDataLength
		(record.createdAt, record.expireTime, record.modificationTime) = struct.unpack("!QQQ", recordStr[cursor:cursor+24])
		
		cursor += 24
		(graphIDLength,) = struct.unpack("!I", recordStr[cursor: cursor + 4])
		cursor += 4
		graphID = recordStr[cursor:cursor + graphIDLength]
		
		if graphID != self.graphGUID:
			self.sendDisconnect()
			return
		
		cursor += graphIDLength
		(protocolVersion, payloadSize) = struct.unpack("!HI", recordStr[cursor:cursor+6])
		cursor += 6
		record.data = recordStr[cursor:cursor+payloadSize]
		
		cursor += payloadSize
		(attributesLength,) = struct.unpack("!I", recordStr[cursor:cursor+4])
		cursor += 4
		if (attributesLength > 0):
			record.attributes = recordStr[cursor:cursor + attributesLength]
		
		
		# check for the 4 special type of records
		if record.typeID.hex == "00000100000000000000000000000000":
			# graph info record
			self.parseGraphInfo(record.data)
		if record.typeID.hex == "00000200000000000000000000000000":
			# graph signature record
			self.parseSignature(record.data)
		if record.typeID.hex == "00000300000000000000000000000000":
			# contact record
			self.parseContact(record.data)
		if record.typeID.hex == "00000400000000000000000000000000":
			# presence record
			self.parsePresence(record.data)

		
		# store in our database
		self.ppgraph.lock.acquire()
		try:
			self.ppgraph.graphs[self.graphGUID].records[record.guid] = record
			
			if not self.ppgraph.graphs[self.graphGUID].recordTypes.has_key(record.typeID):	
				self.ppgraph.graphs[self.graphGUID].recordTypes[record.typeID] = []
			self.ppgraph.graphs[self.graphGUID].recordTypes[record.typeID].append(record)
			
			if not self.ppgraph.graphs[self.graphGUID].nodeRecords.has_key(record.creator):
				self.ppgraph.graphs[self.graphGUID].nodeRecords[record.creator] = []
			self.ppgraph.graphs[self.graphGUID].nodeRecords[record.creator].append(record)
			
		finally:
			self.ppgraph.lock.release()
		
		
		if not self.isSyncing:
			self.sendACK([record])
		
	def parseGraphInfo(self, record):
		
		graphData = self.ppgraph.graphs[self.graphGUID]
		
		formatStr = "!ixxxxII"
		(messageLen, scope, graphIDLength) = struct.unpack(formatStr, record[0:16])		
		
		graphData.scope = scope
		
		cursor = 16
		graphID = record[cursor:cursor + graphIDLength]
		cursor += graphIDLength
		
		(creatorIDLength, ) = struct.unpack("!I", record[cursor: cursor + 4])
		cursor += 4
		graphData.creatorID = record[cursor:cursor + creatorIDLength]
		cursor += creatorIDLength
		
		(friendlyNameLength, ) = struct.unpack("!I", record[cursor: cursor + 4])
		cursor += 4
		graphData.friendlyName = record[cursor:cursor + friendlyNameLength]
		cursor += friendlyNameLength
		
		(commentLength, ) = struct.unpack("!I", record[cursor: cursor + 4])
		cursor += 4
		if commentLength > 0:
			graphData.comment = record[cursor:cursor + commentLength]
		cursor += commentLength
		
		(presenceLifetime, maxPresences, maxRecordSize) = struct.unpack("!III", record[cursor: cursor+12])
		
		graphData.presenceLifetime = presenceLifetime
		graphData.maxPresenceRecords = maxPresences
		graphData.maxRecordSize = maxRecordSize
	
	def parseSignature(self, record):
		(graphSignature, ) = struct.unpack("!Q", record)
		
		self.ppgraph.graphs[self.graphGUID].signatureRecord = graphSignature
	
	# ignored for now
	def parseContact(self, record):
		pass
		
	
	# ignored for now
	def parsePresence(self, record):
		pass
		
	def rcvSyncEnd(self):
		self.isSyncing = False
		
	def rcvPT2PT(self):
		formatStr = "!iBBxxHxx16s"
		(messageLen, version, messageType, dataOffset, dataType) = struct.unpack(formatStr, self.messageBuffer[0:28])
		if version != 0x10 or messageType != 0x0D:
			self.sendDisconnect()
			return
		
		messageGUID = uuid.UUID(bytes=dataType)
		
		if messageGUID.hex == "0ccbb0d2be414bd6914b058ec5dcce64":
			# message is a ping
			# ignore for now
			return
		
		# otherwise it's meant for the application, call the callback
		if not self.ppgraph.graphs[self.graphGUID].secProvider == None:
			secProvider.directMessageCallback(self.messageBuffer[28:])
	
	# good to know
	def rcvACK(self):
		pass

class PPGraph(threading.Thread):
	
	
	def __init__(self, listenPort):
		self.listenPort = listenPort
		self.lock = threading.Lock()
		self.serverThread = 0

		self.graphs = {}
		
		threading.Thread.__init__(self)
	
	def run(self):
		
		
		self.serverThread = Server(self, self.listenPort)
		self.serverThread.daemon = True
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
		newconn.daemon = False
		newconn.start()
		
	
	# throws exception on failure
	def joinGraph(self, graphGUID, localPeerID, address, port, secProvider, friendlyName):
		# return until after successful connection and
		# authentication
		localNodeID = random.uniform(0, 9223372036854775807) #nodeid is a random 64bit int

		graph = Graph()
		graph.secProvider = secProvider
		graph.scope = 0x00000003 
		graph.myNodeID = localNodeID
		graph.myPeerID = localPeerID
		graph.friendlyName = friendlyName
		
		# TODO create node entry for myself
		addrinfo = socket.getaddrinfo(address, port, socket.AF_INET6, socket.SOCK_STREAM)
		(family, socktype, proto, canonname, sockaddr) = addrinfo[0]
		
		s = socket.socket(family, socktype, proto)
		s.connect(sockaddr)
		
		self.lock.acquire()
		try:
			self.graphs[graphGUID] = graph
		finally:
			self.lock.release()
			
		newconn = PPGraphContact(self, s)
		newconn.daemon = False
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
			
		# TODO publish presence records for myself
		
	
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
		
		# TODO create node entry for myself
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
			
	def leaveGraph(self, graphId):
		for (peerID, contact) in self.graphs[graphId].contacts.items():
			contact.sendDisconnect(1)
			
		
		
	
	# publish data of type recordTypeId to all members of the graph, returns
	# the record's GUID
	def publish(self, graphId, recordTypeId, data):
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return None
			
			graphstruct = self.graphs.get(graphId)

			guid = self.genGUID()
			
			lowpart = ""
			lowpart += chr(ord(guid.bytes[0]) ^ ord(guid.bytes[8]))
			lowpart += chr(ord(guid.bytes[1]) ^ ord(guid.bytes[9]))
			lowpart += chr(ord(guid.bytes[2]) ^ ord(guid.bytes[10]))
			lowpart += chr(ord(guid.bytes[3]) ^ ord(guid.bytes[11]))
			lowpart += chr(ord(guid.bytes[4]) ^ ord(guid.bytes[12]))
			lowpart += chr(ord(guid.bytes[5]) ^ ord(guid.bytes[13]))
			lowpart += chr(ord(guid.bytes[6]) ^ ord(guid.bytes[14]))
			lowpart += chr(ord(guid.bytes[7]) ^ ord(guid.bytes[15]))
			
			hash = hashlib.md5(graphstruct.myPeerID).digest()
			highpart = ""
			highpart += chr(ord(hash[0]) ^ ord(hash[8]))
			highpart += chr(ord(hash[1]) ^ ord(hash[9]))
			highpart += chr(ord(hash[2]) ^ ord(hash[10]))
			highpart += chr(ord(hash[3]) ^ ord(hash[11]))
			highpart += chr(ord(hash[4]) ^ ord(hash[12]))
			highpart += chr(ord(hash[5]) ^ ord(hash[13]))
			highpart += chr(ord(hash[6]) ^ ord(hash[14]))
			highpart += chr(ord(hash[7]) ^ ord(hash[15]))
			
			guid = uuid.UUID(bytes=(highpart + lowpart))
			newRecord = Record()
			newRecord.attributes = ""
			newRecord.createdAt = int(time.time() * 1000)
			newRecord.creator = graphstruct.myPeerID
			newRecord.lastModifiedBy = graphstruct.myPeerID
			newRecord.data = data
			newRecord.guid = guid
			newRecord.modificationTime = int(time.time() * 1000)
			newRecord.expireTime = int(time.time() * 1000) + 1000 * 86400
			newRecord.typeID = recordTypeId
		
			graphstruct.records[guid] = newRecord
			if not graphstruct.recordTypes.has_key(recordTypeId):
				graphstruct.recordTypes[recordTypeId] = []
			graphstruct.recordTypes[recordTypeId].append(newRecord)
			
			if not graphstruct.nodeRecords.has_key(graphstruct.myPeerID):
				graphstruct.nodeRecords[graphstruct.myPeerID] = []
				
			graphstruct.nodeRecords[graphstruct.myPeerID].append(newRecord)
			
			for (NodeID, contactStruct) in graphstruct.contacts.items():
				contactStruct.publish(newRecord)
				
			return guid
		finally:
			self.lock.release()
		return None
	
	# retrieve the given record from the given graph
	def get(self, graphId, recordId):
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return None
			
			if not self.graphs.get(graphId).records.has_key(recordId):
				return None
			
			return self.graphs.get(graphId).records.get(recordId)
		finally:
			self.lock.release()

	# retrieve all records of type recordTypeId from the given graph
	def getAll(self, graphId, recordTypeId):
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return None
			
			if not self.graphs.get(graphId).recordTypes.has_key(recordTypeId):
				return []
			
			return self.graphs.get(graphId).recordTypes.get(recordTypeId)
		finally:
			self.lock.release()
	
	# returns all record types for the given peer in the given graph
	def getRecordTypesFor(self, graphId, peerId):
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return None
			
			if not self.graphs.get(graphId).nodeRecords.has_key(peerId):
				return None
			
			return set([i.typeID for i in self.graphs.get(graphId).nodeRecords.get(peerId)])
		finally:
			self.lock.release()
	
	# get all records from a peer for a given graph
	def getAllFor(self, graphId, peerId):
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return None
			
			if not self.graphs.get(graphId).nodeRecords.has_key(peerId):
				return None
			
			return self.graphs.get(graphId).nodeRecords.get(peerId)
		finally:
			self.lock.release()
	
	# list members of the graph that we know about with their peerIds
	def listMembers(self, graphId):
		self.lock.acquire()
		try:
			if not self.graphs.has_key(graphId):
				return None
			
			return self.graphs.get(graphId).nodeRecords.keys()
		finally:
			self.lock.release()
		
	# return a 
	def genGUID(self):
		return uuid.uuid4()

class Server(threading.Thread):
	
	def __init__(self, ppgraph, port):
		self.myppGraph = ppgraph
		self.port = port
		threading.Thread.__init__(self)

	def run(self):
		#create an INET, STREAMing socket
		serversocket = socket.socket(
			socket.AF_INET6, socket.SOCK_STREAM)
		#bind the socket to a public host,
		# and a well-known port
		serversocket.bind(("::", self.port))
		#become a server socket
		serversocket.listen(5)
		while True:
			#accept connections from outside
			(clientsocket, address) = serversocket.accept()
			#now do something with the clientsocket
			#in this case, we'll pretend this is a threaded server
			self.myppGraph.addConnection(clientsocket)
