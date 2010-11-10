import threading

class PPGraph(Thread):
	
	
	def __init__(self, secProvider, listenPort):
		self.listenPort = listenPort
		self.lock = threading.Lock()
		self.serverThread = 0
		self.newConnections = []
		self.dataToPublish = []
		
		self.groups = {}
	
	def run(self):
		
		
		self.serverThread = Server(self)
		self.serverThread.start()
		
		
		# begin loop
		
		# check for new data to publish
		
		# check for new connections to initialize
		
		# check for incoming messages from peers
		
		# check for maintenance timeouts
		
	# adds a new socket, we don't know which graph it's for yet
	def addConnection(self, socket):
		self.lock.acquire()
		try:
			self.newConnections.append(socket);
			
		finally:
			self.lock.release()
		
	
	# join an existing graph, returns the GUID on success
	def joinGraph(self, address, port, secProvider):
		pass
	
	# create a new graph, returns the graph GUID
	def createGraph(self, secProvider):
		pass
	
	# publish data of type recordTypeId to all members of the graph, returns
	# the record's GUID
	def publish(self, graphId, recordTypeId, data):
		guid = self.genGUID()
		self.lock.acquire()
		try:
			self.dataToPublish.append(graphId, guid, recordTypeId, data)
			return guid
		finally:
			self.lock.release()
		return Nil
	
	# retrieve the given record from the given graph
	def get(self, graphId, recordId):
		pass

	# retrieve all records of type recordTypeId from the given graph
	def getAll(self, graphId, recordTypeId):
		pass
	
	# returns all record types for the given peer in the given graph
	def getRecordTypesFor(self, graphId, peerId):
		pass
	
	# get all records from a peer for a given graph
	def getAllFor(self, graphId, peerId):
		pass
	
	# list members of the graph that we know about with their peerIds
	def listMembers(self, graphId):
		pass
		

class Server(Thread):
	myppGraph
	port
	
	def __init__(self, ppgraph, port):
		myppGraph = ppgraph
		self.port = port
	
	def run(self):
		#create an INET, STREAMing socket
		serversocket = socket.socket(
			socket.AF_INET, socket.SOCK_STREAM)
		#bind the socket to a public host,
		# and a well-known port
		serversocket.bind((socket.gethostname(), port))
		#become a server socket
		serversocket.listen(5)

		while True:
			#accept connections from outside
			(clientsocket, address) = serversocket.accept()
			#now do something with the clientsocket
			#in this case, we'll pretend this is a threaded server
			myppGraph.addConnection(clientsocket)
