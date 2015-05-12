from lib import *
from random import randint
# class REQWorkerThread(AgentThread):
		
# 	def __init__(self, context, name, alive_event):
# 		super(REQWorkerThread, self).__init__(context, name, alive_event)
# 		self.setup()

# 	def do_work(self):
# 		sleep(1)
# 		return sum(range(randint(0, 1000)))

# 	def setup(self):
# 		self.frontend = self.context.socket(zmq.REQ)
# 		self.frontend.connect(AddressManager.get_connect_address('db_backend'))
# 		self.poller = zmq.Poller()
		
# 	def run(self):
# 		self.poller.register(self.frontend, zmq.POLLIN)
# 		self.frontend.send_multipart(WorkerMsg(STATUS_READY).message)	
# 		while self.alive_event.isSet():
# 			sockets = dict(self.poller.poll(100))
# 			if self.frontend in sockets:
# 				request = self.frontend.recv_multipart()
# 				self.say('On frontend: {}'.format(request))
# 				client_id, empty, workload = request[0:3]
# 				if workload == 'quit': 
# 					break
# 				result = self.do_work()	
# 				self.frontend.send_multipart(WorkerMsg(STATUS_READY, client_id, str(result)).message)

# 		self.frontend.close()


class REQWorker(AgentProcess):
	

	def do_work(self, msg):
		# sleep(1)
		return str(hash(msg))
		
	def run(self):
		frontend = self.context.socket(zmq.REQ)
		frontend.connect(AddressManager.get_connect_address(self.frontend_name))
		poller = zmq.Poller()
		poller.register(frontend, zmq.POLLIN)
		Package(msg = MsgCode.STATUS_READY).send(frontend)
		while True:
			sockets = dict(poller.poll(100))
			if frontend in sockets:
				package = Package.recv(frontend)
				self.say('On frontend: {}'.format(package))
				result = self.do_work(package.msg)
				client_p = package.encapsulated
				client_p.msg = result
				broker_p = Package(msg = MsgCode.JOB_COMPLETE, encapsulated = client_p)
				self.say('Sending on frontend: {}'.format(broker_p))
				broker_p.send(frontend)
				
		frontend.close()

from threading import Timer



class MJDWorker(AgentProcess):

	__metaclass__ = abc.ABCMeta

	BROKER_TIMEOUT = 1;
	BROKER_ALIVENESS = 3 # Number of timeouts before the worker tries to reconnect to the broker

	def ping(self, socket):
		ping = Package(msg = MsgCode.PING)
		self.send(socket, ping)

	def pong(self, socket):
		pong = Package(msg = MsgCode.PONG)
		self.send(socket, pong)

	def reconnect(self):
		self.say('Connecting to broker...')
		if hasattr(self, 'frontend'):
			self.poller.unregister(self.frontend)

		self.frontend = self.context.socket(zmq.DEALER)
		self.frontend.connect(AddressManager.get_connect_address(self.frontend_name))
		self.poller.register(self.frontend, zmq.POLLIN)
		package = Package(msg = MsgCode.STATUS_READY)
		self.send(self.frontend, package)


	def update_aliveness(self):
		self.broker_aliveness -= 1
		# self.say('aliveness: {}'.format(self.broker_aliveness))
		if self.broker_aliveness == 0:
			self.broker_aliveness = self.BROKER_ALIVENESS
			self.reconnect()
			self.loop()
		else:
			self.ping(self.frontend)

	def reset_timer(self):
		if hasattr(self, 'broker_timer'):
			self.broker_timer.cancel()
		self.broker_timer = Timer(self.BROKER_TIMEOUT, self.update_aliveness)
		self.broker_timer.start()

	def send(self, socket, package):
		assert isinstance(socket, zmq.Socket)
		assert isinstance(package, Package)
		# Set timer for when to update broker aliveness if 
		self.reset_timer()
		package.send(socket)

	def recv(self, socket):
		package = Package.recv(socket)
		### As soon as the worker recieves a message from the broker it resets the broker_aliveness
		self.reset_timer()
		self.broker_aliveness = self.BROKER_ALIVENESS
		return package

	@abc.abstractmethod
	def do_work(self, workload):
		return


	def loop(self):
		while True:
			self.iteration()


	def iteration(self):
		sockets = dict(self.poller.poll())
		if self.frontend in sockets:
			package = self.recv(self.frontend)
			if package.msg == MsgCode.PING:
				### Means that broker is getting impatient, so reply with a PONG
				self.pong()
			self.say('On frontend: {}'.format(package))
			if package.encapsulated:
				result = self.do_work(package.msg)
									
				client_p = package.encapsulated
				client_p.msg = result
				broker_p = Package(msg = MsgCode.JOB_COMPLETE, encapsulated = client_p)
				self.say('Sending on frontend: {}'.format(broker_p))
				self.send(self.frontend, broker_p)
			
	

	@abc.abstractmethod
	def setup(self):
		return

	def run(self):
		self.poller = zmq.Poller()
		self.broker_aliveness = self.BROKER_ALIVENESS
		self.setup()
		self.reconnect()
		self.loop()
		self.frontend.close()





class DBWorker(MJDWorker):

	def setup(self):
		### Nothing to setup
		pass

	def do_work(self, workload):
		self.say('Doing work: {}'.format(workload))
		return MsgCode.ORDER_STORED



class Auth(MJDWorker):

	def setup(self):
		self.backend = self.context.socket(zmq.DEALER)
		self.backend.connect(AddressManager.get_connect_address(self.backend_name))		
		self.poller.register(self.backend, zmq.POLLIN)

	def authenticate_order(self, order):
		return True

	def do_work(self, order):
		if self.authenticate_order(order):
			Package(msg = order).send(self.backend)
			return MsgCode.ORDER_RECEIVED
		else:
			return MsgCode.INVALID_ORDER
