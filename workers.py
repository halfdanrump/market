from lib import *
from random import randint
class REQWorkerThread(AgentThread):
		
	def __init__(self, context, name, alive_event):
		super(REQWorkerThread, self).__init__(context, name, alive_event)
		self.setup()

	def do_work(self):
		sleep(1)
		return sum(range(randint(0, 1000)))

	def setup(self):
		self.frontend = self.context.socket(zmq.REQ)
		self.frontend.connect(AddressManager.get_connect_address('db_backend'))
		self.poller = zmq.Poller()
		
	def run(self):
		self.poller.register(self.frontend, zmq.POLLIN)
		self.frontend.send_multipart(WorkerMsg(STATUS_READY).message)	
		while self.alive_event.isSet():
			sockets = dict(self.poller.poll(100))
			if self.frontend in sockets:
				request = self.frontend.recv_multipart()
				self.say('On frontend: {}'.format(request))
				client_id, empty, workload = request[0:3]
				if workload == 'quit': 
					break
				result = self.do_work()	
				self.frontend.send_multipart(WorkerMsg(STATUS_READY, client_id, str(result)).message)

		self.frontend.close()


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
				broker_p.send(frontend)
				
		frontend.close()

from threading import Timer



class MJDWorker(AgentProcess):

	__metaclass__ = abc.ABCMeta

	BROKER_TIMEOUT = 1;
	BROKER_ALIVENESS = 3 # Number of timeouts before the worker tries to reconnect to the broker

	# sockets = {}
	
	# def __init__(self, *sockets):
	# 	for socket in sockets: 
	# 		assert isinstance(socket, zmqSocket)
	# 		if sockets.has_key(socket.name):
	# 			raise Exception('Socket name must be unique')
	# 		else:
	# 			sockets.update({socket.name:socket})

	def ping(self, socket):
		ping = Package(msg = MsgCode.PING)
		self.send(socket, ping)

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
		self.say('aliveness: {}'.format(self.broker_aliveness))
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
		# assert isinstance(socket, zmq.Socket)
		# assert isinstance(package, Package)
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
		# print('DID WORK!DID WORK!DID WORK!DID WORK!DID WORK!DID WORK!DID WORK!DID WORK!DID WORK!DID WORK!DID WORK!DID WORK!DID WORK!')
		# return 'some work'

	# def reply_frontend(self):

	def loop(self):
		while True:
			# self.say('Sockets in poller: {}'.format(self.poller.sockets))
			# self.say('Frontend socket: {}'.format(self.frontend))
			sockets = dict(self.poller.poll(100))
			if self.frontend in sockets:
				package = self.recv(self.frontend)
				self.say('On frontend: {}'.format(package))
				if package.encapsulated:
					result = self.do_work(package.msg)
					
					client_p = package.encapsulated
					client_p.msg = result
					broker_p = Package(msg = result, encapsulated = client_p)
					self.send(self.frontend, broker_p)
				
					client_p = package.encapsulated
					client_p.msg = MsgCode.ORDER_RECEIVED
					broker_package = Package(msg = MsgCode.JOB_COMPLETE, encapsulated=client_p)
					self.say('Sending on frontend: {}'.format(broker_package))
					broker_package.send(self.frontend)


				# else:
				# 	### This means that the worker simply received a pong
				# 	if package.msg == MsgCode.PONG:
				# 		self.broker_aliveness = self.BROKER_ALIVENESS


				# broker_p.send(frontend)
				
		self.frontend.close()

	@abc.abstractmethod
	def setup(self):
		return

	# @abc.abstractmethod
	def run(self):
		self.poller = zmq.Poller()
		self.broker_aliveness = self.BROKER_ALIVENESS
		self.setup()
		self.reconnect()
		self.loop()		

class DBWorker(MJDWorker):

	def do_work(self, workload):
		return MsgCode.ORDER_STORED

	def setup(self):
		pass

class Auth(MJDWorker):

	# def __init__(self, *args):
	# 	super(Auth, self).__init__(*args)
	# 	self.backend = self.context.socket(zmq.DEALER)
	# 	self.backend.connect(AddressManager.get_connect_address(self.backend_name))

	def setup(self):
		self.backend = self.context.socket(zmq.DEALER)
		self.backend.connect(AddressManager.get_connect_address(self.backend_name))		
		self.poller.register(self.backend, zmq.POLLIN)

	def authenticate_order(self, order):
		return True

	def do_work(self, order):
		if self.authenticate_order(order):
			Package(msg = order).send(self.backend)
			# package.send(backend)
			return MsgCode.ORDER_RECEIVED
		else:
			return MsgCode.INVALID_ORDER



	# def run(self):
	# 	super(Auth, self).run()
	# 	print('ASD')
	# 	self.poller.register(self.backend)


# class Auth(AgentProcess):
# 	"""
# 	REQ worker is frontend, connected to a broker
# 	backend is DEALER socket, handing jobs to DB broker

# 	"""
	


# 	def do_work(self, package):
# 		if self.check_order(package.msg):
# 			Package(msg = package.msg).send(self.backend)
# 		return result


# 	def check_order(self, order):
# 		# sleep(random())
# 		return True

# 	def run(self):
# 		frontend = self.context.socket(zmq.REQ)
# 		frontend.connect(AddressManager.get_connect_address(self.frontend_name))
# 		backend = self.context.socket(zmq.DEALER)
# 		backend.connect(AddressManager.get_connect_address(self.backend_name))

# 		poller = zmq.Poller()
# 		poller.register(frontend, zmq.POLLIN)
# 		poller.register(backend, zmq.POLLIN)

# 		self.say('ready')

# 		# frontend.send_multipart(WorkerMsg(STATUS_READY).message)

# 		Package(msg = MsgCode.STATUS_READY).send(frontend)

# 		pending = dict()

# 		while True:

# 			sockets = dict(poller.poll(100))
# 			if frontend in sockets:
# 				package = Package.recv(frontend)
# 				self.say('On frontend: {}'.format(package))
# 				if self.check_order(package.msg):
# 					### Send package to database
# 					self.say('Sending on backend: {}'.format(package))
			
# 					package.send(backend)
# 					### Send ACK back to trader
# 					result = self.do_work(package.msg)
# 					client_p = package.encapsulated
# 					client_p.msg = MsgCode.ORDER_RECEIVED
# 					broker_package = Package(msg = MsgCode.JOB_COMPLETE, encapsulated=client_p)
# 					self.say('Sending on frontend: {}'.format(broker_package))
# 					broker_package.send(frontend)

					
# 				else:
# 					raise Exception('Not implemented')
					
# 			if backend in sockets:
# 				### Message back from DB
# 				package = Package.recv(backend)
# 				self.say('On backend: {}'.format(package))
# 				# self.say(backend.recv_multipart())
