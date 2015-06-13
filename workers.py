from lib import *
from random import randint
from threading import Timer


class PingPongWorker(AgentProcess):

	__sockets__ = [
	Sock('frontend', zmq.DEALER, bind = False, handler = 'handle_frontend')
	]
	__metaclass__ = abc.ABCMeta

	BROKER_TIMEOUT = 1;
	BROKER_ALIVENESS = 3 # Number of timeouts before the worker tries to reconnect to the broker

	def ping(self, socket):
		ping = Package(msg = MsgCode.PING)
		self.send(socket, ping)

	def pong(self, socket):
		pong = Package(msg = MsgCode.PONG)
		self.send(socket, pong)

	def reconnect_to_broker(self):
		self.say('Connecting to broker...')
		self.broker_aliveness = self.BROKER_ALIVENESS
		self.reinit_socket(self.sockets['frontend'])
		self.send_ready_msg()

	def send_ready_msg(self):
		package = Package(msg = MsgCode.STATUS_READY)
		self.send(self.frontend, package)

	def update_aliveness(self):
		self.broker_aliveness -= 1
		# self.say('aliveness: {}'.format(self.broker_aliveness))
		if self.broker_aliveness == 0:
			print('ASDHASODIJ')
			# self.broker_aliveness = self.BROKER_ALIVENESS
			self.reconnect_to_broker()
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
		address = self.sockets[socket].address
		self.say('Sending to {}: {}'.format(address, package))
		self.reset_timer()
		package.send(socket)

	def recv(self, socket):
		package = Package.recv(socket)
		self.say(package)
		### As soon as the worker recieves a message from the broker it resets the broker_aliveness
		self.reset_timer()
		self.broker_aliveness = self.BROKER_ALIVENESS
		return package

	def loop(self):
		while True:
			self.iteration()

	def handle_frontend(self):
		package = self.recv(self.frontend)
		# print(package)
		if package.msg == MsgCode.PING:
			### Means that broker is getting impatient, so reply with a PONG
			self.pong()
		self.say('On frontend: {}'.format(package))
		if package.encapsulated:
			result = self.do_work(package.msg)
			assert isinstance(result, str)
								
			client_p = package.encapsulated
			client_p.msg = result
			broker_p = Package(msg = MsgCode.JOB_COMPLETE, encapsulated = client_p)
			self.say('Sending on frontend: {}'.format(broker_p))
			self.send(self.frontend, broker_p)

	def iteration(self):
		self.poll_sockets()
			
	def run(self):
		self.init_all_sockets()
		self.broker_aliveness = self.BROKER_ALIVENESS
		self.setup()
		self.send_ready_msg()
		self.loop()

	@abc.abstractmethod
	def setup(self):
		return

	@abc.abstractmethod
	def do_work(self, workload):
		return


class Auction(PingPongWorker):

	def setup(self):
		self.pending_orders = []

	def do_work(self, order):
		self.pending_orders.append(order)
		self.say(self.pending_orders)

	def register_at_broker(self):
		name_p = Package(msg = self.name)
		package = Package(msg = MsgCode.STATUS_READY, encapsulated = name_p)
		self.send(self.frontend, package)		



class DBWorker(PingPongWorker):

	def setup(self):
		### Nothing to setup
		pass

	def do_work(self, workload):
		# print('Doing work: {}'.format(workload))
		self.say('Doing work: {}'.format(workload))
		return MsgCode.ORDER_STORED


# socket = {'name' : 'backend', 
# 			'endpoint' : 'tcp://localhost:5000',
# 			'type' : zmq.DEALER,
# 			'bind': False,
# 			'recv_handler' : 
# 			'send_handler' : 
# 			}


class AuthWorker(PingPongWorker):
	# __sockets__ = [
	# Sock('frontend', zmq.DEALER, bind = False, handler = 'handle_frontend'),
	# # Sock('backend', zmq.DEALER, bind = False, handler = 'handle_backend')
	# ]
	
	def setup(self):
		pass

	def authenticate_order(self, order):
		return True

	def handle_backend(self):
		self.say('In backend')

	def do_work(self, order):
		self.say('Forwarding order to db_cluster')
		if self.authenticate_order(order):
			# Package(msg = order).send(self.backend)
			return MsgCode.ORDER_RECEIVED
		else:
			return MsgCode.INVALID_ORDER
