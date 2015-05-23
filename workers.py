from lib import *
from random import randint




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

		self.new_socket(endpoint = self.frontend_name, socket_name = 'frontend', socket_type = zmq.DEALER, bind = False, handler = self.handle_frontend)
		self.register_at_broker()

	

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

	def loop(self):
		while True:
			self.iteration()

	def handle_frontend(self):
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

	def iteration(self):
		self.poll_sockets()
		# sockets = dict(self.poller.poll())
		# if self.frontend in sockets:
		# 	self.handle_frontend()
			
	def run(self):
		self.poller = zmq.Poller()
		self.broker_aliveness = self.BROKER_ALIVENESS
		self.setup()
		self.reconnect()
		self.loop()
		self.frontend.close()	

	def register_at_broker(self):
		package = Package(msg = MsgCode.STATUS_READY)
		self.send(self.frontend, package)



	@abc.abstractmethod
	def setup(self):
		return

	@abc.abstractmethod
	def do_work(self, workload):
		return



class Auction(MJDWorker):

	def setup(self):
		self.pending_orders = []

	def do_work(self, order):
		self.pending_orders.append(order)
		self.say(self.pending_orders)

	def register_at_broker(self):
		name_p = Package(msg = self.name)
		package = Package(msg = MsgCode.STATUS_READY, encapsulated = name_p)
		self.send(self.frontend, package)		



class DBWorker(MJDWorker):

	def setup(self):
		### Nothing to setup
		pass

	def do_work(self, workload):
		self.say('Doing work: {}'.format(workload))
		return MsgCode.ORDER_STORED


# socket = {	'name' : 'backend', 
# 			'endpoint' : 'tcp://localhost:5000',
# 			'type' : zmq.DEALER,
# 			'bind': False,
# 			'recv_handler' : 
# 			'send_handler' : 
# 			}


class Auth(MJDWorker):

	# def __init__(self, name, frontend, backend_db, backend_auction, verbose = False):
	# 	super(Auth, self).__init__(name = name, frontend = frontend, backend = backend_db, verbose = verbose)
	# 	self.backend_auction_name = backend_auction_name

	def handle_backend(self):
		self.say('On backend: {}'.format(self.backend.recv_multipart()))

	def setup(self):
		self.new_socket(endpoint = self.backend_name, socket_name = 'backend', socket_type = zmq.DEALER, bind = False, handler = self.handle_backend)
		# self.poller.register(self.backend, zmq.POLLIN)
		# self.backend = self.context.socket(zmq.DEALER)
		# self.backend.connect(AddressManager.get_connect_address(self.backend_name))		
		# self.poller.register(self.backend, zmq.POLLIN)

	def authenticate_order(self, order):
		return True

	def do_work(self, order):
		self.say('Forwarding order to db_cluster')
		if self.authenticate_order(order):
			Package(msg = order).send(self.backend)
			return MsgCode.ORDER_RECEIVED
		else:
			return MsgCode.INVALID_ORDER
