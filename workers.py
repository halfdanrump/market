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
	

	def do_work(self):
		sleep(1)
		return sum(range(randint(0, 1000)))
		
	def run(self):
		self.frontend = self.context.socket(zmq.REQ)
		self.frontend.connect(AddressManager.get_connect_address(self.frontend_name))
		self.poller = zmq.Poller()
		self.poller.register(self.frontend, zmq.POLLIN)
		# self.frontend.send_multipart(WorkerMsg(STATUS_READY).message)	
		Package(msg = MsgCode.STATUS_READY).send(frontend)
		while True:
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


class Auth(AgentProcess):
	"""
	REQ worker is frontend, connected to a broker
	backend is DEALER socket, handing jobs to DB broker

	"""
	
	def check_order(self, order):
		sleep(random())
		return True

	def run(self):
		frontend = self.context.socket(zmq.REQ)
		frontend.connect(AddressManager.get_connect_address(self.frontend_name))
		backend = self.context.socket(zmq.DEALER)
		backend.connect(AddressManager.get_connect_address(self.backend_name))

		poller = zmq.Poller()
		poller.register(frontend, zmq.POLLIN)
		poller.register(backend, zmq.POLLIN)

		self.say('ready')

		# frontend.send_multipart(WorkerMsg(STATUS_READY).message)

		Package(msg = MsgCode.STATUS_READY).send(frontend)

		pending = dict()

		while True:

			sockets = dict(poller.poll(100))
			if frontend in sockets:
				order = Package.recv(frontend)
				# order = frontend.recv_multipart()
				
				# order = msg[0], msg[2]
				self.say('On frontend: {}'.format(order))
				if self.check_order(order.msg):
					### Send order to database
					self.say('Sending on backend: {}'.format(order))
					order.send(backend)

					### Send ACK back to trader
					# frontend.send(MsgCode.ORDER_RECEIVED)
					client_package = Package(msg = MsgCode.ORDER_RECEIVED)
					broker_package = Package(msg = MsgCode.JOB_COMPLETE, encapsulated=client_package)
					broker_package.send(frontend)

					# msg = WorkerMsg(STATUS_READY, trader_id, ORDER_RECEIVED)
				else:
					raise Exception('Not implemented')
					
			if backend in sockets:
				### Message back from DB
				self.say(backend.recv_multipart())
