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
		sleep(1)
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
				package = Package.recv(frontend)
				self.say('On frontend: {}'.format(package))
				if self.check_order(package.msg):
					### Send package to database
					self.say('Sending on backend: {}'.format(package))
					Package(msg = package.msg).send(backend)
					# package.send(backend)
					### Send ACK back to trader
					client_p = package.encapsulated
					client_p.msg = MsgCode.ORDER_RECEIVED
					broker_package = Package(msg = MsgCode.JOB_COMPLETE, encapsulated=client_p)
					self.say('Sending on frontend: {}'.format(broker_package))
					broker_package.send(frontend)

					
				else:
					raise Exception('Not implemented')
					
			if backend in sockets:
				### Message back from DB
				package = Package.recv(backend)
				self.say('On backend: {}'.format(package))
				# self.say(backend.recv_multipart())
