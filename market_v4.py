from __future__ import print_function
from multiprocessing import Process
import zmq

import itertools
from lib import *
import Queue
context = zmq.Context()



class SimpleBroker(AgentProcess):
	def run(self):
		backend = self.context.socket(zmq.ROUTER)
		backend.bind(AddressManager.get_bind_address('db_backend'))
		backend.identity = 'backend'
		while True:
			request = backend.recv_multipart()
			worker_id, msg, client_id = request[0], request[2], request[4]
			self.say('sending work to worker')
			client_id = '123'
			backend.send_multipart([worker_id, "", client_id, "", 'workload'])		
			









# class DBWorker(AgentProcess):

# 	def fail_randomly(self):
# 		if random() < 0.9: 
# 			message = 'ready'
# 		else: 
# 			message = 'failed'

# 	def run(self):
# 		socket = self.context.socket(zmq.REQ)
# 		socket.connect(AddressManager.get_connect_address('db_backend'))
# 		jobs_completed = 0
# 		# self.say('ready')
# 		socket.send_multipart(["ready", "", ""])	
# 		while True:
# 			request = socket.recv_multipart()
# 			# self.say("from broker: {}".format(request))
# 			client_id, empty, workload = request[0:3]
# 			if workload == 'quit': break
# 			# sleep(random())
# 			sum(range(1000))
						
# 			jobs_completed += 1
# 			# self.say("Finished working")
# 			socket.send_multipart(['job complete', empty, client_id])
# 		self.say('Jobs completed: {}'.format(jobs_completed))
# 		socket.close()


# class zmqSocket:
# 	def __init__(self, name, )

class MarketProxy(AgentProcess):

	def __init__(self, name_prefix, frontend_name, backend_name, context):
		super(MarketProxy, self).__init__(name_prefix, context)
		self.frontend_name = frontend_name
		self.backend_name = backend_name

	def run(self):
		frontend = self.context.socket(zmq.ROUTER)
		frontend.bind(AddressManager.get_bind_address(self.frontend_name))
		backend = self.context.socket(zmq.DEALER)
		backend.bind(AddressManager.get_bind_address(self.backend_name))
		zmq.proxy(frontend, backend)



class REQTrader(AgentProcess):
	"""
	no frontend
	backend is REQ or DEALER socket sending orders
	"""

	def __init__(self, context, name_prefix, frontend_name, backend_name):
		super(REQTrader, self).__init__(context, name_prefix)
		self.frontend_name = frontend_name
		self.backend_name = backend_name
		sleep(random())



	def run(self):
		backend = self.context.socket(zmq.DEALER)
		backend.connect(AddressManager.get_connect_address(self.backend_name))

		poller = zmq.Poller()
		poller.register(backend, zmq.POLLIN)

		while True:
			

			sockets = dict(poller.poll(1))
			if backend in sockets:
				ack = backend.recv()
				self.say(ack)
			else:
			
				order = 'new order {}'.format(random())
				backend.send_multipart(["", order])
				sleep(1)
			# ack = backend.recv_multipart()
			# self.say(ack)

class OrderRouter:
	"""
	frontend is ROUTER socket accepting authenticated orders
	backend is PUB socket that sends out orders
	"""

	def __init__(self, context, name_prefix, frontend_name, backend_name):
		self.frontend_name = frontend_name
		self.backend_name = backend_name

	def run(self):
		frontend = self.context.socket(zmq.ROUTER)
		frontend.bind(AddressManager.get_bind_address(self.frontend_name))
		backend = self.context.socket(zmq.PUB)	
		backend.bind(AddressManager.get_bind_address(self.backend_name))
		
		poller = zmq.Poller()
		poller.register(backend, zmq.POLLIN)
		poller.register(frontend, zmq.POLLIN)
		while True:
			pass

zmqSocket = namedtuple('zmqSocket', 'name type')


class Message(dict):
	def __init__(self, sender, receiver):
		self.sennder = sender
		self.receiver = receiver

	def __str__(self):
		pass

class OrderMessage(Message):
	pass

class TransactionMessage(Message):
	pass

class WorkerMessage(Message):
	def __init__(self, msg_to_broker, msg_to_client):
		pass


class Auth(AgentProcess):
	"""
	REQ worker is frontend, connected to a broker
	backend is DEALER socket, handing jobs to DB broker

	"""
	def __init__(self, context, name_prefix, frontend_name, backend_name):
		super(Auth, self).__init__(context, name_prefix)
		self.frontend_name = frontend_name
		self.backend_name = backend_name

	def check_order(self, order):
		sleep(random())
		return True

	def run(self):
		frontend = self.context.socket(zmq.REQ)
		print(self.frontend_name)
		frontend.connect(AddressManager.get_connect_address(self.frontend_name))
		backend = self.context.socket(zmq.DEALER)
		backend.connect(AddressManager.get_connect_address(self.backend_name))

		poller = zmq.Poller()
		poller.register(frontend, zmq.POLLIN)
		poller.register(backend, zmq.POLLIN)

		self.say('ready')
		frontend.send_multipart(["ready", "", ""])

		while True:

			sockets = dict(poller.poll(100))
			if frontend in sockets:
				msg = frontend.recv_multipart()
				trader_id, order = msg[0], msg[2]
				if self.check_order(order):
					
					### Send ACK back to trader
					frontend.send_multipart(["job complete", "", trader_id])
					### Send order to database
					backend.send_multipart([trader_id, "", order])
				else:
					frontend.send_multipart(["FAIL", "", trader_id])
				

			if backend in sockets:
				pass

# class Auction:s



# class WorkerPool(Agent):
# 	def run()


# def foo():
# 	print('Step 1: %s'%sum(range(10000000)))
# 	sleep(3)
# 	print('Step 2: %s'%sum(range(10000000)))

# def owner():
# 	Thread(target = foo).start()



AddressManager.register_endpoint('db_frontend', 'tcp', 'localhost', 5560)
AddressManager.register_endpoint('db_backend', 'tcp', 'localhost', 5561)

AddressManager.register_endpoint('market_frontend', 'tcp', 'localhost', 5562)
AddressManager.register_endpoint('market_backend', 'tcp', 'localhost', 5563)


if __name__ == '__main__':
	
	for i in xrange(1): REQTrader(context, 'trader', None, 'market_frontend').start()	
	
	market_broker = BrokerWithQueueing(context, 'market_gateway', 'market_frontend', 'market_backend')
	market_broker.start()
	
	for i in xrange(1): Auth(context, 'authenticator', 'market_backend', 'db_frontend').start()

# 	Auction(context, 'auction').start()
	db_broker = BrokerWithPool(context, 'db_pool', 'db_frontend', 'db_backend')
	db_broker.start()

	# sleep(1)
	
	





	# alive_event = Event()
	# alive_event.set()
	# for j in xrange(10): 
	# 	print('NEW WORKER')
	# 	ThreadDBWorker('worker', context, alive_event).start()	
	

	# for j in xrange(10): DBWorker('worker', context).start()
	# context.term()
	# Thread(target = foo).start()
	# Process(target = owner).start()
