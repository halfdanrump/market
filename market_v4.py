from __future__ import print_function
from multiprocessing import Process
import zmq
from time import sleep
from random import random
import itertools
from lib import *
import Queue
context = zmq.Context()


DB_FRONTEND = {'protocol': 'tcp', 'host': 'localhost', 'port': 5561} 
DB_BACKEND = {'protocol': 'tcp', 'host': 'localhost', 'port': 5560}

AddressManager.register_endpoint('db_frontend', **DB_FRONTEND)
AddressManager.register_endpoint('db_backend', **DB_BACKEND)

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
			



class OrderProxy(AgentProcess):

	def run(self):
		
		frontend = self.context.socket(zmq.ROUTER)
		frontend.bind(AddressManager.get_bind_address('db_frontend'))
		backend = self.context.socket(zmq.DEALER)
		backend.bind(AddressManager.get_bind_address('db_frontend'))
		zmq.proxy(frontend, backend)





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








# class WorkerPool(Agent):
# 	def run()


# def foo():
# 	print('Step 1: %s'%sum(range(10000000)))
# 	sleep(3)
# 	print('Step 2: %s'%sum(range(10000000)))

# def owner():
# 	Thread(target = foo).start()




if __name__ == '__main__':
	# BrokerWithQueueing('broker', context, 'db_frontend', 'db_backend').start()
	
	Auction('auction', context).start()
	BrokerWithPool('db_frontend', 'db_backend', 2, 10, context).start()
	# alive_event = Event()
	# alive_event.set()
	# for j in xrange(10): 
	# 	print('NEW WORKER')
	# 	ThreadDBWorker('worker', context, alive_event).start()	
	

	# for j in xrange(10): DBWorker('worker', context).start()
	# context.term()
	# Thread(target = foo).start()
	# Process(target = owner).start()
