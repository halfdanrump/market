from lib import *
import itertools
import atexit
import sys




context = zmq.Context()


class Trader(AgentProcess):

	__sockets__ = [Sock('backend', zmq.DEALER, False, 'backend_handler')]
	
	def backend_handler(self):
		m = self.backend.recv_multipart()
		self.say(str(m))

	def iteration(self):
		# print('Iterate')
		self.poll_sockets()
		# Thread(target = self.poll_sockets).start()
		# self.poll_sockets()
		order = 'new order {}'.format(random())
		package = Package(msg = order)
		self.say('Sending on backend: {}'.format(package))
		package.send(self.backend)
		# print(Package.recv(self.backend))
		sleep(1)

		
	

	def setup(self):
		pass

class TeztAgentReconnect(AgentProcess):

	__sockets__ = [Sock('backend', zmq.DEALER, False, 'backend_handler')]
	
	def backend_handler(self):
		m = self.backend.recv_multipart()
		# self.say(str(m))

	def iteration(self):
		# print('Iterate')
		# self.poll_sockets()
		# Thread(target = self.poll_sockets).start()
		self.poll_sockets()
		order = 'new order {}'.format(random())
		package = Package(msg = order)
		self.say('Sending on backend: {}'.format(package))
		package.send(self.backend)
		# sleep(1)
		self.say(Package.recv(self.backend))
		self.init_socket(self.sockets['backend'])
		self.say('Done initializing')
	
	

	def setup(self):
		pass
