import zmq
from zmq.eventloop import ioloop, zmqstream
from multiprocessing import Process
import abc
from datetime import datetime


class Agent(Process):

	__metaclass__ = abc.ABCMeta

	def __init__(self, name):
		Process.__init__(self)		
		self.context = zmq.Context()
		self.loop = ioloop.IOLoop.instance()
		
	def run(self):
		self.say('Setting up agent...')
		self.setup()
		self.say('Starting ioloop...')
		self.loop.start()

	@abc.abstractmethod
	def setup(self):
		return
		
	def say(self, msg):
		print('{} - {}: {}'.format(datetime.now().strftime('%H:%M:%S'), self.name, msg))

class Server(Agent):
	def handle_socket(self, msg):
		address, m = msg[0], msg[2]
		self.say(m)
		self.socket.send_multipart([address, '', 'REQUEST OK'])
	
	def setup(self):
		self.socket = self.context.socket(zmq.ROUTER)
		self.socket.bind('tcp://*:6000')
		stream_pull = zmqstream.ZMQStream(self.socket)
		stream_pull.on_recv(self.handle_socket)



from time import sleep

class Client(Agent):
	i = 0

	def handle_socket(self, msg):
		self.say("{} ,".format(self.i) + "".join(msg))
		self.i += 1
		self.socket.send_multipart(["", 'NEW REQUEST'])

	def reconnect(self):
		self.say('Reconnecting...')
		sleep(2)
		self.stream.flush()
		self.stream.close()
		self.setup()
	
	def setup(self):
		ioloop.DelayedCallback(self.reconnect, 3000, self.loop).start()
		self.socket = self.context.socket(zmq.DEALER)
		self.socket.connect('tcp://localhost:6000')
		self.stream = zmqstream.ZMQStream(self.socket)
		self.stream.on_recv(self.handle_socket)
		self.socket.send_multipart(["", 'NEW REQUEST'])

if __name__ == '__main__':
	Server(name = 'server').start()
	Client(name = 'client').start()

