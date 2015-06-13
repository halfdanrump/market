import zmq
from zmq.eventloop import ioloop, zmqstream
from time import sleep
from multiprocessing import Process



def server():
	# ioloop.install()
	context = zmq.Context()
	def handle_socket(msg):
		address, m = msg[0], msg[2]
		print('In server: {}'.format(m))
		socket.send_multipart([address, '', 'REQUEST OK'])
	
	socket = context.socket(zmq.ROUTER)
	socket.bind('tcp://*:6000')
	stream_pull = zmqstream.ZMQStream(socket)
	stream_pull.on_recv(handle_socket)
	loop = ioloop.IOLoop.instance()
	loop.start()


def client():
	# ioloop.install()
	context = zmq.Context()
	def handle_socket(msg):
		print('In client: {}'.format(msg))
		socket.send_multipart(["", 'NEW REQUEST'])
	
	socket = context.socket(zmq.DEALER)
	socket.connect('tcp://localhost:6000')
	stream_pull = zmqstream.ZMQStream(socket)
	stream_pull.on_recv(handle_socket)
	socket.send_multipart(["", 'NEW REQUEST'])
	ioloop.IOLoop.instance().start()

if __name__ == '__main__':
	Process(target = server).start()
	Process(target = client).start()

class AgentProcess(Process):

	__metaclass__ = abc.ABCMeta

	def __init__(self, name):
		Process.__init__(self)		
		self.context = zmq.Context()
		
		
	def say(self, msg):
		# if not (re.match('.*{}.*'.format(MsgCode.PING), str(msg)) or re.match('.*{}.*'.format(MsgCode.PONG), str(msg))):
		print('{} - {}: {}'.format(datetime.now().strftime('%H:%M:%S'), self.name, msg))
		 # or not re.match('.*{}.*'.format(MsgCode.PONG), msg):
			