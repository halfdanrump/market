import zmq
from zmq.eventloop import ioloop, zmqstream
from time import sleep
from multiprocessing import Process

ioloop.install()
context = zmq.Context()

def server():
	socket = context.socket(zmq.PUSH)
	socket.bind('tcp://*:6000')
	while True:
		socket.send('Yo')
		sleep(1)
	

def worker():
	def handle_socket(msg):
		print('In worker 1: {}'.format(msg))
	socket = context.socket(zmq.PULL)
	socket.connect('tcp://localhost:6000')
	stream_pull = zmqstream.ZMQStream(socket)
	stream_pull.on_recv(handle_socket)
	ioloop.IOLoop.instance().start()
	

def worker2():
	def handle_socket(msg):
		print('In worker 2: {}'.format(msg))
	socket = context.socket(zmq.PULL)
	socket.connect('tcp://localhost:6000')
	stream_pull = zmqstream.ZMQStream(socket)
	stream_pull.on_recv(handle_socket)
	ioloop.IOLoop.instance().start()

if __name__ == '__main__':
	Process(target = server).start()
	Process(target = worker).start()
	Process(target = worker2).start()