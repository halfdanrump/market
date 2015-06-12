import zmq
from multiprocessing import Process
from time import sleep

addr1 = 'ipc://broker1'
addr2 = 'ipc://broker2'
context = zmq.Context()

def worker():
	socket = context.socket(zmq.DEALER) 
	socket.connect(addr1)
	socket.connect(addr2)
	while True:
		socket.send_multipart(["", "Hello"])
		print(socket.recv_multipart())
		sleep(1)

def broker_1():
	socket = context.socket(zmq.ROUTER) 
	socket.bind(addr1)
	while True:
		addr, e, msg = socket.recv_multipart()
		print("In broker 1: %s"%addr)
		socket.send_multipart([addr, "", "hello from broker 1"])

def broker_2():
	socket = context.socket(zmq.ROUTER) 
	socket.bind(addr2)
	while True:
		addr, e, msg = socket.recv_multipart()
		print("In broker 2: %s"%addr)
		socket.send_multipart([addr, "", "hello from broker 2"])


if __name__ == '__main__':
	Process(target = worker).start()
	Process(target = broker_1).start()
	Process(target = broker_2).start()