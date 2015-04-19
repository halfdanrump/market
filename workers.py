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
				self.say(request)
				client_id, empty, workload = request[0:3]
				if workload == 'quit': 
					break
				result = self.do_work()	
				self.frontend.send_multipart(WorkerMsg(STATUS_READY, client_id, str(result)).message)

		self.frontend.close()