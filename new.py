from uuid import uuid4
import zmq
from zmq.eventloop import ioloop, zmqstream
from multiprocessing import Process
import abc
from datetime import datetime
from lib import AddressManager, MsgCode
from random import random

class Agent(Process):

	__metaclass__ = abc.ABCMeta

	def __init__(self, name, endpoints):
		Process.__init__(self)
		### Make assertions about endpoints
		self.endpoints = endpoints		
		self.context = zmq.Context()
		self.loop = ioloop.IOLoop.instance()
		
	def get_endpoint(self, name):
		return self.endpoints[name]

	def stream(self, name, socket_type, bind, handler):
		""" create a new stream """
		socket = self.context.socket(socket_type)
		# print(socket)
		endpoint = self.get_endpoint(name)
		if bind: 
			address = AddressManager.get_bind_address(endpoint)
			socket.bind(address)
		else:
			address = AddressManager.get_connect_address(endpoint)
			socket.connect(address)
		stream = zmqstream.ZMQStream(socket)
		stream.on_recv(handler)	
		# setattr(self, name + '_stream', stream)
		# setattr(self, name + '_socket', socket)
		return stream, socket
	
	def simulate_overload(self, probability = 0.1):
		""" Simulate CPU overload by sleeping for some time. """
		if random() < probability: 
			self.say('I am busy')
			sleep(5)

	def simulate_crash(self, probability = 0.1):
		""" Simulate crash forcing IndexError"""
		if random() < probability: 
			l = list()
			l[0]

	def run(self):
		""" overrides Process.run(). This will be called when start() is called """
		self.say('Setting up agent...')
		self.setup()
		self.say('Starting ioloop...')
		self.loop.start()

	def say(self, msg):
		print('{} - {}: {}'.format(datetime.now().strftime('%H:%M:%S'), self.name, msg))

	@abc.abstractmethod
	def setup(self):
		""" Will usually be run a single time when the process is started. Should be used to create sockets, bind handlers, create instance variables and so on. """
		return
		
	

class Server(Agent):
	def handle_socket(self, msg):
		address, m = msg[0], msg[2]
		self.say(m)
		timer = ioloop.DelayedCallback(lambda: self.send_reply(address), 1000)
		self.workers[address] = (timer, m)
		timer.start()
		# self.socket.send_multipart([address, '', 'REQUEST OK'])

	def send_reply(self, address):
		reply = self.workers.get(address)[1]
		self.socket.send_multipart([address, '', '******' + reply])

	def setup(self):
		self.workers = {}
		self.stream, self.socket = self.stream('backend', zmq.ROUTER, True, self.handle_socket)


from time import sleep

class Client(Agent):
	i = 0

	def handle_socket(self, msg):
		self.say("{} ,".format(self.i) + "".join(msg))
		self.i += 1
		self.socket.send_multipart(["", 'NEW REQUEST FROM WORKER %s'%self.name])

	def reconnect(self):
		self.say('Reconnecting...')
		# self.stream.flush()
		# self.stream.close()
		# self.socket.close()
		# sleep(3)
		del self.stream
		del self.socket
		self.setup()
	
	def setup(self):
		ioloop.DelayedCallback(self.reconnect, 30000, self.loop).start()
		self.stream, self.socket = self.stream('frontend', zmq.DEALER, False, self.handle_socket)
		self.socket.send_multipart(["", 'READY %s'%self.name])



from datetime import datetime, timedelta
from heapdict import heapdict





from transitions import Machine



class PPWorker(Agent):
	

	def EVENT_frontend_msg(self, msg):
		msg = msg[1]
		self.say(msg)
		self.state = 3
		if msg == MsgCode.PONG:
			self.action_reset_timer()
		else:
			self.action_stop_timer()
			self.action_do_work(msg)

	def event_timer_exp(self):
		self.state -= 1
		if self.state > 0:
			self.action_reset_timer()
			self.action_send_ping()
		else:
			self.action_reset_timer()
			self.action_reconnect()
			self.action_send_ready()



	def event_finish_work(self, result):
		self.frontend.send_multipart(['', result])
		self.action_reset_timer()

	def action_start_timer(self):
		self.timer = ioloop.DelayedCallback(self.event_timer_exp, 1000, self.loop)
		self.timer.start()

	def action_stop_timer(self):
		self.timer.stop()

	def action_reset_timer(self):
		self.action_stop_timer()
		self.action_start_timer()

	def action_connect_frontend(self):
		self.frontend,s = self.stream('frontend', zmq.DEALER, False, self.EVENT_frontend_msg)

	def action_reconnect(self):
		self.frontend.close()
		del self.frontend
		self.action_connect_frontend()


	def action_do_work(self, task):
		result = 'Work done: %s'%task
		self.event_finish_work(result)


	def action_send_ping(self):
		self.frontend.send_multipart(['', MsgCode.PING])

	def action_send_ready(self):
		self.frontend.send_multipart(['', MsgCode.STATUS_READY])

	
	def setup(self):
		self.state = 6
		self.action_connect_frontend()
		self.action_start_timer()



class PPStateWorker(Agent):

	states = ['connecting', 'idle', 'working']

	def __init__(self, *args, **kwargs):
		super(PPStateWorker, self).__init__(*args, **kwargs)
		self.machine = Machine(model = self, states = PPStateWorker.states, initial = 'connecting')
		
		self.machine.add_transition(trigger = 'complete_connect', source = 'connecting', dest = 'idle', after = ['send_ready', 'start_timer', 'reset_alive'])
		
		self.machine.add_transition(trigger = 'recv_job', source = 'idle', dest = 'working', after = ['reset_alive', 'stop_timer'])
		self.machine.add_transition(trigger = 'recv_not_job', source = 'idle', dest = 'idle', after = ['reset_alive', 'reset_timer'])
		self.machine.add_transition(trigger = 'broker_expired', source = 'idle', dest = 'idle', after = ['decrement_alive', 'send_ping', 'reset_timer'])
		self.machine.add_transition(trigger = 'broker_dead', source = 'idle', dest = 'connecting', after = ['reset_alive', 'reconnect_frontend', 'send_ready'])
		
		self.machine.add_transition(trigger = 'job_sent', source = 'working', dest = 'idle', after = ['reset_timer'])
		self.machine.add_transition(trigger = 'fail_job', source = 'working', dest = 'idle', after = ['send_error', 'reset_timer'])

	def connect_frontend(self):
		self.frontend,s = self.stream('frontend', zmq.DEALER, False, self.EVENT_frontend_msg)

	def reconnect_frontend(self):
		self.frontend.close()
		del self.frontend
		self.connect_frontend()
		self.complete_connect()

	def send_ping(self):
		self.frontend.send_multipart(['', MsgCode.PING])

	def send_ready(self):
		self.frontend.send_multipart(['', MsgCode.STATUS_READY])

	def decrement_alive(self):
		self.alive -= 1

	def reset_alive(self):
		self.alive = 0

	def start_timer(self):
		self.timer = ioloop.DelayedCallback(self.event_timer_exp, 2000, self.loop)
		self.timer.start()

	def stop_timer(self):
		self.timer.stop()

	def reset_timer(self):
		self.stop_timer()
		self.start_timer()

	def event_timer_exp(self):
		if self.alive > 0:
			self.broker_expired()
		else:
			self.broker_dead()
			
	def EVENT_frontend_msg(self, msg):
		msg = msg[1]
		self.i += 1
		self.say(str(self.i) + msg)
		if msg == MsgCode.PONG:
			self.recv_not_job()
		else:
			self.recv_job()
			result = self.do_work()
			self.frontend.send_multipart(['', result])
			self.job_sent()


	def setup(self):
		self.i = 0
		self.connect_frontend()
		self.complete_connect()
		

from Queue import Queue
from uuid import uuid4
from collections import OrderedDict



class StateBroker(Agent):

	# states = ['connecting', 'accepting', 'full']
	# transitions = [
	# 	{'source' : 'accepting', 'trigger' : 'recv_ready', 'dest' : 'accepting', 'after' : 'add_worker'},
	# 	{'source' : 'accepting', 'trigger' : 'recv_result', 'dest' : 'accepting', 'after' :},
	# 	{'source' : 'accepting', 'trigger' :, 'dest' : 'accepting', 'after' :},
	# 	{'source' : 'accepting', 'trigger' :, 'dest' : 'accepting', 'after' :},
	# 	{'source' : 'accepting', 'trigger' :, 'dest' : 'accepting', 'after' :},
	# 	{'source' :, 'trigger' :, 'dest' :, 'after' :},
	# ]


	# }
	def __init__(self, *args, **kwargs):
		super(StateBroker).__init__(*args, **kwargs)
		self.workers = OrderedDict() ### Ordered map from worker_addr -> timer. Used both as a queue of idle workers, and to get store worker timers
		self.in_progress = {} ### map from job_token -> client_id. Used to keep track of jobs assigned to workers and to store the address of the client that made the request

	def setup(self):
		self.connect_frontend()
		self.connect_backend()
		self.complete_connect()

	def connect_frontend(self):
		self.frontend = self.stream('frontend', zmq.ROUTER, True, self.EVENT_frontend_recv)

	def connect_backend(self):
		self.frontend = self.stream('backend', zmq.ROUTER, True, self.EVENT_backend_recv)

	def EVENT_frontend_recv(self, msg):
		client, job = msg[0], msg[2]
		self.recv_job()
		if len(self.workers) >= 0:
			self.forward_job(client, job)
		else:
			self.deny_request(client)

	def EVENT_backend_recv(self, msg):
		# if len(msg) == 
		worker = msg[0]
		worker, token, result = msg[::2]
		if result == MsgCode.STATUS_READY:
			pass
		elif result == MsgCode.ERROR:
			self.forward_error(token, result)
		else:
			self.forward_result(token, result)
		self.add_worker(worker)

	def event_expire_worker(self, addr):
		timer = self.workers[addr]
		timer.stop()
		del self.workers[addr]
	
	def add_worker(self, worker_addr):
		self.__store_worker_addr(worker_addr)
		self.__send_pong(worker_addr)
		self.__restart_worker_timer(worker_addr)

	def forward_job(self, client, job):
		worker = self.__get_ready_worker()
		token = self.__generate_job_id()
		self.__store_pending_job(token, client)
		self.send_job(job, token, worker)

	def forward_result(self, token, result):
		client = self.__get_job_client(token)
		self.__remove_pending_job(token)
		self.__send_result(client, result)

	def forward_error(self, token, error):
		client = self.__get_job_client(token)
		self.__remove_pending_job(token)
		self.__send_result(client, error)		

	def deny_request(self, client):
		self.frontend.send_multipart([client, "", MsgCode.QUEUE_FULL])

	def __send_pong(self, worker_addr):
		self.backend.send_multipart([worker_addr, "", Msg.PONG])

	def __send_job(self, job, token, worker_addr):
		self.backend.send_multipart([worker_addr, "", token, "", job])

	def __send_result(client, result):
		self.frontend.send_multipart([client, "", result])

	def __store_worker_addr(self, addr):
		timer = ioloop.DelayedCallback(lambda: self.event_expire_worker(addr), 5000, self.loop)
		timer.start()
		self.workers[addr] = timer

	def __get_ready_worker(self):
		addr, timer = self.workers.popitem()
		timer.stop()
		return addr

	def __store_pending_job(self, token, client):
		self.in_progress[token] = client

	def __remove_pending_job(self, token):
		del self.in_progress[token]

	def __get_job_client(self, token):
		return self.in_progress[token]

	


	# def __start_worker_timer(self, addr):
	# 	self.timers[addr] = ioloop.DelayedCallback(lambda: self.event_expire_worker(addr), 5000, self.loop)
	# 	self.timers[addr].start()

	# def __stop_worker_timer(self, addr):
	# 	timer = self.timers[addr]
	# 	timer.stop()

	# def __restart_worker_timer(self, addr):
	# 	self.__stop_worker_timer(addr)
	# 	self.__start_worker_timer(addr)

	def __generate_job_id(self):
		return uuid4().HEX


		
	
# class JobQueueBroker(PingPongBroker):


	
# 	def setup(self):
# 		super(JobQueueBroker, self).setup()
# 		self.jobs = DQueue(item_type = Job)
# 		self.jobs_received = 0

# 	def send_job(self):
# 		worker_addr = self.workers.popitem()[0]
# 		job = self.jobs.get()
# 		client_p = Package(dest_addr = job.client)
# 		package = Package(dest_addr = worker_addr, msg = job.work, encapsulated = client_p)
# 		self.say('sending on backend: {}'.format(package))
# 		package.send(self.backend)

# 	def handle_frontend(self):
# 		package = Package.recv(self.frontend)
# 		self.say('On frontend: {}'.format(package))
# 		job = Job(client=package.sender_addr, work=package.msg)
# 		self.jobs_received += 1
# 		self.jobs.put(job)

# 	def handle_backend(self, package):
# 		if package.msg == MsgCode.JOB_COMPLETE:
# 			if package.encapsulated:
# 				### Forward result from worker to client
# 				self.say('Sending on frontend: {}'.format(package.encapsulated))
# 				package.encapsulated.send(self.frontend)





AddressManager.register_endpoint('market_frontend', 'tcp', 'localhost', 6001)
AddressManager.register_endpoint('market_backend', 'tcp', 'localhost', 6002)












if __name__ == '__main__':
	PingPongBroker(name = 'server', endpoints = {'backend' : 'market_backend'}).start()
	PingPongWorker(name = 'client', endpoints = {'frontend' : 'market_backend'}).start()
	# Server(name = 'server', endpoints = {'backend' : 'market_backend'}).start()
	# Client(name = 'client', endpoints = {'frontend' : 'market_backend'}).start()

