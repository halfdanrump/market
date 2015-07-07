from lib import *
from uuid import uuid4
from ping_pong import PingPongBroker
from datetime import datetime
import ujson
from collections import OrderedDict		
	

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





from transitions import Machine



class PPWorker(Agent):
	

	def EVENT_frontend_msg(self, msg):
		payload = ujson.loads(msg[0])
		self.say(payload)
		self.state = 3
		if payload['status'] == MsgCode.PONG:
			self.action_reset_timer()
		elif payload['status'] == MsgCode.WORK:
			self.action_stop_timer()
			reply = {'result': self.action_do_work(payload['job']), 'token': payload['token'], 'status': MsgCode.SUCCESS}
			self.state = 3
			self.frontend.send(ujson.dumps(reply))


	def event_timer_exp(self):
		self.state -= 1
		if self.state > 0:
			self.action_reset_timer()
			self.action_send_ping()
		else:
			self.action_reset_timer()
			self.action_reconnect()
			self.action_send_ready()

	def event_finish_work(self):
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
		self.say('Reconnecting...')
		self.frontend.close()
		del self.frontend
		self.action_connect_frontend()

	def action_do_work(self, task):
		result = 'Work done: %s'%task
		self.event_finish_work()
		return result

	def action_send_ping(self):
		self.frontend.send(ujson.dumps({'status': MsgCode.PING}))

	def action_send_ready(self):
		self.frontend.send(ujson.dumps({'status': MsgCode.STATUS_READY}))

	def action_send_result(self, result):
		self.frontend.send(ujson.dumps({'status': MsgCode.STATUS_READY, 'msg': result}))

	
	def setup(self):
		self.state = 3
		self.action_connect_frontend()
		self.action_start_timer()




import inspect
from termcolor import colored
def debug(s):
	print(colored(s, 'red'))

class TraderClient(Agent):
	def setup(self):
		self.__connect_backend()
		self.start_trading_alg()

	def start_trading_alg(self):
		ioloop.PeriodicCallback(self.send_order, 3000, self.loop).start()

	def send_order(self):
		self.backend.send(ujson.dumps({'order': {'auction_id': '123', 'side': 'sell', 'price': 100, 'expire': 60}}))

	def EVENT_backend_recv(self, msg):
		debug(msg[0])
		payload = ujson.loads(msg[0])
		self.say(payload)

	def __connect_backend(self):
		self.backend, s = self.stream('backend', zmq.DEALER, False, self.EVENT_backend_recv)



# class Handler(object):

# 	def __call__(self):


class WorkerQueue(OrderedDict):
	

	def put(self, worker_addr):
		self.__store_worker_addr(worker_addr)

	def popitem(self):
		return __get_ready_worker()

	def __store_worker_addr(self, addr):
		if self.workers.has_key(addr):
			self.workers[addr].stop()
		timer = ioloop.DelayedCallback(lambda: self.event_expire_worker(addr), 5000, self.loop)
		timer.start()
		self.workers[addr] = timer

	def __get_ready_worker(self):
		addr, timer = self.workers.popitem()
		timer.stop()
		return addr



class MarketBroker(Agent):

	
		

	def setup(self):
		self.workers = OrderedDict() ### Ordered map from worker_addr -> timer. Used both as a queue of idle workers, and to get store worker timers
		self.in_progress = {} ### map from job_token -> client_id. Used to keep track of jobs assigned to workers and to store the address of the client that made the request
		self.__connect_frontend()
		self.__connect_backend()
	
	def EVENT_frontend_recv(self, msg):
		client, payload = msg[0], ujson.loads(msg[1])
		if len(self.workers) > 0:
			self.forward_job(client, payload['order'])
		else:
			self.deny_request(client)

	def EVENT_backend_recv(self, msg):
		worker, payload = msg[0], ujson.loads(msg[1])
	
		status = payload.get('status')
		if status == MsgCode.STATUS_READY:
			pass
		elif status == MsgCode.SUCCESS:
			self.forward_result(payload['token'], payload['result'])
		elif status == MsgCode.ERROR:
			self.forward_error(payload['token'], payload['msg'])
		elif status == MsgCode.PING:
			pass
		else:
			raise Exception('Worker status must be either READY, SUCCESS, ERROR or PING')
		self.add_worker(worker)

	def event_expire_worker(self, addr):
		timer = self.workers[addr]
		timer.stop()
		del self.workers[addr]
		# del timer
	
	def add_worker(self, worker_addr):
		self.__store_worker_addr(worker_addr)
		self.__send_pong(worker_addr)

	def forward_job(self, client, job):
		worker = self.__get_ready_worker()
		token = self.__generate_job_id()
		self.__store_pending_job(token, client)
		self.__send_job(job, token, worker)

	def forward_result(self, token, result):
		client = self.__get_job_client(token)
		self.__remove_pending_job(token)
		self.__send_result(client, result)

	def forward_error(self, token, error):
		client = self.__get_job_client(token)
		self.__remove_pending_job(token)
		self.__send_result(client, error)		

	def deny_request(self, client):
		self.frontend.send_multipart([client, ujson.dumps({'msg':MsgCode.QUEUE_FULL})])

	def __connect_frontend(self):
		self.frontend, s = self.stream('frontend', zmq.ROUTER, True, self.EVENT_frontend_recv)

	def __connect_backend(self):
		self.backend, s = self.stream('backend', zmq.ROUTER, True, self.EVENT_backend_recv)

	def __send_pong(self, worker_addr):
		self.backend.send_multipart([worker_addr, ujson.dumps({'status': MsgCode.PONG})])

	def __send_job(self, job, token, worker_addr):
		self.backend.send_multipart([worker_addr, ujson.dumps({'status': MsgCode.WORK, 'token': token, 'job': job})])

	def __send_result(self, client, result):
		self.frontend.send_multipart([client, ujson.dumps({'msg':result})])

	def __store_worker_addr(self, addr):
		if self.workers.has_key(addr):
			self.workers[addr].stop()
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

	def __generate_job_id(self):
		return uuid4().hex


class AuctionBroker(MarketBroker):
	def setup(self):
		self.auctions = {}
		self.__connect_frontend()
		self.__connect_backend()

	def EVENT_frontend_recv(self, msg):
		client, payload = msg[0], ujson.loads(msg[1])
		assert payload.has_key('auction')
		assert payload.has_key('order')
		if self.auctions.has_key(payload['auction']):
			auction = self.auctions[payload['auction']]
			self.forward_job(auction, payload['order'])
		else:
			self.deny_request(client)

	def EVENT_backend_recv(self, msg):
		worker, payload = msg[0], ujson.loads(msg[1])
	
		status = payload.get('status')
		if status == MsgCode.STATUS_READY:
			pass
		elif status == MsgCode.SUCCESS:
			self.forward_result(payload['token'], payload['result'])
		elif status == MsgCode.ERROR:
			self.forward_error(payload['token'], payload['msg'])
		elif status == MsgCode.PING:
			pass
		else:
			raise Exception('Worker status must be either READY, SUCCESS, ERROR or PING')
		self.add_worker(worker)
	
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
	TraderClient(name = 'trader', endpoints = {'backend' : 'market_frontend'}).start()
	MarketBroker(name = 'server', endpoints = {'backend' : 'market_backend', 'frontend': 'market_frontend'}).start()
	PPWorker(name = 'worker', endpoints = {'frontend' : 'market_backend'}).start()
	# Server(name = 'server', endpoints = {'backend' : 'market_backend'}).start()
	# Client(name = 'client', endpoints = {'frontend' : 'market_backend'}).start()

