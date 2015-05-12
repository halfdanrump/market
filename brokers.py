from lib import *
from collections import namedtuple

Job = namedtuple('Job', 'client work')
# Worker = namedtuple('Worker', 'worker_addr expires')
from datetime import datetime, timedelta
from heapdict import heapdict

class MJDBroker(AgentProcess):
	
	WORKER_EXPIRE_SECONDS = 3

	def expire_workers(self):
		if len(self.workers) > 0:
			now = datetime.now()
			while len(self.workers) > 0 and self.workers.peekitem()[1] < now:
				self.say('Expiring worker')
				self.workers.popitem()

	def add_worker(self, worker_addr):
		expires = datetime.now() + timedelta(seconds = self.WORKER_EXPIRE_SECONDS)
		self.say('Adding worker {}. Expires at {}'.format(worker_addr, expires))
		self.workers[worker_addr] = expires

	def remove_worker(self, worker_addr):
		"""
		This doesn't remove the worker from the priority queue, but it doesn't matter since 
		the worker will eventually be removed when it was supposed to expire anyway
		"""
		self.say('Removing worker: {}'.format(worker_addr))
		if self.workers.has_key(worker_addr):
			del self.workers[worker_addr]	

class BrokerWithQueueing(AgentProcess):

	WORKER_EXPIRE_SECONDS = 3

	def expire_workers(self):
		if len(self.workers) > 0:
			now = datetime.now()
			while len(self.workers) > 0 and self.workers.peekitem()[1] < now:
				self.say('Expiring worker')
				self.workers.popitem()

	def add_worker(self, worker_addr):
		expires = datetime.now() + timedelta(seconds = self.WORKER_EXPIRE_SECONDS)
		# self.say('Adding worker {}. Expires at {}'.format(worker_addr, expires))
		self.workers[worker_addr] = expires

	def remove_worker(self, worker_addr):
		"""
		This doesn't remove the worker from the priority queue, but it doesn't matter since 
		the worker will eventually be removed when it was supposed to expire anyway
		"""
		self.say('Removing worker: {}'.format(worker_addr))
		if self.workers.has_key(worker_addr):
			del self.workers[worker_addr]



	def run(self):
		start_time = datetime.now()

		frontend = self.context.socket(zmq.ROUTER)
		frontend.bind(AddressManager.get_bind_address(self.frontend_name))
		frontend.identity = AddressManager.get_bind_address(self.frontend_name)
		backend = self.context.socket(zmq.ROUTER)
		backend.bind(AddressManager.get_bind_address(self.backend_name))

		poller = zmq.Poller()
		poller.register(backend, zmq.POLLIN)
		poller.register(frontend, zmq.POLLIN)
		
		self.workers = heapdict()
		self.jobs = DQueue(item_type = Job)
		n_jobs_received = 0
		while n_jobs_received <= 1000000:
			
			sockets = dict(poller.poll(10))	

			if frontend in sockets:
				n_jobs_received += 1
	
			
			if backend in sockets:
				package = Package.recv(backend)
				self.say('On backend: {}'.format(package))
				worker = package.sender_addr
				if package.msg == MsgCode.JOB_COMPLETE:
					if package.encapsulated:
						### Forward result from worker to client
						self.say('Sending on frontend: {}'.format(package.encapsulated))
						package.encapsulated.send(frontend)
				
				if package.msg == MsgCode.DISCONNECT:
					self.remove_worker(worker)
				else:
					self.add_worker(worker)
					# Send PONG to worker
					Package(dest_addr = worker, msg = MsgCode.PONG).send(backend)
			
			if not self.jobs.empty() and len(self.workers) > 0:
				
				worker_addr = self.workers.popitem()[0]
				job = self.jobs.get()
				client_p = Package(dest_addr = job.client)
				package = Package(dest_addr = worker_addr, msg = job.work, encapsulated = client_p)
				self.say('sending on backend: {}'.format(package))
				package.send(backend)


			if self.jobs.qsize() > 10:
				pass



			self.expire_workers()
			
			# if sockets: 
			# 	self.say('pending jobs: {}, workers in pool: {}'.format(self.jobs.qsize(), self.workers.__len__()))
		end_time = datetime.now()
		td = end_time - start_time
		print('Time for 1000000 orders; {}'.format(td.total_seconds()))

		backend.close()
		frontend.close()

# class MJDBroker(BrokerWithQueueing):

# 	a

# class OrderRouter(AgentProcess):
# 	self.



class PingPongBroker(AgentProcess):
	WORKER_EXPIRE_SECONDS = 3

	def setup(self):
		self.poller = zmq.Poller()
		self.new_socket(self.frontend_name, 'frontend', zmq.ROUTER, bind = True, handler = self.handle_frontend)
		self.new_socket(self.backend_name, 'backend', zmq.ROUTER, bind = True, handler = self.handle_backend)
		
		
		# for socket_name, socket in self.sockets.items():
		# 	self.poller.register(socket, zmq.POLLIN)
		
		self.workers = heapdict()
		self.jobs = DQueue(item_type = Job)

		self.jobs_received = 0

	def expire_workers(self):
		if len(self.workers) > 0:
			now = datetime.now()
			while len(self.workers) > 0 and self.workers.peekitem()[1] < now:
				self.say('Expiring worker')
				self.workers.popitem()

	def add_worker(self, worker_addr):
		expires = datetime.now() + timedelta(seconds = self.WORKER_EXPIRE_SECONDS)
		self.workers[worker_addr] = expires

	def remove_worker(self, worker_addr):
		"""
		This doesn't remove the worker from the priority queue, but it doesn't matter since 
		the worker will eventually be removed when it was supposed to expire anyway
		"""
		self.say('Removing worker: {}'.format(worker_addr))
		if self.workers.has_key(worker_addr):
			del self.workers[worker_addr]

	
	def send_job(self):
		worker_addr = self.workers.popitem()[0]
		job = self.jobs.get()
		client_p = Package(dest_addr = job.client)
		package = Package(dest_addr = worker_addr, msg = job.work, encapsulated = client_p)
		self.say('sending on backend: {}'.format(package))
		package.send(self.backend)


	def handle_frontend(self):
		package = Package.recv(self.frontend)
		self.say('On frontend: {}'.format(package))
		job = Job(client=package.sender_addr, work=package.msg)
		self.jobs_received += 1
		self.jobs.put(job)

	def handle_backend(self):
		package = Package.recv(self.backend)
		self.say('On backend: {}'.format(package))
		worker = package.sender_addr
		if package.msg == MsgCode.DISCONNECT:
			self.remove_worker(worker)
		else:
			self.add_worker(worker)
			# Send PONG to worker
			Package(dest_addr = worker, msg = MsgCode.PONG).send(self.backend)
		
		if package.msg == MsgCode.JOB_COMPLETE:
			if package.encapsulated:
				### Forward result from worker to client
				self.say('Sending on frontend: {}'.format(package.encapsulated))
				package.encapsulated.send(self.frontend)
	
	def iteration(self):
		self.poll_sockets()
		if not self.jobs.empty() and len(self.workers) > 0:
			self.send_job()
		self.expire_workers()


class AuctionPool(AgentProcess):

	def register_auction(self):
		pass

	def run(self):
		self.frontend = sel


class BrokerWithPool(BrokerWithQueueing):
	def __init__(self, name, frontend_name, backend_name, n_workers_start = 2, n_workers_max = 10):
		super(BrokerWithPool, self).__init__(name, frontend_name, backend_name)
		self.alive_event = Event()
		self.alive_event.set()
		self.n_workers_start = n_workers_start
		self.n_workers_max = n_workers_max
		self.started_workers = 0

	def start_worker(self):
		self.say('Starting worker...')
		REQWorkerThread(self.context, '{}_{}'.format(self.name, 'worker'), self.alive_event).start()	
		self.started_workers += 1

	def run(self):
		for j in xrange(self.n_workers_start): 
			self.start_worker()
		try:
			super(BrokerWithPool, self).run()
		except KeyboardInterrupt:
				self.alive_event.clear()	

