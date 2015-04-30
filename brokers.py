from lib import *
from workers import REQWorkerThread
from collections import namedtuple

Job = namedtuple('Job', 'client work')
# Worker = namedtuple('Worker', 'worker_addr expires')
from Queue import PriorityQueue
from datetime import datetime, timedelta
from heapdict import heapdict

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



	def run(self):
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

		while True:
			
			sockets = dict(poller.poll(100))	

			# self.simulate_crash(0.01)

			if frontend in sockets:
				package = Package.recv(frontend)
				self.say('On frontend: {}'.format(package))
				self.jobs.put(Job(client=package.sender_addr, work=package.msg))
			
			if backend in sockets:
				package = Package.recv(backend)
				self.say('On backend: {}'.format(package))
				worker = package.sender_addr
				if package.msg == MsgCode.JOB_COMPLETE:
					if package.encapsulated:
						### Forward result from worker to client
						package.encapsulated.send(frontend)
				
				if package.msg == MsgCode.DISCONNECT:
					self.remove_worker(worker)
				else:
					self.add_worker(worker)
					# Send PONG to worker
					Package(dest_addr = worker, msg = MsgCode.PONG).send(backend)


			if not self.jobs.empty() and not self.workers.empty():
				worker_addr = self.workers.get()
				job = self.jobs.get()
				client_p = Package(dest_addr = job.client)
				package = Package(dest_addr = worker_addr, msg = job.work, encapsulated = client_p)
				self.say('sending on backend: {}'.format(package))
				package.send(backend)


			if self.jobs.qsize() > 10:
				pass

			self.expire_workers()
			
			if sockets: 
				self.say('pending self.jobs: {}, self.workers in pool: {}'.format(self.jobs.qsize(), self.workers.__len__()))


		backend.close()
		frontend.close()

# class MJDBroker(BrokerWithQueueing):

# 	a



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

