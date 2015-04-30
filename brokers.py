from lib import *
from workers import REQWorkerThread
from collections import namedtuple

Job = namedtuple('Job', 'client work')
from Queue import PriorityQueue



class BrokerWithQueueing(AgentProcess):


	def run(self):
		frontend = self.context.socket(zmq.ROUTER)
		frontend.bind(AddressManager.get_bind_address(self.frontend_name))
		frontend.identity = AddressManager.get_bind_address(self.frontend_name)
		backend = self.context.socket(zmq.ROUTER)
		backend.bind(AddressManager.get_bind_address(self.backend_name))

		poller = zmq.Poller()
		poller.register(backend, zmq.POLLIN)
		poller.register(frontend, zmq.POLLIN)
		
		workers = DQueue()
		jobs = DQueue()
		# in_progress = {}
		# worker_last_active = {}
		# worker_hearbeats = PriorityQueue()

		while True:
			
			sockets = dict(poller.poll(1000))	

			

			if frontend in sockets:
				package = Package.recv(frontend)
				self.say('On frontend: {}'.format(package))
				jobs.put(Job(client=package.sender_addr, work=package.msg))
			
			if backend in sockets:
				package = Package.recv(backend)
				self.say('On backend: {}'.format(package))
				worker = package.sender_addr
				if package.msg == MsgCode.JOB_COMPLETE:
					if package.encapsulated:
						### Forward result from worker to client
						package.encapsulated.send(frontend)
					# workers.put(worker)
				if package.msg != MsgCode.DISCONNECT:
					
					if not worker in workers: workers.put(worker)
					# Send PONG to worker
					Package(dest_addr = worker, msg = MsgCode.PONG).send(backend)


				# elif 

				# elif package.msg == MsgCode.STATUS_READY:					
				# 	workers.put(worker)
				# elif package.msg == MsgCode.PING:
				# 	pass






			if not jobs.empty() and not workers.empty():
				worker_addr = workers.get()
				job = jobs.get()
				client_p = Package(dest_addr = job.client)
				package = Package(dest_addr = worker_addr, msg = job.work, encapsulated = client_p)
				self.say('sending on backend: {}'.format(package))
				package.send(backend)


			if jobs.qsize() > 10:
				pass
			if sockets: self.say('pending jobs: {}, workers in pool: {}'.format(jobs.qsize(), workers.qsize()))
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

