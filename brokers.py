from lib import *
from workers import REQWorkerThread
from collections import namedtuple

Job = namedtuple('Job', 'client work')

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
		
		workers = Queue.Queue()
		jobs = Queue.Queue()
		in_progress = {}

		while True:
			
			sockets = dict(poller.poll(1000))	

			# if sockets: self.say('pending jobs: {}, workers in pool: {}'.format(jobs.qsize(), workers.qsize()))

			if frontend in sockets:
				package = Package.recv(frontend)
				self.say('On frontend: {}'.format(package))
				jobs.put(Job(client=package.sender_addr, work=package.msg))
			
			if backend in sockets:
				package = Package.recv(backend)
				self.say('On backend: {}'.format(package))

				if package.msg == MsgCode.JOB_COMPLETE:
					if package.encapsulated:
						package.encapsulated.send(frontend)
					workers.put(package.sender_addr)
				elif package.msg == STATUS_READY:					
					workers.put(package.sender_addr)

				

				# if p.status in [STATUS_READY, JOB_COMPLETE]:
				# 	workers.put(reply.worker_addr)
				# if reply.client_addr:
				# 	reply_to_client = ClientMsg(reply.client_msg, reply.client_addr)
				# 	self.say('Sending to frontend: {}'.format([reply.client_addr, "", reply.client_msg]))
				# 	frontend.send_multipart([reply.client_addr, "", reply.client_msg])
				
			if not jobs.empty() and not workers.empty():
				worker_addr = workers.get()
				job = jobs.get()
				# in_progress[worker_addr] = job
				client_p = Package(dest_addr = job.client)
				package = Package(dest_addr = worker_addr, msg = job.work, encapsulated = client_p)
				self.say('sending on backend: {}'.format(package))
				package.send(backend)
				
				# backend.send_multipart(package.__aslist__())
				# backend.send_multipart([worker_addr, "", client_addr, "", workload])

			if jobs.qsize() > 10:
				pass

		backend.close()
		frontend.close()



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

