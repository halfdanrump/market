from lib import *
from workers import REQWorkerThread


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

		while True:
			
			sockets = dict(poller.poll(1000))	

			# if sockets: self.say('pending jobs: {}, workers in pool: {}'.format(jobs.qsize(), workers.qsize()))

			if backend in sockets:

				request = backend.recv_multipart()
				# self.say('from worker: {}'.format(request))
				reply = WorkerMsg.from_mulipart(request, backend.TYPE)
	
				if reply.status in [STATUS_READY, JOB_COMPLETE]:
					workers.put(reply.worker_addr)
				if reply.client_addr:
					reply_to_client = ClientMsg(reply.client_msg, reply.client_addr)
					
					frontend.send_multipart([reply.client_addr, "", reply.client_msg])
				
			if frontend in sockets:
				request = frontend.recv_multipart()
				# print('REQUEST')
				# print(request)
				# r = ClientMsg.from_multipart(request, frontend.TYPE)
				# self.say(r.message)
				# self.say(r.client_addr)
				# self.say(r.client_msg)
				# client_addr, workload = r.client_addr, r.client_msg
				# self.say(request)
				client_addr, workload = request[0], request[2]

				jobs.put((client_addr, workload))
			
			if not jobs.empty() and not workers.empty():
				worker_addr = workers.get()
				client_addr, workload = jobs.get()
				backend.send_multipart([worker_addr, "", client_addr, "", workload])

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
