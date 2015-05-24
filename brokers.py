from lib import *
from collections import namedtuple
from datetime import datetime, timedelta
from heapdict import heapdict
Job = namedtuple('Job', 'client work')


class PingPongBroker(AgentProcess):
	WORKER_EXPIRE_SECONDS = 3

	__sockets__ = [
	Sock('frontend', zmq.ROUTER, bind = True, handler = 'handle_frontend'),
	Sock('backend', zmq.ROUTER, bind = True, handler = 'recv_from_worker')
	]

	def setup(self):
		self.poller = zmq.Poller()
		# self.new_socket(self.frontend_name, 'frontend', zmq.ROUTER, bind = True, handler = self.handle_frontend)
		# self.new_socket(self.backend_name, 'backend', zmq.ROUTER, bind = True, handler = self.recv_from_worker)
		self.workers = heapdict()
		
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

	def recv_from_worker(self):
		package = Package.recv(self.backend)
		self.say('On backend: {}'.format(package))
		worker = package.sender_addr
		if package.msg == MsgCode.DISCONNECT:
			self.remove_worker(worker)
		else:
			self.add_worker(worker)
			# Send PONG to worker
			Package(dest_addr = worker, msg = MsgCode.PONG).send(self.backend)
		self.handle_backend(package)
	
	@abc.abstractmethod
	def iteration(self):
		return

	@abc.abstractmethod
	def handle_frontend(self):
		return

	@abc.abstractmethod
	def handle_backend(self, package):
		return



class JobQueueBroker(PingPongBroker):


	
	def setup(self):
		super(JobQueueBroker, self).setup()
		self.jobs = DQueue(item_type = Job)
		self.jobs_received = 0

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

	def handle_backend(self, package):
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


# class ServiceQueueBroker(PingPongBroker):

# 	def setup(self):
# 		super(JobQueueBroker, self).setup()
# 		self.services = 

# 		self.jobs = DQueue(item_type = Job)
# 		self.jobs_received = 0	

