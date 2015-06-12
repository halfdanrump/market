from __future__ import print_function
from multiprocessing import Process, current_process
import atexit

import zmq
from lib import AgentProcess
context = zmq.Context()
# When trader exits it sends signal to test process. Test process counts signals and then 


# Test wrapper for class. when wrapped, create socket for signalling terminationg at also register atexit

# def timed_loop(job_func):

# 	start_date = datetime.now()

# 	while 

# 	end_time = datetime.now()

# def profile(agent):
# 	assert hasattr(agent, 'loop')
# 	def wrapper(*args, **kwargs):
# 		pass

def wrap_incorrect(agent):
	assert hasattr(agent, 'loop')

	def before_loop():
		print('Before loop')

	def after_loop():
		print('Doing stuff after loop')


	def wrapped():
		before_loop()
		print(agent.loop)
		agent.loop()
		after_loop()

	print('Before replacing: ', agent.loop)
	agent.loop = wrapped
	print(agent.loop)

def wrap(agent):
	assert hasattr(agent, 'loop')

	def before_loop():
		print('Before loop')

	def after_loop():
		print('Doing stuff after loop')

	print(agent.loop)
	lfunc = agent.loop
	print(lfunc)

	def wrapped():
		print(lfunc)
		before_loop()
		lfunc()
		after_loop()
	
	agent.loop = wrapped
	print(agent.loop)


def decorate():
	pass

class TestAgent:
	def loop(self):
		for i in range(5):
			print('In loop')



import sys
from time import sleep



import os


class multi_push_to_one_pull(AgentProcess):
	__address__ = 'ipc://mpp'

	def pull(self):
		# atexit.register(send_signal)
		socket = context.socket(zmq.PULL)
		socket.bind(self.__address__)
		for n in range(10000):
			r = socket.recv()
			if n%1000==0: print(n,r)
		print('Pull is done')

	def push(self, msg):
		socket = context.socket(zmq.PUSH)
		socket.connect(self.__address__)
		while n in range(1000):
			self.say(socket.send(msg))

	def run(self):
		Process(target = self.pull).start()
		Process(target = self.push, args = ('push',)).start()


class Worker(Process):

	# @staticmethod
	def exit_handler(self):
		print('In worker exit handler')

	def run(self):
		print('Done: {}'.format(sum(range(10000000))))	
