import abc

class Animal(object):
	__metaclass__ = abc.ABCMeta

	@abc.abstractmethod	
	def speak(self):
		print('Animal speaking')
		return

class Cat(Animal):

	def speak(self):
		super(Cat, self).speak()
		print('Miaow')