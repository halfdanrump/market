from time import sleep
from lib import *

HOSPITAL_FEES = 1000000

class Animal(Agent):
	
	def __init__(self, preference = 0.5):
		assert isinstance(preference, float) and preference > 0 and preference < 1
		self.preference = preference

	def animal_style(self):
		pass



class Human(Animal):
	def breed(self, partner):


class Baby(Human):
	def __new__(self, mother, father, savings):
		assert isinstance(mother, Human)
		assert isinstance(father, Human)
		assert isinstance(savings, int)
		assert saving > HOSPITAL_FEES, "Go to a country with social welfare"


	def __init__(self, hospital_fees):
		pass
	def __del__(self):
		"""immortal"""
		pass