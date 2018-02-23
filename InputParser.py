import sys
import json

class parseInput:
	def __init__(self,fileloc):
		with open(fileloc) as runSet:
			self.params=json.load(runSet)

	def numRuns(self):
		if 'numRuns' in self.params['wrap'].keys():
			return self.params['wrap']['numRuns']
		return 1

	def specified_settings(self):
		return self.params['wrap']['settings']

	def printer(self):
		return self.params['wrap']['printer']

	def workloads(self):
		return self.params['wrap']['workloads']

	def rollBack(self):
		return (self.params['wrap']['enableRollBack'].lower()=='true')

	def base_version(self):
		return self.params['wrap']['base_version']

	def rollBack_service(self):
		return self.params['wrap']['rollBackService']

	def clusterInfo(self):
		return [self.params['wrap']['cluster']['host'],self.params['wrap']['cluster']['clustername'],self.params['wrap']['cluster']['user'],self.params['wrap']['cluster']['password']]

	def whetherZeppelin(self):
		return (self.params['wrap']['zeppelin'].lower()=='true')

	def noteInfo(self):
		return [self.params['wrap']['notebook']['host'],self.params['wrap']['notebook']['user'],self.params['wrap']['notebook']['password'],self.params['wrap']['notebook']['note'],self.params['wrap']['notebook']['zepInputFile']]



