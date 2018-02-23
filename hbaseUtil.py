import sys
import subprocess
import re
from collections import defaultdict

class hbaseUtil:

	def __init__(self):
		self.hivetrials=defaultdict(lambda:{})
		self.viaAmbari=defaultdict(lambda:defaultdict(lambda:{}))
		self.restarts=defaultdict(lambda:defaultdict(lambda:[]))
		self.sysMod={}

	def addSettings(self,name,setting):
		self.hivetrials[name]=setting

	def addAmbariConf(self,name,setting):
		for key in setting.keys():
			self.viaAmbari[name][key]=setting[key]

	def addRestart(self,name,setting):
		for key in setting.keys():
			self.restarts[name][key]=setting[key]

	def addSysMod(self,name,setting):
		self.sysMod[name]=setting


	def HbaseCommand(self,setting,workload,runType):
		if runType.lower()=="load":
			return "./bin/ycsb load hbase10 -P ./workloads/"+workload+" -p columnfamily=cf -p hbase.zookeeper.znode.parent=/hbase-unsecure -p recordcount="+self.hivetrials[setting]["records"]+" -threads "+self.hivetrials[setting]["loadthreads"]
		elif runType.lower()=="run":
			return "./bin/ycsb run hbase10 -P ./workloads/"+workload+" -p columnfamily=cf -p hbase.zookeeper.znode.parent=/hbase-unsecure -p recordcount="+self.hivetrials[setting]["records"]+" -p operationcount="+self.hivetrials[setting]['operations']+" -threads "+self.hivetrials[setting]["runthreads"]

			
