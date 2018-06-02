import sys
import subprocess
import re
from collections import defaultdict

class hbaseUtil:

	def __init__(self):
		self.hbasetrials=defaultdict(lambda:{})
		self.viaAmbari=defaultdict(lambda:defaultdict(lambda:{}))
		self.restarts=defaultdict(lambda:defaultdict(lambda:[]))
		self.sysMod={}

	def addSettings(self,name,setting):
		self.hbasetrials[name]=setting

	def addAmbariConf(self,name,setting):
		for key in setting.keys():
			self.viaAmbari[name][key]=setting[key]

	def addRestart(self,name,setting):
		for key in setting.keys():
			self.restarts[name][key]=setting[key]

	def addSysMod(self,name,setting):
		self.sysMod[name]=setting

	def HbaseLoadCommand(self,setting,workload,binding):
		return "./bin/ycsb load "+binding+" -P ./workloads/"+workload+" -p columnfamily=cf -p hbase.zookeeper.znode.parent=/hbase-unsecure -p recordcount="+self.hbasetrials[setting]["records"]+" -threads "+self.hbasetrials[setting]["loadthreads"]
		

	def HBaseRunCommand(self,setting,workload,binding):
		return "./bin/ycsb run "+binding+" -P ./workloads/"+workload+" -p columnfamily=cf -p hbase.zookeeper.znode.parent=/hbase-unsecure -p recordcount="+self.hbasetrials[setting]["records"]+" -p operationcount="+self.hbasetrials[setting]['operations']+" -threads "+self.hbasetrials[setting]["runthreads"]

