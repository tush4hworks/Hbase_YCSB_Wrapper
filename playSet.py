import sys
import subprocess
import re
from collections import defaultdict
import itertools
import json
import threading
import hbaseUtil
import logging
import modifyConfig
import analyzeResults
import InputParser
import notes
import datetime
import time
import computeStats


class controls:
	
	def __init__(self,jsonFile):
		"""Init Function for class controls"""
		FORMAT = '%(asctime)-s-%(levelname)s-%(message)s'
		logging.basicConfig(format=FORMAT,filename='HBasetests.log',filemode='w',level='INFO')
		logging.getLogger("requests").setLevel(logging.WARNING)
		self.logger=logging.getLogger(__name__)
		self.fetchParams(jsonFile)

	def getDateTime(self,epochT=False):
		if epochT:
			return str(int(time.time()))
		return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


	def toZeppelinAndTrigger(self):
		try:
			subprocess.check_output('hadoop fs -rm /tmp/'+self.zepInputFile,stderr=subprocess.STDOUT,shell=True)
		except Exception as e:
			self.logger.info(e.__str__())
		try:
			with open(self.zepInputFile,'w+') as f:
				for db in self.results.keys():
					for ql in self.results[db].keys():
						for setting in self.results[db][ql].keys():
							for i in range(len(self.results[db][ql][setting])):
								f.write(','.join([ql,setting,str(i+1),str(self.results[db][ql][setting][i])
									])+'\n')
		except Exception as e:
			self.logger.info(e.__str__())
		try:			
			subprocess.check_output('hadoop fs -put '+self.zepInputFile+' /tmp',stderr=subprocess.STDOUT,shell=True)
		except Exception as e:
			self.logger.info(e.__str__())
		try:
			self.zepObj.zepLogin()
			self.zepObj.runParagraphs(self.zeppelinNote)
		except Exception as e:
			self.logger.info(e.__str__())
	

	def runCmd(self,cmd,setting,workload,runType,run):
		"""Wrapper to run shell"""
		try:
			self.logger.info('+ Executing command '+cmd)
			result=subprocess.check_output(cmd+'>'+'History/'+'_'.join([setting,workload,runType,run,self.getDateTime()],stderr=subprocess.STDOUT,shell=True)
			self.logger.info('- Finished executing command '+cmd)
		except Exception as e:
			self.logger.error('- Finished executing command with exception '+cmd)
			if hasattr(e,'output'):
				with open('History/'+'_'.join([setting,workload,runType,run,self.getDateTime()]),'w+') as f:
					f.write(e.output)

	def sysConf(self,cmds,setting):
		for cmd in cmds:
			try:
				self.logger.info('+ Running '+cmd+' for setting '+setting)
				subprocess.check_output(cmd,stderr=subprocess.STDOUT,shell=True)
				self.logger.info('- Finished executing command '+cmd)
			except Exception as e:
				self.logger.error('- Finished executing command with exception '+cmd)
		
	def modifySettingsAndRestart(self,ambariSetting,services,components,force_restart=False):
		"""Calling ambari API to change configuration and restart services/components"""
		reset=False
		for key in ambariSetting.keys():
			if self.modconf.putConfig(key,ambariSetting[key]):
				reset=True
		if reset or force_restart:
			self.logger.warn('+ Config changed. Going to restart services/components if any! +')
			for service in services:
				self.logger.info('+ Restarting '+service+' +')
				self.modconf.restartService(service)
				self.logger.info('- Restarted '+service+' -')
			for component in components:
				self.logger.info('+ Restarting '+component+' +')
				self.modconf.restartComponent(component)
				self.logger.info('- Restarted '+component+' -')

	def updateNote(self):
		try:
			t=threading.Thread(target=self.toZeppelinAndTrigger,args=())
			t.start()
		except Exception as e:
			self.logger.info(e.__str__())

	def runTests(self,settings,workloads,numRuns,runZep=False):
		"""Main entry function to run TPCDS suite"""
		currSet=None
		for setting,workload in list(itertools.product(settings,workloads)):
			try:
				updateZeppelin=False
				self.logger.info('+ BEGIN EXECUTION '+' '.join([workload,setting])+' +')
				if not(currSet) or not(setting==currSet):
					force_restart=False
					updateZeppelin=runZep
					if setting in self.hbase.viaAmbari.keys():
						if self.rollBack:
							self.logger.warn('+ Rolling back to base version before making changes for setting '+currSet+ '+')
							self.modconf.rollBackConfig(self.rollBack_service,self.base_version) 
							self.logger.info('- Rolled back to base version before making changes for setting '+currSet+ '-')
							force_restart=True
						self.logger.info('+ Comparing with existing configurations via ambari for '+setting+' +')
						self.modifySettingsAndRestart(self.hbase.viaAmbari[setting],self.hbase.restarts[setting]['services'],self.hbase.restarts[setting]['components'],force_restart)
					if setting in self.hbase.sysMod.keys():
						self.sysConf(self.hbase.sysMod[setting],setting)
					self.logger.info('Starting execution with below configurations for '+setting)
					for toPrint in self.printer:
						self.logger.info(json.dumps(self.modconf.getConfig(toPrint),indent=4,sort_keys=True))
					currSet=setting
				HbaseLoadCmd=self.hbase.HbaseCommand(setting,workload,'load')
				HbaseRunCmd=self.hbase.HbaseCommand(setting,workload,'run')
				self.runCmd(HbaseLoadCmd,setting,workload,'load','0')
				for i in xrange(numRuns):
					self.runCmd(HbaseRunCmd,setting,workload,'load',str(i))
				self.logger.info('+Dropping/Recreating Table For Next Run+')
				self.runCmd('hbase shell ./hbase_truncate')
				self.logger.info('-Dropped/Recreated Table For Next Run-')
				self.logger.info('- FINISHED EXECUTION '+' '.join([workload,setting])+' -')
				if updateZeppelin:
					self.updateNote()
			except Exception as e:
				self.logger.error(e.__str__())
				self.logger.warn('- FINISHED EXECUTION WITH EXCEPTION'+' '.join([workload,setting])+' -')


	def addHbaseSettings(self,name,runSettings):
		"""Segregate settings and add"""
		if 'runconf' in runSettings.keys():
			self.hbase.addSettings(name,runSettings['runconf'])
		if 'ambari' in runSettings.keys():
			self.hbase.addAmbariConf(name,runSettings['ambari'])
		if 'restart' in runSettings.keys():
			self.hbase.addRestart(name,runSettings['restart'])
		if 'system' in runSettings.keys():
			self.hbase.addSysMod(name,runSettings['system'])
		self.hbaseconfs.append(name)

	def fetchParams(self,fileloc):
		"""Parse input json"""
		iparse=InputParser.parseInput(fileloc)
		host,clustername,user,password=iparse.clusterInfo()
		self.modconf=modifyConfig.ambariConfig(host,clustername,user,password)
		self.hbaseconfs=[]
		self.hbase=hbaseUtil.hbaseUtil()
		self.numRuns=iparse.numRuns()
		self.printer=iparse.printer()
		self.rollBack=iparse.rollBack()
		self.workloads=iparse.workloads()
		if self.rollBack:
			self.base_version=iparse.base_version()
			self.rollBack_service=iparse.rollBack_service()
		for setting in iparse.specified_settings():
			self.addHbaseSettings(setting['name'],setting['config'])
		self.runZep=False
		if iparse.whetherZeppelin():
			self.runZep=True
			host,user,password,note,zepInputFile=iparse.noteInfo()
			self.zeppelinNote=note
			self.zepInputFile=zepInputFile
			self.zepObj=notes.zepInt(host,user,password)

if __name__=='__main__'
	C=controls('params.json')
	C.runTests(C.hbaseconfs,C.workloads,C.numRuns,C.runZep)
	