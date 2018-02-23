import sys
import requests
import json
from requests.auth import HTTPBasicAuth
import time


class ambariConfig:

	def __init__(self,host,cluster,user,password,isSecure=False,admin_princ=None,admin_pass=None):
		self.headers={'X-Requested-By':'ambari'}
		self.host=host
		self.cluster=cluster
		self.prefix='http://'+host+':8080/api/v1/clusters/'+cluster
		self.auth=HTTPBasicAuth(user,password)

	def commonGet(self,url):
		resp=requests.get(url,headers=self.headers,auth=self.auth)
		if resp.status_code==200:
			return resp.content
		else:
			return None

	def compareConf(self,existing,desired):
		toChange=False
		for key in desired.keys():
			if ((not key in existing.keys()) or not(desired[key]==existing[key])):
				toChange=True
				return toChange
		return toChange

	def commonPut(self,url,data):
		resp=requests.put(url,headers=self.headers,auth=self.auth,data=data)
		return resp.content

	def printConfig(self,config):
		confs=self.commonGet(self.prefix+'?fields=Clusters/desired_configs')
		if confs:
			try:
				tag=json.loads(confs)['Clusters']['desired_configs'][config]['tag']
				service_conf=self.commonGet(self.prefix+'/configurations?type='+config+'&tag='+tag)
				if service_conf: 
					print service_conf
			except KeyError:
				print 'Desired Config does not exist!. Please check config name.'
			
	def getConfig(self,config):
		confs=self.commonGet(self.prefix+'?fields=Clusters/desired_configs')
		if confs:
			try:
				tag=json.loads(confs)['Clusters']['desired_configs'][config]['tag']
				service_conf=self.commonGet(self.prefix+'/configurations?type='+config+'&tag='+tag)
				if service_conf: 
					return json.loads(service_conf)['items'][0]['properties']
			except KeyError:
				print 'Desired Config does not exist!. Please check config name.'

	def rollBackConfig(self,service,version):
		roll_payload={
		  'Clusters': {
		    'desired_service_config_versions': {
		      'service_name' :service,
		      'service_config_version' : version,
		      'service_config_version_note' : 'Manual rollback to base configuration'
		    }	
		  }
		}
		self.commonPut(self.prefix,json.dumps(roll_payload))


	def stopService(self,service):
		stop_payload={'RequestInfo': {'context': 'Stop '+service}, 'ServiceInfo': {'state': 'INSTALLED'}}
		req_href=json.loads(self.commonPut(self.prefix+'/services/'+service,json.dumps(stop_payload)))['href']
		while not (json.loads(self.commonGet(req_href))['Requests']['request_status']=='COMPLETED'):
			time.sleep(5)

	def startService(self,service):
		start_payload={'RequestInfo': {'context': 'Start '+service}, 'ServiceInfo': {'state': 'STARTED'}}
		req_href=json.loads(self.commonPut(self.prefix+'/services/'+service,json.dumps(start_payload)))['href']
		while not (json.loads(self.commonGet(req_href))['Requests']['request_status']=='COMPLETED'):
			time.sleep(5)
		#Giving Buffer
		time.sleep(60)

	def stopComponent(self,comp):
		stop_payload={'RequestInfo': {'context': 'Stop '+comp}, 'ServiceComponentInfo': {'state': 'INSTALLED'}}
		req_href=json.loads(self.commonPut(self.prefix+'/services/'+comp,json.dumps(stop_payload)))['href']
		while not (json.loads(self.commonGet(req_href))['Requests']['request_status']=='COMPLETED'):
			time.sleep(5)

	def startComponent(self,comp):
		start_payload={'RequestInfo': {'context': 'Start '+comp}, 'ServiceComponentInfo': {'state': 'STARTED'}}
		req_href=json.loads(self.commonPut(self.prefix+'/services/'+comp,json.dumps(start_payload)))['href']
		while not (json.loads(self.commonGet(req_href))['Requests']['request_status']=='COMPLETED'):
			time.sleep(5)
		#Giving Buffer
		time.sleep(60)

	def restartComponent(self,comp):
		self.stopComponent(comp)
		self.startComponent(comp)

	def restartService(self,service):
		self.stopService(service)
		self.startService(service)		
		

	def putConfig(self,config,propDict):
		currConf=self.getConfig(config)
		if self.compareConf(currConf,propDict):
			for prop in propDict.keys():
				currConf[prop]=propDict[prop]
			payload={
			  'Clusters': {
			    'desired_config': {
			      'type': config,
			      'tag': 'version'+str(int(time.time())),
			      'properties': currConf
			    }
			  }
			}
			self.commonPut(self.prefix,json.dumps(payload
				))
			return True
		return False

if __name__=='__main__':
	'''
	s=ambariConfig('localhost','DPH','admin','admin')
	s.printConfig('hbase-site')
	s.printConfig('tez-interactive-site')
	s.putConfig('hive-interactive-site',{'hive.tez.container.size':'6144'})
	s.putConfig('tez-interactive-site',{'tez.runtime.io.sort.mb':'1200'})
	s.printConfig('hive-interactive-site')
	s.printConfig('tez-interactive-site')
	s.restartComponent('HIVE/components/HIVE_SERVER_INTERACTIVE')
	'''
	pass
	





