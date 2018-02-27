import sys
import requests
from collections import defaultdict
import json

class getQueryMetrics:
	def __init__(self,host,port):
		self.prefix='http://{}:{}/ws/v1/timeline/metrics'.format(host,port)
		self.host_stats=defaultdict(lambda:defaultdict(lambda:defaultdict(lambda:'NA')))
		self.service_stats=defaultdict(lambda:defaultdict(lambda:'NA'))
		self.ifheaderservice=False
		self.ifheaderhost=False

	def fetch_stats(self,query,metricType,metricNames,startTime,endTime,dumpfile,hostname=None,precision=None,appId=None):
		url=self.prefix+'?metricNames={}{}{}{}&startTime={}&endTime={}'.format(metricNames,'&appId='+appId if appId else '','&hostname='+hostname if hostname else '','&precision='+precision if precision else '',startTime,endTime)
		resp=requests.get(url,headers={'Accept':'application/json'})
		if resp.status_code==200:
			self.addToMetrics(query,metricType,json.loads(resp.content)['metrics'])
		self.dumptofile(query,metricType,dumpfile)

	def addToMetrics(self,query,metricType,metricList):
		if metricType=='host':
			for stat in metricList:
				if len(stat['metrics'].keys())>0:
					self.host_stats[query][stat['hostname']][stat['metricname']]=str(int(sum(stat['metrics'].values())/len(stat['metrics'].keys())))
		elif metricType=='service':
			for stat in metricList:
				if len(stat['metrics'].keys())>0:
					self.service_stats[query][stat['metricname']]=str(int(sum(stat['metrics'].values())/len(stat['metrics'].keys())))

	def dumptofile(self,query,metricType,dumpfile):
		if metricType=='host':
			self.dumpHostStats(query,dumpfile)
		elif metricType=='service':
			self.dumpServiceStats(query,dumpfile)
	
	def dumpServiceStats(self,query,dumpfile):
		if len(self.service_stats[query].keys())>0:
			with open(dumpfile,'a+') as f:
				if not self.ifheaderservice:
					f.write('query,'+','.join(sorted(self.service_stats[query].keys()))+'\n')
					self.ifheader=True
				f.write(','.join([query]+[str(self.service_stats[query][key]) for key in sorted(self.service_stats[query].keys())])+'\n')

	def dumpHostStats(self,query,dumpfile):
		if len(self.host_stats[query].keys())>0:
			with open(dumpfile,'a+') as f:
				if not self.ifheaderhost:
					f.write('query,'+','.join(['-'.join([host,metric]) for host in sorted(self.host_stats[query].keys()) for metric in sorted(self.host_stats[query][host].keys())])+'\n')
					self.ifheaderhost=True
				f.write(','.join([query]+[str(self.host_stats[query][host][metric]) for host in sorted(self.host_stats[query].keys()) for metric in sorted(self.host_stats[query][host].keys())])+'\n')

