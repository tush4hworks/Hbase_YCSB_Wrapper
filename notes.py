import sys
import requests

class zepInt:

	def __init__(self,host,zepUser,zepPass):
		self.url='http://'+host
		self.username=zepUser
		self.password=zepPass

	def zepLogin(self):
		resp=requests.post(self.url+'/api/login',data={'userName':self.username,'password':self.password})
		self.cookie=resp.cookies

	def getAllNotebooks(self):
		resp=requests.get(self.url+'/api/notebook',cookies=self.cookie)
		return resp.json()

	def runParagraphs(self,notebook):
		for note in self.getAllNotebooks()['body']:
			if note['name']==notebook:
				resp=requests.post(self.url+'/api/notebook/job/'+note['id'],cookies=self.cookie)
				break

	def checkIfExist(self,notebook):
		zepNotes=[note['name'] for note in self.getAllNotebooks()['body']]
		return (notebook in zepNotes)









