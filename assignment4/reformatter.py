import os
from os import listdir
from os.path import isfile, join
import sys, urllib
import socket
import json
import xml.etree.ElementTree as ET
import argparse, pickle

# tree = ET.parse('test.xml')


wikiurl = 'https://en.m.wikipedia.org/wiki/'
def clean_old_in(job_path):
	file_list  = listdir(job_path)
	out_list = [f for f in file_list if (isfile(join(job_path, f)) and f[-3:] == ".in") ]
	for of in out_list:
		os.remove(join(job_path, of))

# class XMLNode(ET.Element):
class XMLNode(object):
	# class member 
	nodeList = []
	tag = ''
	fulltag = ''
	def __init__(self, node):
		self.__node  = node

	def get_node(self):
		# return self.__node
		return self.__node

	@staticmethod
	def childnum(root):
		c = 0
		for child in root:
			c += 1
		return c
	#set self.attr as find value 

	def setAttr(self, root, attr): #say attr = id
		tagroot = str(root.tag)
		# print (tagroot)
		if (tagroot.find( attr )>=0):
			try:
				getattr(self, attr)
			except AttributeError :
				setattr(self, attr, root.text) 
			# setattr(self, attr+"fulltag", tagroot) 
			# cls.nodeList.append(  cls(root) ) # add the node to nodeList 
			return

		if(XMLNode.childnum(root) != 0):
			for child in root:
				self.setAttr(child, attr) #recursion for 
		else: return


	@classmethod
	def createNodeList(cls, root):
		tagroot = str(root.tag)
		if (tagroot.find( cls.tag )>=0):
			if( cls.fulltag== ''): cls.fulltag = tagroot
			cls.nodeList.append(  cls(root) ) # add the node to nodeList 
			return

		if(XMLNode.childnum(root) != 0):
			for child in root:
				cls.createNodeList(child) #recursion for 
		else: return


#class member variable and classmethod can be inherited from parent
class PageNode (XMLNode):
	
	page_num = 0
	tag  = "page"

	def __init__(self, node):
		super(PageNode, self).__init__(node) #self is virtual PageNode instance

	def get_id(self):
		return self.id

	@staticmethod	
	def setAllAttr(attrs): #get all attribute in attrs array
		#every node in nodeList
		for page in PageNode.nodeList:
			for attr in attrs:
				page.setAttr(page.get_node(), attr)
				# print (getattr(page, attr))
			page.setUrl()
			# page.setSnippet()
	def setUrl(self):
		self.url = wikiurl + self.title.replace(' ', '_')

	@staticmethod
	def createDocset(root):
		PageNode.createNodeList(root) #call class method in PageNode class
		PageNode.page_num = len(PageNode.nodeList)
		print("Pages number: "+ str(PageNode.page_num) )

		# print (PageNode.nodeList[0].get_node())
		# PageNode.nodeList[0].setAttr(PageNode.nodeList[0].get_node(), "id")
		# print(PageNode.nodeList[0].get_id())

		attrs = ['id', 'title','text']
		PageNode.setAllAttr(attrs)

		if(PageNode.nodeList[0].get_id() == ''):
			print('attribute setting failed')
		
		

if __name__ == "__main__":
	#get input_file by opt parse
	#python -m assignment4.reformatter assignment2/data/info_ret.xml --job_path=assignment4/idf_jobs --num_partitions=1
	parser = argparse.ArgumentParser(description='Process XML.')

	parser.add_argument('data_path')
	parser.add_argument('--job_path')
	parser.add_argument('--num_partitions' )
	args = parser.parse_args()

	tree = ET.parse(args.data_path)
	root = tree.getroot()
	PageNode.createDocset(root)

	num_partitions = int(args.num_partitions)
	job_path =args.job_path

	lines = [0]*num_partitions
	clean_old_in(job_path)
	fds = [open( join( job_path+'%d.in'%index), 'wb') for index in range(num_partitions)] #binary form output
	page_entries = [{} for i in range(num_partitions)]
	for page in PageNode.nodeList:
		idx = int(page.id) %  num_partitions
		page_entries[idx][page.id] = (page.title, page.text)	
		# page_entries[idx][page.id] =  page.text	
		lines[idx] +=1

	for idx in range(num_partitions):
		pickle.dump(  page_entries[idx] ,  fds[idx] )
		fds[idx].close()

	print("num_lines: ",lines)
