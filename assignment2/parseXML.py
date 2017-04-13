# this IndexerHandler reads an xml and give every term the docID set, and calculate all the docs' relative scores and return {(docID, score)}
# store this doc to doc-partition server.

# ways to calculate the socre
# term frequency TF-IDF (term frequency in a Doc, and doc frequency for a item regarding to doc set) , appear-in-title bonus 


import socket
import json
import xml.etree.ElementTree as ET
from assignment2.inventory import *

# tree = ET.parse('test.xml')


wikiurl = 'https://en.m.wikipedia.org/wiki/'

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
		PageNode.createNodeList(root)
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
	PageNode.createDocset()


