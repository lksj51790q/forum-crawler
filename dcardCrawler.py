import requests
import bs4
from dateutil.parser import parse
import time, random, datetime

import logging, os
#logging configure
logger_level = logging.WARNING
formatter = logging.Formatter('%(asctime)s | %(name)s \n%(levelname)-8s : \n%(message)s \n-------------------------------------\n','%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler("/".join(os.path.realpath(__file__).split("/")[:-1]) + "/crawler.log", 'a', 'utf-8')
file_handler.setFormatter(formatter)
file_handler.setLevel(logger_level)
logger = logging.getLogger("dcardCrawler")
logger.setLevel(logger_level)
logger.addHandler(file_handler)

class Crawler:

	def __init__(self):
		"""
		可自訂變數：
			request最大嘗試次數	in _connect()
			request timeout 	in _connect()

		"""
		#For all
		self.connection = requests.Session()
		self.headers = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36", "referer":'https://www.dcard.tw/f'}
		
		#For _connect() in article_id_generate()
		self.index_html = None
		self.index_url = None

		#For _connect() in get_article(), get_reply()
		self.article_html = None
		self.article_url = None

		#For get_article()
		self.article_id = None
		self.main_content = None
		self.article_result_temp = None

		#For get_reply()
		self.reply_content = []
		self.reply_result_temp = None

		return
	def __enter__(self):
		pass
		return self
	def __exit__(self, type, value, traceback):
		self.index_html = None
		self.index_url = None
		self.article_html = None
		self.article_url = None
		self.article_id = None
		self.main_content = None
		self.article_result_temp = None
		self.reply_content = None
		self.reply_result_temp = None
		self.connection.close()
		return
	def __del__(self):
		self.index_html = None
		self.index_url = None
		self.article_html = None
		self.article_url = None
		self.article_id = None
		self.main_content = None
		self.article_result_temp = None
		self.reply_content = None
		self.reply_result_temp = None
		self.connection.close()
		return

	def article_id_generate(self, board_name, time_start=None, time_end=None):
		#################################################
		## Generate article id that matched conditions ##
		#################################################
		#Note that this function cannot occur error except parameter parsing or something unavoidable.
		#Otherwise, program will crash.

		#Parse parameters
		board_name = str(board_name)
		if time_start:
			time_start = parse(time_start).timestamp()
		else:
			time_start = 0 #time stamp = 0
		if time_end:
			time_end = parse(time_end).timestamp()
		else:
			time_end = (datetime.datetime.now() + datetime.timedelta(days=3)).timestamp() #Time after 3 days

		import json

		url = "https://www.dcard.tw/_api/forums/" + board_name + "/posts?popular=false"
		self._connect(url)
		stop_flag = False
		last_id = None
		while True:
			if last_id:
				url = "https://www.dcard.tw/_api/forums/" + board_name + "/posts?popular=false&before=" + str(last_id)
				self._connect(url)
			article_dict_list = json.loads(self.index_html)
			if type(article_dict_list).__name__ != "list":
				#Error
				return []
			for article in article_dict_list:
				article_time = parse(article["createdAt"]).timestamp()
				last_id = article["id"]
				if article_time > time_end:
					#Omit
					continue
				if article_time < time_start:
					#Stop
					stop_flag = True
					break
				yield article["id"]

			if stop_flag:
				break
		return

	def set_target_article(self, board_name, article_id):
		##############################################################################
		## Scrap appointed article and save necessary information at class variable ##
		##############################################################################
		#https://www.dcard.tw/_api/posts?popular=false&before=228464614

		#Initialize
		self.article_html = None
		self.article_url = None
		self.article_id = None
		self.main_content = None
		self.article_result_temp = None
		self.reply_content = []
		self.reply_result_temp = None

		#Parse parameters
		try:
			article_id = int(article_id)
			self.article_id = article_id
		except ValueError:
			return
		board_name = str(board_name)

		import json

		#Content
		url = "https://www.dcard.tw/_api/forums/" + board_name + "/posts?popular=false&before=" + str(article_id+1)
		self._connect(url, False)
		main_content_list = json.loads(self.article_html)
		for dict_ in main_content_list:
			if dict_["id"] == article_id:
				self.main_content = dict_

		#Reply
		after = 0
		reply_content_list = []
		reply_list_buffer = None
		while True:
			url = "https://www.dcard.tw/_api/posts/" + str(article_id) + "/comments?after=" + str(after)
			self._connect(url, False)
			reply_list_buffer = json.loads(self.article_html)
			if type(reply_list_buffer).__name__ == "list":
				reply_content_list += reply_list_buffer
				if len(reply_list_buffer) == 30:
					#Full reply, try next page
					after += 30
				else:
					#Over
					break
			else:
				#Error
				break
		self.reply_content = reply_content_list

		return
	def get_article(self):
		#####################################
		## Get main article's information  ##
		#####################################
		"""
		Return format:
			{
				'title'		:		(string)
				'time'		:		(float)
				'content'	:		(string)
			}
		"""
		if self.article_result_temp:
			return self.article_result_temp

		try:
			url = "https://www.dcard.tw/f/all/p/" + str(self.article_id)
			self._connect(url, False)
			soup = bs4.BeautifulSoup(self.article_html, "lxml")
			for div in soup.findAll("div"):
				if div.get("class") and div.get("class")[0].find("Post_content") != -1:
					content = div.text

			title = self.main_content["title"]
			time = parse(self.main_content["createdAt"]).timestamp()
			self.article_result_temp = {"title":title, "time":time, "content":content}
		except:
			#import traceback
			#print(self.article_url)
			#print(traceback.format_exc(None))
			return
		return self.article_result_temp
	def get_reply(self):
		############################
		## Get replies of article ##
		############################
		"""
		Return format:
			[
				{
					'time'		:		(float)
					'content'	:		(string)
				},
				...
			]
		"""
		if self.reply_result_temp:
			return self.reply_result_temp
		try:
			reply_buffer = []
			for reply in self.reply_content:
				time = None
				content = None
				time = parse(reply["createdAt"]).timestamp()
				try:
					content = reply["content"]
				except KeyError:
					content = ""
				if time and content:
					reply_buffer.append({'time':time, 'content':content})
			self.reply_result_temp = reply_buffer
		except:
			#import traceback
			#print(self.article_url)
			#print(traceback.format_exc(None))
			return []
		return self.reply_result_temp

	def _connect(self, url, index=True):
		########################################################
		## Get html code from url and store in class variable ##
		########################################################
		if index:
			self.index_html = None
			self.index_url = None
		else:
			self.article_html = None
			self.article_url = None

		#Connect
		for j in range(0, 10):
			for i in range(0,15):
				try:
					target = self.connection.get(url, headers=self.headers, timeout=10)
				except requests.exceptions.ReadTimeout:
					logger.warning("Connection timeout\nurl : " + url)
					time.sleep(random.randint(1,5))
					continue
				except requests.exceptions.ConnectionError:
					import traceback
					logger.warning(traceback.format_exc(None))
					time.sleep(random.randint(60,300))
					continue
				break
			if target.status_code == requests.codes.ok:
				break
			elif target.status_code == 404:
				target.raise_for_status()
			logger.warning("Response status : " + str(target.status_code))
			if j == 9:
				target.raise_for_status()
			time.sleep(60 * j)
			
		#Save
		if index:
			self.index_html = target.text
			self.index_url = target.url
		else:
			self.article_html = target.text
			self.article_url = target.url

		return

def get_board():
	#################################
	## Get all board name of Dcard ##
	#################################
	"""
	Return Format:
		{
			'分類看板':[
				(板名1, 中文板名1),
				(板名2, 中文板名2),
				...
			],
			'校園看板':[
				(板名1, 中文板名1),
				(板名2, 中文板名2),
				...
			]
		}
	"""
	url = "https://www.dcard.tw/f"
	school_board = []
	theme_board = []
	with Crawler() as crawler:
		crawler._connect(url)
		if not crawler.index_html:
			return
		soup = bs4.BeautifulSoup(crawler.index_html, "lxml")
		for li in soup.findAll("li"):     
			if li.get("class") and li.get("class")[0].find("ForumEntryGroup") != -1:
				if li.a and li.a.text == "分類看板":
					for l in li.findAll("li"):
						theme_board.append((l.a.get("href").split("/")[-1],l.a.text))
				elif li.a and li.a.text == "校園看板":
					for l in li.findAll("li"):
						school_board.append((l.a.get("href").split("/")[-1],l.a.text))

	return {'分類看板':theme_board,'校園看板':school_board}