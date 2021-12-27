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
logger = logging.getLogger("pttCrawler")
logger.setLevel(logger_level)
logger.addHandler(file_handler)

class Crawler:

	def __init__(self):
		"""
		可自訂變數：
			request最大嘗試次數	in _connect()
			request timeout 	in _connect()

		"""
		#logger.debug("Initialize Crawler Object")

		#For all
		self.connection = requests.Session()
		self.headers = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36", "referer":'https://www.ptt.cc/bbs/hotboards.html'}
		
		#For _connect() in article_id_generate()
		self.index_html = None
		self.index_url = None

		#For _connect() in get_article(), get_reply()
		self.article_html = None
		self.article_url = None

		#For get_article()
		self.article_result_temp = None

		#For get_reply()
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
		self.article_result_temp = None
		self.reply_result_temp = None
		self.connection.close()
		return
	def __del__(self):
		self.index_html = None
		self.index_url = None
		self.article_html = None
		self.article_url = None
		self.article_result_temp = None
		self.reply_result_temp = None
		self.connection.close()
		return

	def article_id_generate(self, board_name, time_start=None, time_end=None):
		###########################################
		## Generate urls that matched conditions ##
		###########################################
		#Note that this function cannot occur error except parameter parsing or something unavoidable.
		#Otherwise, program will crash.

		#Parse parameters
		board_name = str(board_name)
		if time_start:
			time_start = parse(time_start)
		else:
			time_start = parse("1970-01-01 08:00:00") #time stamp = 0
		if time_end:
			time_end = parse(time_end)
		else:
			time_end = datetime.datetime.now() + datetime.timedelta(days=3) #Time after 3 days
		logger.debug("Article ID generate start\nBoard Name : " + board_name + "\nTime Range : " + str(time_start) + " to " + str(time_end))

		#Connect to board index
		#"https://www.ptt.cc/bbs/" + board_name + "/index.html"
		self._connect("https://www.ptt.cc/bbs/" + board_name + "/index.html")
		soup = bs4.BeautifulSoup(self.index_html, "lxml")

		#Get current index page number
		page_num = None
		page_links = soup.find(class_="btn-group-paging").findAll("a")
		if not page_links:
			page_num = 1
		page_href = None
		for a in page_links:
			if a.text.find("上頁") != -1:
				page_href = a.get("href")
				break
		if not page_href:
			page_num = 1
		try:
			page_num = int(page_href.split("/")[-1].split(".")[0].replace("index", "")) + 1
		except ValueError:
			page_num = 1

		#Iterate all page
		stop_flag = False
		for page in reversed(range(1, page_num + 1)):
			logger.info("Article generate board " + board_name + " page " + str(page))
			if page != page_num:
				#Get new page
				self._connect("https://www.ptt.cc/bbs/" + board_name + "/index" + str(page) + ".html")
				soup = bs4.BeautifulSoup(self.index_html, "lxml")

			#Remove top article
			top_flag = False
			for div in soup.findAll("div"):
				if not div.get("class"):
					continue
				if div.get("class")[0] == "r-list-sep":
					top_flag = True
				if top_flag:
					div.extract()

			#yield article url
			for div in reversed(soup.find_all("div", "r-ent")):
				if not div.find("a"):
					continue

				url = div.find("a").get("href")
				if url:
					#Time check
					article_time = datetime.datetime.fromtimestamp(int(url.split("/")[-1].split(".")[1]))
					if article_time > time_end:
						#Omit
						continue
					if article_time < time_start:
						#Stop
						logger.info("Out of time range, set stop flag")
						stop_flag = True
						break
					yield url

			#Check stop flag
			if stop_flag:
				break
		logger.info("Board " + board_name + " generate over")
		return

	def set_target_article(self, url):
		##############################################################################
		## Scrap appointed article and save necessary information at class variable ##
		##############################################################################

		#Initialize
		self.article_html = None
		self.article_url = None
		self.article_result_temp = None
		self.reply_result_temp = None

		#Parse parameters
		if url.find("https://www.ptt.cc") == -1:
			url = "https://www.ptt.cc/" + url[url.find("bbs"):]

		#Connect
		self._connect(url, False)
		return
	def get_article(self):
		#####################################
		## Get main article's information  ##
		#####################################
		"""
		Return format:
			{
				'title'		:		(string)
				'time'		:		(datetime)
				'content'	:		(string)
			}
		"""
		#If an error occur then return None
		if self.article_result_temp:
			return self.article_result_temp
		try:
			import re

			soup = bs4.BeautifulSoup(self.article_html, "lxml")
			for script in soup.findAll("script"):
				script.extract()
			for div in soup.find("div", id="main-content").findAll("div"):
				if not div.get("class"):
					continue
				if div.get("class")[0] == "article-metaline" or div.get("class")[0] == "article-metaline-right":
					div.extract()

			time = datetime.datetime.fromtimestamp(int(self.article_url.split("/")[-1].split(".")[1]))
			title = " ".join(soup.find("meta",property="og:title").get("content").split())
			text = re.sub(re.compile('[\n]{2,}'), "\n", soup.find("div", id="main-content").text.strip())
			content = text[:text.find("--\n※ 發信站")].strip()
			self.article_result_temp = {"title":title, "time":time, "content":content}

			#Replies parse
			replies = text[text.find("\n", text.find("※ 文章網址")):].strip().split("\n")
			index = -1
			content_buffer = ""
			time_buffer = None
			last_id = ""

			for reply in replies:
				result = self._reply_parse(reply)
				if not result:
					continue
				if result["id"] == last_id:
					content_buffer += result["content"]
				else:
					last_id = result["id"]
					if index >= 0:
						replies[index] = {"content":content_buffer, "time":str(time_buffer.date())[str(time_buffer.date()).find("-")+1:] + " " + str(time_buffer.time())[:str(time_buffer.time()).rfind(":")]}
					content_buffer = result["content"]
					time_buffer = result["datetime"]
					index += 1

			if index == -1:
				self.reply_result_temp = []
			else:
				replies[index] = {"content":content_buffer, "time":str(time_buffer.date())[str(time_buffer.date()).find("-")+1:] + " " + str(time_buffer.time())[:str(time_buffer.time()).rfind(":")]}
				index += 1
				self.reply_result_temp = replies[:index]
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
					'time'		:		(string)
					'content'	:		(string)
				},
				...
			]
		"""
		if self.reply_result_temp:
			return self.reply_result_temp
		try:
			import re

			soup = bs4.BeautifulSoup(self.article_html, "lxml")
			for script in soup.findAll("script"):
				script.extract()
			for div in soup.find("div", id="main-content").findAll("div"):
				if not div.get("class"):
					continue
				if div.get("class")[0] == "article-metaline" or div.get("class")[0] == "article-metaline-right":
					div.extract()

			time = datetime.datetime.fromtimestamp(int(self.article_url.split("/")[-1].split(".")[1]))
			title = " ".join(soup.find("meta",property="og:title").get("content").split())
			text = re.sub(re.compile('[\n]{2,}'), "\n", soup.find("div", id="main-content").text.strip())
			content = text[:text.find("--\n※ 發信站")].strip()
			self.article_result_temp = {"title":title, "time":time, "content":content}

			#Replies parse
			replies = text[text.find("\n", text.find("※ 文章網址")):].strip().split("\n")
			index = -1
			content_buffer = ""
			time_buffer = None
			last_id = ""

			for reply in replies:
				result = self._reply_parse(reply)
				if not result:
					continue
				if result["id"] == last_id:
					content_buffer += result["content"]
				else:
					last_id = result["id"]
					if index >= 0:
						replies[index] = {"content":content_buffer, "time":str(time_buffer.date())[str(time_buffer.date()).find("-")+1:] + " " + str(time_buffer.time())[:str(time_buffer.time()).rfind(":")]}
					content_buffer = result["content"]
					time_buffer = result["datetime"]
					index += 1

			if index == -1:
				self.reply_result_temp = []
			else:
				replies[index] = {"content":content_buffer, "time":str(time_buffer.date())[str(time_buffer.date()).find("-")+1:] + " " + str(time_buffer.time())[:str(time_buffer.time()).rfind(":")]}
				index += 1
				self.reply_result_temp = replies[:index]
		except:
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
					target = self.connection.get(url, headers=self.headers, timeout=10, cookies = {'over18':'1'})
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

	def _reply_parse(self, reply):
		############################
		## Parse reply's elements ##
		############################
		#reply format:
		#tag id: content (ip) date (time)

		reply_split = reply.split()
		tag = None
		id_ = None
		content = None
		ip = None
		datetime = None
		content_index = 2

		#Check format
		if len(reply_split) < 4:
			return
		if reply_split[0] not in ["推", "→", "噓"]:
			return
		else:
			tag = reply_split[0]
		if reply_split[1][-1] != ":":
			if reply_split[2] == ":":
				id_ = reply_split[1]
				content_index = 3
			else:
				return
		else:
			id_ = reply_split[1][:-1]


		if is_time(reply_split[-1]) and is_date(reply_split[-2]):
			datetime = parse(reply_split[-2] + " " + reply_split[-1])
			if is_ip(reply_split[-3]):
				# ip date time
				ip = reply_split[-3]
				content = " ".join(reply_split[content_index:-3])
			else:
				# date time
				content = " ".join(reply_split[content_index:-2])
		elif is_date(reply_split[-1]):
			datetime = parse(reply_split[-1])
			if is_ip(reply_split[-2]):
				# ip date
				ip = reply_split[-2]
				content = " ".join(reply_split[content_index:-2])
			else:
				# date
				content = " ".join(reply_split[content_index:-1])
		else:
			return
		return {'tag':tag, 'id':id_, 'ip':ip, 'datetime':datetime, 'content':content}


def get_board():
	###############################
	## Get all board name of PTT ##
	###############################
	#return set object
	import concurrent.futures
	from queue import Queue

	board = set()
	clses = set()
	clses.add(1)
	q = Queue()
	q.put("1")
	with Crawler() as crawler:

		def _get_nodes(cls_):
			url = 'https://www.ptt.cc/cls/' + cls_
			crawler._connect(url)
			if not crawler.index_html:
				return
			soup = bs4.BeautifulSoup(crawler.index_html, "lxml")
			boards = soup.findAll('a', class_="board")
			for a in boards:
				url = a.get("href")
				if url.find("bbs") != -1:
					if url.split("/")[2] in board:
						continue
					board.add(url.split("/")[2])
				elif url.find("cls") != -1:
					if int(url.split("/")[-1]) in clses:
						continue
					clses.add(int(url.split("/")[-1]))
					q.put(url.split("/")[-1])
				else:
					pass
			return

		with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
			futures = []
			while True:
				while not q.empty():
					futures.append(executor.submit(_get_nodes, q.get()))
				if len(concurrent.futures.wait(futures, 1)[1]) != 0 or not q.empty():
					continue
				break

	return board

def get_hot_board():
	boards = []
	url = "https://www.ptt.cc/bbs/hotboards.html"
	with Crawler() as crawler:
		crawler._connect(url)
		soup = bs4.BeautifulSoup(crawler.index_html, "lxml")
		for a in soup.findAll("a", class_="board"):
			boards.append(a.get("href").split("/")[-2])

	return boards

def is_ip(s):
	if len(s.split(".")) != 4:
		return False
	for i in s.split("."):
		try:
			if int(i) < 0 or int(i) > 255:
				return False
		except:
			return False
	return True
def is_date(s):
	if len(s.split("/")) != 2:
		return False
	try:
		if int(s.split("/")[0]) < 0 or int(s.split("/")[0]) > 12:
			return False
		if int(s.split("/")[1]) < 0 or int(s.split("/")[1]) > 31:
			return False
	except:
		return False
	return True
def is_time(s):
	if len(s.split(":")) != 2:
		return False
	try:
		if int(s.split(":")[0]) < 0 or int(s.split(":")[0]) > 23:
			return False
		if int(s.split(":")[1]) < 0 or int(s.split(":")[1]) > 59:
			return False
	except:
		return False
	return True




