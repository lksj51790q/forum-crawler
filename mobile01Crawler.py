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
logger = logging.getLogger("mobile01Crawler")
logger.setLevel(logger_level)
logger.addHandler(file_handler)

class Crawler:

	def __init__(self):
		"""
		可新增自訂變數：
			request最大嘗試次數	in _connect()
			request timeout 	in _connect()

		"""
		#For all
		self.connection = requests.Session()
		self.headers = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36", "referer":'https://www.mobile01.com/'}

		#For _connect() in article_id_generate()
		self.index_html = None
		self.index_url = None

		#For _connect() in get_article(), get_reply()
		self.article_html = None
		self.article_url = None

		#For get_article(), get_reply()
		self.php_url = None
		self.php_payload = None
		self.forum_content = None

		#For get_article()
		self.article_result_temp = None

		#For get_reply()
		self.reply_result_temp = None
		self.page_num = None
		
		return
	def __enter__(self):
		pass
		return self
	def __exit__(self, type, value, traceback):
		self.index_html = None
		self.index_url = None
		self.php_payload = None
		self.article_html = None
		self.article_url = None
		self.php_url = None
		self.forum_content = None
		self.article_result_temp = None
		self.reply_result_temp = None
		self.page_num = None
		self.connection.close()
		return
	def __del__(self):
		self.index_html = None
		self.index_url = None
		self.php_payload = None
		self.article_html = None
		self.article_url = None
		self.php_url = None
		self.forum_content = None
		self.article_result_temp = None
		self.reply_result_temp = None
		self.page_num = None
		self.connection.close()
		return

	def article_id_generate(self, board_id, time_start=None, time_end=None):
		###########################################
		## Generate urls that matched conditions ##
		###########################################
		#Note that this function cannot occur error except parameter parsing or something unavoidable.
		#Otherwise, program will crash.

		#Parse parameters
		board_id = str(board_id)
		if time_start:
			time_start = parse(time_start)
		else:
			time_start = parse("1970-01-01 08:00:00") #time stamp = 0
		if time_end:
			time_end = parse(time_end)
		else:
			time_end = datetime.datetime.now() + datetime.timedelta(days=3) #Time after 3 days

		#Connect to Mobile01
		self._connect("https://www.mobile01.com/topiclist.php?f=" + board_id + "&sort=topictime")
		if self.index_url.find("sort=topictime") == -1:
			#If "topiclist.php" isn't correct website, mobile01 will redirect page to correct one.
			#But "sort=topictime" will be delete. So we need to add it again
			self._connect(self.index_url + "?f=" + board_id + "&sort=topictime")
		
		#Find the maximum page number
		soup = bs4.BeautifulSoup(self.index_html, "lxml")
		page_num = self._get_page_num(soup.findAll("div", class_="pagination")[1].findAll("a"))

		#Iterate all page in time range
		stop_flag = False
		for page in range(1, page_num+1):
			if page != 1:
				#Next page
				url = self.index_url[:self.index_url.find("?")] + "?f=" + board_id + "&sort=topictime&p=" + str(page)
				self._connect(url)
				soup = bs4.BeautifulSoup(self.index_html, "lxml")

			#yield article url
			for row in soup.findAll('tr'):
				if row.find('a', class_="topic_gen") == None:
					continue

				#Check if article is in time range
				article_time = parse(row.find('td', class_="authur").p.string)
				if article_time > time_end:
					#Omit
					continue
				if article_time < time_start:
					#Stop
					stop_flag = True
					break

				#get url
				yield row.find('td', class_="authur").a.get('href').split("=")[-1]

			#Check stop flag
			if stop_flag:
				break

		return
	
	def set_target_article(self, board_id, article_id):
		##############################################################################
		## Scrap appointed article and save necessary information at class variable ##
		##############################################################################

		#Initialize
		self.article_html = None
		self.article_url = None
		self.php_url = None
		self.php_payload = None
		self.forum_content = None
		self.page_num = None
		self.article_result_temp = None
		self.reply_result_temp = None

		#Parse parameters
		board_id = str(board_id)
		article_id = str(article_id)

		#Connect
		self.php_payload = "?f=" + board_id + "&t=" + article_id
		url = "https://www.mobile01.com/topicdetail.php" + self.php_payload
		self._connect(url, False)
		self.php_url = self.article_url[:self.article_url.find("?")]

		#Parse article content
		soup = bs4.BeautifulSoup(self.article_html, "lxml")
		self.forum_content = soup.find("div", class_="forum-content")
		self.page_num = self._get_page_num(soup.findAll("div", class_="pagination")[1].findAll("a"))

		#Remove useless nodes
		for trash in self.forum_content.findAll('blockquote'):
			trash.extract()

		#Release useless variable
		self.article_html = None
		self.article_url = None
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
		if self.article_result_temp:
			return self.article_result_temp

		import re

		#Scrap main data
		title = self.forum_content.main.find("h1", class_="topic").string.strip()
		article_time = parse(self.forum_content.find('div', class_="date").string.split("#")[0])
		content = re.sub(re.compile('[\n]{3,}'), "\n\n", self.forum_content.find('div', class_="single-post-content").text.strip())
		self.article_result_temp = {'title':title, 'time':article_time, 'content':content}

		#Release useless variable
		if self.article_result_temp and self.reply_result_temp:
			self.forum_content = None

		return self.article_result_temp
	def get_reply(self):
		############################
		## Get replies of article ##
		############################
		"""
		Return format:
			[
				{
					'time'		:		(datetime)
					'content'	:		(string)
				},
				...
			]
		"""
		if self.reply_result_temp:
			return self.reply_result_temp

		import re, copy

		forum_content = copy.copy(self.forum_content)
		self.reply_result_temp = []
		for page in range(1, self.page_num+1):
			if page == 1:
				forum_content.find('div', class_="date").extract()
				forum_content.find('div', class_="single-post-content").extract()
			else:
				url = self.php_url + self.php_payload + "&p=" +str(page)
				self._connect(url, False)
				forum_content = bs4.BeautifulSoup(self.article_html, "lxml").find("div", class_="forum-content")
				for trash in forum_content.findAll('blockquote'):
					trash

			time_buffer = []
			conotent_buffer = []

			for article_time in forum_content.findAll('div', class_="date"):
				time_buffer.append(parse(article_time.string.split("#")[0]))

			for content in forum_content.findAll('div', class_="single-post-content"):
				conotent_buffer.append(re.sub(re.compile('[\n]{3,}'), "\n\n", content.text.strip()).strip())

			for article_time, content in zip(time_buffer, conotent_buffer):
				self.reply_result_temp.append({'time':article_time,'content':content})

			time_buffer = None
			conotent_buffer = None

		#Release useless variable
		self.page_num = None
		if self.article_result_temp and self.reply_result_temp:
			self.forum_content = None

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
	def _get_page_num(self, pagination):
		####################################################################################
		## Find out how many page in the article or the board use div of pagination class ##
		####################################################################################
		if len(pagination) == 0:
			pagination = 1
		elif len(pagination) < 5:
			pagination = int(pagination[-2].get("href").split("=")[-1])
		else:
			pagination = int(pagination[-1].get("href").split("=")[-1])
		return pagination


def get_board():
	#####################################################
	## Get all forum id, name and category of Mobile01 ##
	#####################################################
	"""
		return format:
		{
			category1 : {forum_id1 : forum_name1, forum_id2 : forum_name2, ...},
			category2 : { ... },
			...
		}
	"""
	with Crawler() as crawler:
		crawler._connect("https://www.mobile01.com/")
		if not crawler.index_html:
			return
		soup = bs4.BeautifulSoup(crawler.index_html, "lxml")
		if not soup:
			return
		forum_id_dict = {}
		category_name = None
		while True:
			category_list = soup.find("ul", class_="sf-menu").li
			if not category_list: 
				break
			try:
				if category_list.a.get("href").find("category.php") == -1 and category_list.a.get("href").find("waypoint.php") == -1:
					category_list.extract()
					continue
			except AttributeError:
				category_list.extract()
				continue
			category_name = category_list.a.text.replace(".","")
			forum_id_dict.update({category_name:{}})
			for a in category_list.findAll("a"):
				href = a.get("href")
				name = a.string
				if href.find("topiclist.php?") == -1 and href.find("waypointtopiclist.php?") == -1:#
					continue
				forum_id_dict[category_name].update({int(href.split("f=")[-1].split("&")[0]):name})
			category_list.extract()
	return forum_id_dict
