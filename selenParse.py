from datetime import datetime
from socket import gaierror
import time
import sys
import re
import ast
import psycopg2
from arcgis.gis import GIS
from arcgis.geocoding import geocode
from selenium import webdriver

PG_DBNAME           = 'PG_DBNAME'
PG_USER             = 'PG_USER'
PG_HOST             = 'PG_HOST'
PG_PASSWORD         = 'PG_PASSWORD'
GOOGLE_LOGIN        = 'GOOGLE_LOGIN'
GOOGLE_PASSWORD     = 'GOOGLE_PASSWORD'
PATH_SAVESCREEN     = '/path_savescreen'
PATH_CHROMEDRIVER   = '/path_chromedriver/webdriver/chrome/chromedriver.exe'

count               = 0
data                = []
error_data          = []

conn = psycopg2.connect("dbname="+ PG_DBNAME +
						" user="+ PG_USER +
						" host="+ PG_HOST +
						" password="+ PG_PASSWORD)

# скрин экрана браузера
def screen(message):
	driver.save_screenshot(PATH_SAVESCREEN +
						   message + " ~ " + 
						   str(datetime.now().strftime('%d-%m-%Y %H.%M.%S')) + 
						   ".png")

# сохранение в таблицу
def save_to_db(data):
	curs = conn.cursor()    
	
	for dt in data:
		curs.execute("""SELECT * from bigquery_results where country = %(country)s and tag = %(tags)s""",
			{'country': dt['country'], 'tags': dt['tags']})

		result = curs.fetchall()

		if len(result):
			print("IFData:\n", dt)
			curs.execute("""
				UPDATE bigquery_results
				SET count_questions = count_questions + %(count_questions)s
				where country = %(country)s and tag = %(tags)s;""",
				{'country': dt['country'], 'count_questions': int(dt['count_questions']), 'tags': dt['tags']})
			conn.commit()
		else: 
			print("ELSEData:\n", dt)
			curs.execute("""
				INSERT INTO bigquery_results VALUES (%(country)s, %(count_questions)s, %(tags)s);""",
				{'country': dt['country'], 'count_questions': int(dt['count_questions']), 'tags': dt['tags']})
			conn.commit()

try:
	# инициализация GIS для выполнения geocode
	gis = GIS()
		
	# запуск Selenium, открытие браузера
	driver = webdriver.Chrome(PATH_CHROMEDRIVER)

	# переход на сайт
	driver.get("https://bigquery.cloud.google.com/queries")

	# ждёт появление элементов 
	driver.implicitly_wait(130)

	# ввод email 
	driver.find_element_by_name("identifier").send_keys(GOOGLE_LOGIN)

	# нажатие на кнопку далее
	driver.find_element_by_id("identifierNext").click()

	# ввод пароля
	driver.find_element_by_name("password").send_keys(GOOGLE_PASSWORD)

	# нажатие на кнопку далее
	driver.find_element_by_id("passwordNext").click()

	# нажатие на Compose Query
	driver.find_element_by_tag_name("jfk-button").click()

	# нажатие на Open Query последнего запроса в истории
	driver.find_element_by_css_selector(".history-row-title + .jfk-button-mini").click()

	# нажатие на Run Query
	driver.find_element_by_css_selector("jfk-button[class^=query-run]").click()
	time.sleep(1.9)

	# нажатие на Query Close
	driver.find_element_by_css_selector("span[id=query-close]").click()

	# берём текст с номерами вопросов
	textNumber = driver.find_element_by_css_selector("span[class=page-number]").text

	# регулярка для поикса номера последнего вопроса 
	matches = list(filter(None, re.split("\D+", textNumber)))

	# если есть аргумент команды
	if len(sys.argv) == 2:
		# пока начало номеров страницы != введённому числу
		# выполнять переходы и запись строки с номерами вопросов 
		while (int(matches[1]) != int(sys.argv[1])):
			time.sleep(0.5)
			print("matches[1]: ", matches[1])
			driver.find_element_by_css_selector("span[class=records-link]:nth-child(4)").click()
			textNumber = driver.find_element_by_css_selector("span[class=page-number]").text
			matches = list(filter(None, re.split("\D+", textNumber)))

		driver.find_element_by_css_selector("span[class=records-link]:nth-child(4)").click()
		textNumber = driver.find_element_by_css_selector("span[class=page-number]").text
		matches = list(filter(None, re.split("\D+", textNumber)))

	# пока страница с вопросами не последняя
	while int(matches[1]) != int(matches[2]):

		# нажимаем на кнопку JSON
		driver.find_element_by_css_selector("jfk-segmented-button[jfk-model-value*='JSON'] > .jfk-button").click()

		# текст с данными ввиде строки 
		text = driver.find_element_by_css_selector("textarea[class=records-json]").text

		# изменяем строку на список с словарями
		text = ast.literal_eval(text)
		data_to_save = []

		for txt in text:
			# изменяем текст с местоположением на страну
			geocode_result = geocode(txt['Country'])

			# проверка на пустой результат запроса
			if not geocode_result:
				error_data.append({"Text": txt['Country'], "Page": count, "Len_data": len(data)})
				print("Error_data:\n",{"Text": txt['Country'], "Page": count, "Len_data": len(data)})
				continue
			if geocode_result[0]['attributes']['Country'] == "''":
				error_data.append({"Text": txt['Country'], "Page": count, "Len_data": len(data)})
				print("geocode_result: ''\n",{"Text": txt['Country'], "Page": count, "Len_data": len(data)})
				continue
			elif geocode_result[0]['attributes']['Country'] == "":
				error_data.append({"Text": txt['Country'], "Page": count, "Len_data": len(data)})
				print("geocode_result: \n",{"Text": txt['Country'], "Page": count, "Len_data": len(data)})
				continue
			geocode_result = geocode_result[0]['attributes']['Country']

			data_to_save.append({"country": geocode_result, "count_questions": txt['Num_Questions'], "tags": txt['Tags']})

			print({"country": geocode_result, "count_questions": txt['Num_Questions'], "tags": txt['Tags']})
			data.append({"country": geocode_result, "count_questions": txt['Num_Questions'], "tags": txt['Tags'], 
						 "Text": txt['Country'], "Page": count, "Len_data": len(data)})

		# берём текст с номерами вопросов
		textNumber = driver.find_element_by_css_selector("span[class=page-number]").text

		# регулярка для поикса номера последнего вопроса 
		matches = list(filter(None, re.split("\D+", textNumber)))
		print("matches:", matches)

		# клик Next 
		driver.find_element_by_css_selector("span[class=records-link]:nth-child(4)").click()
		count = count + 1
		save_to_db(data_to_save)

except gaierror as e:
	print("Gaierror: ", e)

except Exception as e:
	print("Exception: ", e)

finally:
	print("Error_data: ", error_data)
	print("Len_data: ", len(data))
	print("Len_error_data: ", len(error_data))
	print("Count: ", count)
	screen("finallyScreen")
	conn.close()
	sys.exit()