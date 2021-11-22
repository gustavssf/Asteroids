from configparser import Interpolation
from os import name
import requests
import json
import datetime
import mysql
import mysql.connector
import logging
import logging.config
import time
import yaml

from configparser import ConfigParser
from datetime import datetime
from mysql.connector import Error

#Initialize database
def init_db():
	global connection
	connection = mysql.connector.connect(host=database_host, database=database_name, user=database_user, password=database_pass)

#Getting cursot for database
def get_cursor():
	global connection
	try:
		connection.ping(reconnect=True, attempts=1, delay=0)
		connection.commit()
	except mysql.connector.Error as err:
		logger.error("No connection to db " + str(err))
		connection = init_db()
		connection.commit()
	return connection.cursor()

#Function to get today's date
def get_date():
	# Getting todays date
	dt = datetime.now()
	request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
	logger.info("Generated today's date: " + str(request_date))
	return str(request_date)

#Function to insert data in database
def insert_in_database(create_date, hazardous, name, url, diam_min, diam_max, ts, dt_utc, dt_local, speed, distance, ast_id):
	cursor = get_cursor()
	try:
		cursor = connection.cursor()
		result  = cursor.execute( "INSERT INTO `asteroids_daily` (`create_date`, `hazardous`, `name`, `url`, `diam_min`, `diam_max`, `ts`, `dt_utc`, `dt_local`, `speed`, `distance`, `ast_id`) VALUES ('" + str(create_date) + "', '" + str(hazardous) + "', '" + str(name) + "', '" + str(url) + "', '" + str(diam_min) + "', '" + str(diam_max) + "', '" + str(ts) + "', '" + str(dt_utc) + "', '" + str(dt_local) + "', '" + str(speed) + "', '" + str(distance) + "', '" + str(ast_id) + "')")
		connection.commit()
	except Error as e :
		logger.error( "INSERT INTO `asteroids_daily` (`create_date`, `hazardous`, `name`, `url`, `diam_min`, `diam_max`, `ts`, `dt_utc`, `dt_local`, `speed`, `distance`, `ast_id`) VALUES ('" + str(create_date) + "', '" + str(hazardous) + "', '" + str(name) + "', '" + str(url) + "', '" + str(diam_min) + "', '" + str(diam_max) + "', '" + str(ts) + "', '" + str(dt_utc) + "', '" + str(dt_local) + "', '" + str(speed) + "', '" + str(distance) + "', '" + str(ast_id) + "')")
		logger.error('Problem inserting asteroid values into DB: ' + str(e))
		pass

#Function to check if asteroid exists in database
def check_asteroid_existence(request_day, ast_id):
	records = []
	cursor = get_cursor()
	try:
		cursor = connection.cursor()
		result  = cursor.execute("SELECT count(*) FROM asteroids_daily WHERE `create_date` = '" + str(request_day) + "' AND `ast_id` = '" + str(ast_id) + "'")
		records = cursor.fetchall()
		connection.commit()
	except Error as e :
		logger.error("SELECT count(*) FROM asteroids_daily WHERE `create_date` = '" + str(request_day) + "' AND `ast_id` = '" + str(ast_id) + "'")
		logger.error('Problem checking if asteroid exists: ' + str(e))
		pass
	return records[0][0]

#Function to prepare asteroids array for inserting in database
def push_asteroids_to_db(request_day, ast_array, hazardous):
	for asteroid in ast_array:
		if check_asteroid_existence(request_day, asteroid[9]) == 0:
			logger.debug("Asteroid NOT in db")
			insert_in_database(request_day, hazardous, asteroid[0], asteroid[1], asteroid[2], asteroid[3], asteroid[4], asteroid[5], asteroid[6], asteroid[7], asteroid[8], asteroid[9])
		else:
			logger.debug("Asteroid already IN DB")

#Main function
if __name__ == "__main__":
	# Atver žurnalēšanas konfigurācijas failu
	with open('./log_asteroids.yaml', 'r') as stream:
		config = yaml.safe_load(stream)

	logging.config.dictConfig(config)

	#Creating config reader
	logger = logging.getLogger('root')

	logger.info('Asteroid processing service started')

	#Reaading from config file
	logger.info('Loading configuration from file')

	try:
		config = ConfigParser()
		config.read('config.ini')

		api_key = config.get('nasa', 'api_key')
		api_url = config.get('nasa', 'api_url')
		database_host = config.get('database_config', 'database_host')
		database_name = config.get('database_config', 'database_name')
		database_user = config.get('database_config', 'database_user')
		database_pass = config.get('database_config', 'database_pass')
	except:
		logger.exception('')
	logger.info('DONE')

	connection = None
	connected = False

	init_db()

	#Opening connection to mysql DB
	logger.info('Connecting to MySQL DB')
	try:
		cursor = get_cursor()
		if connection.is_connected():
			db_Info = connection.get_server_info()
			logger.info('Connected to MySQL database. MySQL Server version on ' + str(db_Info))
			cursor = connection.cursor()
			cursor.execute("select database();")
			record = cursor.fetchone()
			logger.debug('Your connected to - ' + str(record))
			connection.commit()
	except Error as e :
		logger.error('Error while connecting to MySQL' + str(e))

	logger.info("Request url: " + str(api_url + "rest/v1/feed?start_date=" + get_date() + "&end_date=" + get_date() + "&api_key=" + api_key))
	r = requests.get(api_url + "rest/v1/feed?start_date=" + get_date() + "&end_date=" + get_date() + "&api_key=" + api_key)

	logger.info("Response status code: " + str(r.status_code))
	logger.info("Response headers: " + str(r.headers))
	logger.info("Response content: " + str(r.text))

	#Collectiong asteroid data if API responds successfully
	if r.status_code == 200:

		json_data = json.loads(r.text)

		ast_safe = []
		ast_hazardous = []

		if 'element_count' in json_data:
			ast_count = int(json_data['element_count'])
			print("Asteroid count today: " + str(ast_count))

			if ast_count > 0:
				for val in json_data['near_earth_objects'][get_date()]:
					if 'name' and 'nasa_jpl_url' and 'estimated_diameter' and 'is_potentially_hazardous_asteroid' and 'close_approach_data' in val:
						tmp_ast_name = val['name']
						tmp_ast_nasa_jpl_url = val['nasa_jpl_url']
						tmp_ast_id = val['id']
						if 'kilometers' in val['estimated_diameter']:
							if 'estimated_diameter_min' and 'estimated_diameter_max' in val['estimated_diameter']['kilometers']:
								tmp_ast_diam_min = round(val['estimated_diameter']['kilometers']['estimated_diameter_min'], 3)
								tmp_ast_diam_max = round(val['estimated_diameter']['kilometers']['estimated_diameter_max'], 3)
							else:
								tmp_ast_diam_min = -2
								tmp_ast_diam_max = -2
						else:
							tmp_ast_diam_min = -1
							tmp_ast_diam_max = -1

						tmp_ast_hazardous = val['is_potentially_hazardous_asteroid']

						if len(val['close_approach_data']) > 0:
							if 'epoch_date_close_approach' and 'relative_velocity' and 'miss_distance' in val['close_approach_data'][0]:
								tmp_ast_close_appr_ts = int(val['close_approach_data'][0]['epoch_date_close_approach']/1000)
								tmp_ast_close_appr_dt_utc = datetime.utcfromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')
								tmp_ast_close_appr_dt = datetime.fromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')

								if 'kilometers_per_hour' in val['close_approach_data'][0]['relative_velocity']:
									tmp_ast_speed = int(float(val['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']))
								else:
									tmp_ast_speed = -1

								if 'kilometers' in val['close_approach_data'][0]['miss_distance']:
									tmp_ast_miss_dist = round(float(val['close_approach_data'][0]['miss_distance']['kilometers']), 3)
								else:
									tmp_ast_miss_dist = -1
							else:
								tmp_ast_close_appr_ts = -1
								tmp_ast_close_appr_dt_utc = "1969-12-31 23:59:59"
								tmp_ast_close_appr_dt = "1969-12-31 23:59:59"
						else:
							print("No close approach data in message")
							tmp_ast_close_appr_ts = 0
							tmp_ast_close_appr_dt_utc = "1970-01-01 00:00:00"
							tmp_ast_close_appr_dt = "1970-01-01 00:00:00"
							tmp_ast_speed = -1
							tmp_ast_miss_dist = -1

						print("------------------------------------------------------- >>")
						print("Asteroid name: " + str(tmp_ast_name) + " | INFO: " + str(tmp_ast_nasa_jpl_url) + " | Diameter: " + str(tmp_ast_diam_min) + " - " + str(tmp_ast_diam_max) + " km | Hazardous: " + str(tmp_ast_hazardous))
						print("Close approach TS: " + str(tmp_ast_close_appr_ts) + " | Date/time UTC TZ: " + str(tmp_ast_close_appr_dt_utc) + " | Local TZ: " + str(tmp_ast_close_appr_dt))
						print("Speed: " + str(tmp_ast_speed) + " km/h" + " | MISS distance: " + str(tmp_ast_miss_dist) + " km")
						
						# Adding asteroid data to the corresponding array
						if tmp_ast_hazardous == True:
							ast_hazardous.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist, tmp_ast_id])
						else:
							ast_safe.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist, tmp_ast_id])

			else:
				print("No asteroids are going to hit earth today")
				logger.info('We are safe')

		print("Hazardous asteorids: " + str(len(ast_hazardous)) + " | Safe asteroids: " + str(len(ast_safe)))

		#Sorting hazardous asteroids
		if len(ast_hazardous) > 0:

			ast_hazardous.sort(key = lambda x: x[4], reverse=False)

			logger.info("Today's possible apocalypse (asteroid impact on earth) times:")
			for asteroid in ast_hazardous:
				logger.info(str(asteroid[6]) + " " + str(asteroid[0]) + " " + " | more info: " + str(asteroid[1]))

			ast_hazardous.sort(key = lambda x: x[8], reverse=False)
			logger.info("Closest passing distance is for: " + str(ast_hazardous[0][0]) + " at: " + str(int(ast_hazardous[0][8])) + " km | more info: " + str(ast_hazardous[0][1]))
			date = get_date()
			
		else:
			print("No asteroids close passing earth today")
		#Sending asteroids to database
		push_asteroids_to_db(get_date(), ast_hazardous, 1)
		push_asteroids_to_db(get_date(), ast_safe, 0)
	else:
		logger.error("Unable to get response from API. Response code: " + str(r.status_code) + " | content: " + str(r.text))