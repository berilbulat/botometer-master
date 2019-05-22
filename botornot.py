import MySQLdb
import tweepy
import botometer
import json
import sys
import time
import os

def UtcNow ( ):
    now = int( time.time ( ) )
    return now

def connect_remote_db ( ): 
	# Open database connection
	db = MySQLdb.connect ( "remote-host","USER","PWD","DATABASE" )
 
	# prepare a cursor object using cursor  method
	cursor = db.cursor () 
 
	sql = "select distinct user_name from mueller"
	cursor.execute ( sql )
	results = cursor.fetchall ( ) 
 
	# disconnect from server
	db.close ( )
	
	return results

def read_local_db ( ):
        # Open database connection
        db = MySQLdb.connect ( "localhost","root","PWD","DATABABSE" )

        # prepare a cursor object using cursor  method
        cursor = db.cursor ()

        sql = "select screen_name from botometer where id_str IS NULL ORDER BY RAND() LIMIT 5"
        cursor.execute ( sql )
        results = cursor.fetchall ( )

        # disconnect from server
        db.close ( )

        return results

def write_local_db ( results ):
	db = MySQLdb.connect ( "localhost","root","PWD","DATABASE" )
	sql = "insert into botometer ( screen_name ) values ( %s )"
	
	cursor = db.cursor ( )
	for res in results:
		try:
			cursor.execute ( sql, res )
			db.commit ( )
		except Exception as e:
			print ( "ERROR: " + str( e ) )
			db.rollback ( )
			continue

	db.close ( )

def update_local_db ( bot_result ):
        db = MySQLdb.connect ( "localhost","root","PWD","DATABASE" )
        sql = "update botometer set id_str = %s, universal = %s, english = %s, cap_universal = %s, cap_english = %s where screen_name = %s"

        cursor = db.cursor ( )
        try:
        	cursor.execute ( sql, bot_result )
                db.commit ( )
        except Exception as e:
        	print ( "ERROR: " + str( e ) )
                db.rollback ( )

        db.close ( )

def update_error_local_db ( bot_result ):
        db = MySQLdb.connect ( "localhost","root","PWD","DATABASE" )
        sql = "update botometer set error_message = %s, id_str = %s where screen_name = %s"

        cursor = db.cursor ( )
        try:
                cursor.execute ( sql, bot_result )
                db.commit ( )
        except Exception as e:
                print ( "ERROR: " + str( e ) )
                db.rollback ( )

        db.close ( )

def send_botometer ( bom, screen_name ):
	bot_result = ( )
	try:
		result = bom.check_account ( screen_name )
		id_str = result['user']['id_str']
		if 'display_scores' in result:
			universal = result['display_scores']["universal"]
			english = result['display_scores']["english"]
		if 'cap' in result:
			cap_universal = result['cap']["universal"]
                        cap_english = result['cap']["english"]	
		bot_result = ( id_str, universal, english, cap_universal, cap_english, screen_name )
		print ( bot_result )
		update_local_db ( bot_result )
	except Exception as e: 
		print ( str( e ) )
		bot_result = ( str( e ), 'error', screen_name )
		update_error_local_db ( bot_result )
		


#results = connect_remote_db ( )

#write_local_db ( results )

pid = str( os.getpid( ) )
pidfile = "/tmp/botornot.pid"

if os.path.isfile(pidfile):
    print "%s already exists, exiting" % pidfile
    sys.exit ( )

file( pidfile, 'w' ).write( pid )
try:
	start_time = UtcNow ( )
	mashape_key = "XXXXXXX"
	twitter_app_auth = {
    		'consumer_key': 'XXXXXXXXX',
    		'consumer_secret': 'XXXXXXX',
    		'access_token': 'XXXXXXXX',
    		'access_token_secret': 'XXXXXXXXX',
  	}
	bom = botometer.Botometer ( wait_on_ratelimit=True, mashape_key=mashape_key, **twitter_app_auth )

	count = 0

	print ( start_time )
	while True:
		results = read_local_db ( )
		print ( results )
		for res in results:
			now = UtcNow ( )
        		if now - start_time > 180:
                		print ( "Exitting: " + str( now - start_time ) )
				sys.exit ( )		
			send_botometer ( bom, res[0] )
			count += 1
			if count > 60:
				sys.exit ( )
finally:
    os.unlink ( pidfile )			
