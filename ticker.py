import time
import os 

starttime = time.time ( )
while True:
  print "tick"
  try:
  	os.system ( 'python ./botornot.py' )
  	time.sleep ( 30.0 - ( ( time.time ( ) - starttime ) % 30.0 ) )
  except Exception as e:
	print ( str( e ) )
	continue
