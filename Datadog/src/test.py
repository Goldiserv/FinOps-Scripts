
import os
from dotenv import load_dotenv
import json
import myUtils

load_dotenv()

class Secrets:
	api_key = ''
	app_key = ''
		
#create object
laptop1 = Secrets()
laptop1.api_key =  os.getenv("DD_API_KEY")
laptop1.app_key = os.getenv("DD_APP_KEY")

#convert to JSON string
jsonStr = json.dumps(laptop1.__dict__)

#print json string
print(jsonStr)

x = myUtils.getTimeRange(4,14)
print(x)
