
import os
from dotenv import load_dotenv
from numpy import average

load_dotenv()

# regionName = os.getenv("AWS_PAYER_ACCT_ID")
# print(regionName)

# arr = [1, None, 2]
# a = [1, None, 2, None]
# res = average([x for x in a if x != None])
# print(res)


# a = "'\n                           '"
# b = "'\
#                            '"
# # b = a+"_str"
# print(a)

import json

class Laptop:
	name = 'My Laptop'
	processor = 'Intel Core'
		
#create object
laptop1 = Laptop()
laptop1.name =  os.getenv("DD_API_KEY")
laptop1.processor = os.getenv("DD_APP_KEY")

#convert to JSON string
jsonStr = json.dumps(laptop1.__dict__)

#print json string
print(jsonStr)