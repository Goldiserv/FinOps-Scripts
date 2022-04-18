
import os
from dotenv import load_dotenv

load_dotenv()

regionName = os.getenv("AWS_PAYER_ACCT_ID")

print(regionName)