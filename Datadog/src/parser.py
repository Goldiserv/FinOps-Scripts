# TBD

import os
from dotenv import load_dotenv

load_dotenv()
        
def write_to_file(data_str):
    with open("x.txt", "a") as o:
        o.write(data_str)
        o.close()


# write_to_file("aws.rds.read_latency")
