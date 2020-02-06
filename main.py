from plugins.fetcher.ftp import FTPFetcher
import datetime
import uuid
import errno
import os
from dateutil import parser

#config
config = {
    "host"          :   "192.168.2.84",
    "user"          :   "ftpuser",
    "password"      :   "beautiful",
    "dir"           :   "/files/*"
}


progress = {}

# progress = {
#     '/files/t1' : '2020-01-27 12:39:00',
#     '/files/t2' : '2020-01-27 16:38:00',
#     '/files/t3' : '2020-01-28 10:25:00',
#     '/files/t4' : '2020-01-27 15:12:00',
# }


fetcherlog = {
    "progress" : progress,
}

# progress = {}
# print(progress)



archieved_path =  "/home/mandeep/copyfile"


ftp = FTPFetcher(config,archieved_path, fetcherlog)
ftp.client.login(config["user"], config["password"])
response, progress = ftp.run()
print("RESPONSE ", response)

print("PROGRESS ", progress)

# print(len(responses))/