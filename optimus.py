import requests
import json
import sys

response = requests.get("http://optimus-agent.snc1/")


print(response)


# o = response.json()
# for result in o["results"]:
#     print(result["trackName"])



# http://optimus-agent.snc1/public/job/trigger/