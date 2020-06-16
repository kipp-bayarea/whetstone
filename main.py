from whetstone import Authenticate, HTTPRequest
import os
import json

api_key = os.getenv("API_TOKEN")
auth = Authenticate(api_key)
token = auth.GetToken()


meetings = HTTPRequest("meetings", token, api_key).GetData()
with open("meetings.json", "w") as f:
    f.write(json.dumps(meetings))
