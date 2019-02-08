# Whetstone
Access teacher observation data from Whetstone via REST API.

## Getting Started
### Requirements
* Python 3+

### Installation
From command line run the following command
```
pip install git+https://github.com/KIPPDC/whetstone.git
```

### Parameters
* *apikey* (String) - A valid API key taken from the Whetstone user settings found at https://app.whetstoneeducation.com/me. **Note: You must be a Super Admin in Whetstone to access a functional API key.**

## Usage
Use the methods below in this library
### Authenticate(apikey)
Get access token from Whetstone in order to make API calls.

### GetRequest(object, token, apikey, payload)
Get record level response of a specific Whetstone object. The output of this call is a JSON object.
```json
[
  {
    "object":   "users",
    "rowcount": "2",
    "records":  [
      {"_id": "8921j32hsjad8", "email": "user@whetstone.com"},
      {"_id": "823122jkls89ff", "email": "user2@whetstone.com"}
    ]
  },

]
```

Available HTTPRequest objects:
* users
* observations
* meetings
* meeting_modules
* schools
* assignments
* scores
* informals
* rubrics
* measurements
* measurement_groups
* measurement_types
* tags
* grade
* courses
* period
* track
* goaltype
* action_step_options
* files
* videos
* observation_tag_1
* observation_tag_2
* observation_tag_3
* observation_tag_4
* observation_type
* observation_modules
* observation_label
* collaboration_type
* plu_event_location
* plu_event_type
* plu_series
* plu_content_area
* video_type
* meeting_tag_1
* meeting_tag_2'
* meeting_tag_3
* meeting_tag_4
* goal_options
* user_tag_1
* user_tag_2
* user_tag_3
* user_tag_4
* user_tag_5
* user_tag_6
* user_tag_7
* user_tag_8
* rubric_tag_1
* rubric_tag_2
* rubric_tag_3
* rubric_tag_4
* event_tag_1

### Example Usage
```python
from whetstone import Authenticate, HTTPRequest

# 1. Get access token using
api_key = '123ThisIsSuperSecret'
access_token = whetstone.Authenticate(api_key)

# 2. Set parameters for get request using Unix time for dates
meeting_parameters = {
                        'created': '1514764800',
                        'lastModified': '1514764800',
                        'archivedAt': '1531872000'
                      }

# 3. Get data from Whetstone API and output JSON object(s)
meetings = whetstone.HTTPRequest('meetings', access_token, meeting_parameters).GetData()
```

## Whetstone Documentation
View example scripts and request on Whetstone's github below
<https://github.com/WhetstoneEducation/API>
