import requests
import json
import os
import sys

class Authenticate(object):
    def __init__(self, apikey):
        self.apikey = apikey
        self.authurl = 'https://app.whetstoneeducation.com/auth/api'

    def GetToken(self):
        """
        Parse response token from Whetstone server to make API calls.
        """
        res = requests.post(self.authurl, data={"apikey": self.apikey})
        response = json.loads(res.text)

        try:
            token = response['token']
            return token
        except KeyError:
            print("Invalid API key\n\nKey:", self.apikey, "\nServer Response:", response, "\n")
            sys.exit(1)

class HTTPRequest(object):
    def __init__(self, object, token, apikey, parameters = {}):
        self.token = token
        self.parameters = parameters
        self.object = object
        self.apikey = apikey
        self.url = 'https://app.whetstoneeducation.com'
        self.endpoints = {'users': '/api/v2/users',
                          'observations': '/api/v2/observations',
                          'meetings': '/api/v2/meetings',
                          'meeting_modules': '/api/v2/meetingmodules',
                          'schools': '/api/v2/schools',
                          'assignments': '/api/v2/assignments',
                          'scores': '/api/v2/scores',
                          'informals': '/api/v2/informals',
                          'rubrics': '/api/v2/rubrics',
                          'measurements': '/api/v2/measurements',
                          'measurement_groups': '/api/v2/measurementGroups',
                          'measurement_types': '/api/v2/measurementTypes',
                          'tags': '/api/v2/tags',
                          'grade': '/api/v2/grades',
                          'courses': '/api/v2/courses',
                          'period': '/api/v2/periods',
                          'track': '/api/v2/tracks',
                          'goaltype': '/api/v2/goalTypes',
                          'action_step_options': '/api/v2/actionstepopts',
                          'files': '/api/v2/files',
                          'videos': '/api/v2/videos',
                          'observation_tag_1': '/api/v2/observationtag1s',
                          'observation_tag_2': '/api/v2/observationtag2s',
                          'observation_tag_3': '/api/v2/observationtag3s',
                          'observation_tag_4': '/api/v2/observationtag4s',
                          'observation_type': '/api/v2/observationTypes',
                          'observation_modules': '/api/v2/observationModules',
                          'observation_label': '/api/v2/observationLabels',
                          'collaboration_type': '/api/v2/collaborationtypes',
                          'plu_event_location': '/api/v2/plueventlocations',
                          'plu_event_type': '/api/v2/plueventtypes',
                          'plu_series': '/api/v2/pluseries',
                          'plu_content_area': '/api/v2/plucontentareas',
                          'video_type': '/api/v2/videotypes',
                          'meeting_tag_1': '/api/v2/meetingtag1s',
                          'meeting_tag_2': '/api/v2/meetingtag2s',
                          'meeting_tag_3': '/api/v2/meetingtag3s',
                          'meeting_tag_4': '/api/v2/meetingtag4s',
                          'goal_options': '/api/v2/goalopts',
                          'user_tag_1': '/api/v2/usertag1s',
                          'user_tag_2': '/api/v2/usertag2s',
                          'user_tag_3': '/api/v2/usertag3s',
                          'user_tag_4': '/api/v2/usertag4s',
                          'user_tag_5': '/api/v2/usertag5s',
                          'user_tag_6': '/api/v2/usertag6s',
                          'user_tag_7': '/api/v2/usertag7s',
                          'user_tag_8': '/api/v2/usertag8s',
                          'rubric_tag_1': '/api/v2/rubrictag1s',
                          'rubric_tag_2': '/api/v2/rubrictag2s',
                          'rubric_tag_3': '/api/v2/rubrictag3s',
                          'rubric_tag_4': '/api/v2/rubrictag4s',
                          'event_tag_1': '/api/v2/eventtag1s'}
        self.endpoint = self.endpoints[object]

    def GetData(self):
        # Use requests to get data from Whetstone API
        try:
            endpointurl = self.url + self.endpoint
            payload = {}
            params = self.parameters
            headers = {'content-type': 'application/json',
                       'x-access-token': self.token,
                       'x-key': self.apikey}

            # make request
            response = requests.get(endpointurl, data=payload, headers=headers, params=params)

        except ConnectionError:
            print("Error: endpoint does not exist\n")
            sys.exit(1)

        # Format the json object
        try:
            data = json.loads(response.text)
            output = []
            output.append({'object': self.object, 'rowcount': len(data), 'records': []})

            sub = {}

            for row in data:  # Iterate through each row in the JSON response
                r = 0
                s = {} # Create empty list

                for c in row.keys():  # Iterate through each column in the response dictionary keys
                    if type(row[c]) is dict:  # These are the columns that can be flattened to the parent level
                        for key, value in row[c].items():
                            if type(value) is dict:
                                for subkey, subvalue in value.items():
                                    s[c+'_'+key+'_'+subkey] = subvalue
                            if type(value) is str:
                                s[c+'_'+key] = value

                    if type(row[c]) is str:  # These are the columns with key:value pair at the parent level
                        s[c] = row[c]

                    # Nested object are not currently coming out of the export

                output[0]['records'].append(s)

            return output
        except:
            print (sys.exc_info())
            sys.exit(1)
