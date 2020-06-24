import os
import json
import requests
import base64
import pandas as pd


class CredentialError(Exception):
    def __init__(self):
        self.message = "Authorization Failed: Verify CLIENT_ID and CLIENT_SECRET are set in the environment"
        super().__init__(self.message)


class Whetstone:
    def __init__(self, sql, qa=False):
        subdomain = "api-qa" if qa else "api"
        self.url = f"https://{subdomain}.whetstoneeducation.com"
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.token = self._authorize()
        self.endpoint = self.__class__.__name__
        self.filename = f"data/{self.endpoint}.json"
        self.sql = sql
        self.tag = False
        self.columns = []
        self.dates = [
            "archivedAt",
            "created",
            "date",
            "firstPublished",
            "lastActivity",
            "lastModified",
            "lastPublished",
            "observedAt",
        ]

    def _authorize(self):
        auth_url = f"{self.url}/auth/client/token"
        headers = {"Authorization": self._encode_credentials()}
        response = requests.post(auth_url, headers=headers)

        if response.status_code == 200:
            response_json = response.json()
            return response_json["access_token"]
        else:
            raise CredentialError

    def _encode_credentials(self):
        if self.client_id is None or self.client_secret is None:
            raise CredentialError
        client_credential_string = self.client_id + ":" + self.client_secret
        encoded_credentials = base64.b64encode(client_credential_string.encode("utf-8"))
        encoded_credentials_string = str(encoded_credentials, "utf-8")
        return "Basic " + encoded_credentials_string

    def get_all(self):
        if self.tag:
            endpoint_url = f"{self.url}/external/generic-tags/{self.endpoint}"
        else:
            endpoint_url = f"{self.url}/external/{self.endpoint}"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(endpoint_url, headers=headers)
        if response.status_code == 200:
            response_json = response.json()
            return response_json["data"]
        else:
            raise Exception(f"Failed to list {self.endpoint}")

    def _write_to_db(self, df, model):
        tablename = f"whetstone_{model}"
        self.sql.insert_into(tablename, df, chunksize=10000, if_exists="replace")

    def _write_to_json(self, data):
        if os.path.exists(self.filename):
            os.remove(self.filename)
        with open(self.filename, "w") as f:
            json.dump(data, f, indent=2)

    def transform_and_load(self):
        data = self.get_all()
        models = self._preprocess_records(data)
        for model, records in models.items():
            if records:
                df = pd.DataFrame(records)
                df = df.astype("object")
                df = self._convert_dates(df)
                df.rename(columns={"_id": "id"}, inplace=True)
                self._write_to_db(df, model)

    def _preprocess_records(self, records):
        return records

    def _convert_dates(self, df):
        date_types = {col: "datetime64[ns]" for col in df.columns if col in self.dates}
        if date_types:
            df = df.astype(date_types)
        return df


class Users(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "_id",
            "internalId",
            "activeDistrict",
            "archivedAt",
            "created",
            "email",
            "inactive",
            "lastActivity",
            "lastModified",
            "locked",
            "name",
            "first",
            "last",
            "school",
            "course",
            "coach",
        ]

    def _preprocess_records(self, records):
        models = {"Users": []}
        for record in records:
            record["school"] = record.get("defaultInformation").get("school")
            record["course"] = record.get("defaultInformation").get("course")
            user = {k: v for (k, v) in record.items() if k in self.columns}
            models["Users"].append(user)
        return models


class Schools(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "_id",
            "internalId",
            "name",
            "abbreviation",
            "archivedAt",
            "principal",
            "gradeSpan",
            "lowGrade",
            "highGrade",
            "district",
            "phone",
            "address",
            "city",
            "cluster",
            "region",
            "state",
            "zip",
            "lastModified",
        ]

    def _preprocess_records(self, records):
        models = {"Schools": [], "ObservationGroups": [], "ObservationGroupMembers": []}
        for record in records:
            record_id = record.get("_id")
            school = {k: v for (k, v) in record.items() if k in self.columns}
            models["Schools"].append(school)
            groups = record.get("observationGroups")
            if groups:
                for group in groups:
                    observers = self._extract_group_members(
                        group, "observers", record_id
                    )
                    models["ObservationGroupMembers"].extend(observers)
                    observees = self._extract_group_members(
                        group, "observees", record_id
                    )
                    models["ObservationGroupMembers"].extend(observees)

                groups = [
                    {
                        "id": item.get("_id"),
                        "name": item.get("name"),
                        "school": record_id,
                        "lastModified": item.get("lastModified"),
                    }
                    for item in groups
                ]
                models["ObservationGroups"].extend(groups)
        return models

    def _extract_group_members(self, group, role, record_id):
        members = [
            dict(
                item,
                school=record_id,
                role=role[:-1],
                observationGroup=group.get("_id"),
            )
            for item in group.get(role)
        ]
        return members


class Meetings(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "_id",
            "isWeeklyDataMeeting",
            "locked",
            "private",
            "signatureRequired",
            "course",
            "date",
            "grade",
            "school",
            "title",
            "type",
            "creator",
            "district",
            "created",
            "lastModified",
        ]

    def _preprocess_records(self, records):
        models = {
            "Meetings": [],
            "MeetingObservations": [],
            "MeetingParticipants": [],
            "MeetingAdditionalFields": [],
        }
        for record in records:
            record_id = record.get("_id")
            record["creator"] = record.get("creator").get("_id")
            meeting = {k: v for (k, v) in record.items() if k in self.columns}
            models["Meetings"].append(meeting)

            observations = record.get("observations")
            if observations:
                observations = [
                    dict(meeting=record_id, observation=observation)
                    for observation in observations
                ]
                models["MeetingObservations"].extend(observations)

            participants = record.get("participants")
            if participants:
                participants = [dict(item, meeting=record_id) for item in participants]
                models["MeetingParticipants"].extend(participants)

            additional_fields = record.get("additionalFields")
            if additional_fields:
                additional_fields = [
                    dict(item, meeting=record_id) for item in additional_fields
                ]
                models["MeetingAdditionalFields"].extend(additional_fields)

        return models


class Observations(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "_id",
            "observedAt",
            "observedUntil",
            "firstPublished",
            "lastPublished",
            "viewedByTeacher",
            "isPublished",
            "archivedAt",
            "requireSignature",
            "locked",
            "isPrivate",
            "signed",
            "observer",
            "rubric",
            "teacher",
            "district",
            "observationType",
            "observationModule",
            "observationtag1",
            "observationtag2",
            "observationtag3",
            "created",
            "lastModified",
            "quickHits",
            "score",
            "scoreAveragedByStrand",
        ]

    def _preprocess_records(self, records):
        models = {
            "Observations": [],
            "ObservationScores": [],
            "ObservationMagicNotes": [],
        }
        for record in records:
            record_id = record.get("_id")
            observation = {
                k: v["_id"] if type(v) is dict else v
                for (k, v) in record.items()
                if k in self.columns
            }
            models["Observations"].append(observation)
            scores = record.get("observationScores")

            if scores:
                scores = [
                    {
                        "observation": record_id,
                        "measurement": item.get("measurement"),
                        "measurementGroup": item.get("measurementGroup"),
                        "valueScore": item.get("valueScore"),
                        "valueText": item.get("valueText"),
                        "percentage": item.get("percentage"),
                        "lastModified": item.get("lastModified"),
                    }
                    for item in scores
                ]
                models["ObservationScores"].extend(scores)
            notes = record.get("magicNotes")
            if notes:
                notes = [dict(item, observation=record_id) for item in notes]
                models["ObservationMagicNotes"].extend(notes)
        return models


class Measurements(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "_id",
            "name",
            "description",
            "measurementType",
            "isPercentage",
            "created",
            "district",
            "scaleMin",
            "scaleMax",
            "lastModified",
            "rowStyle",
        ]

    def _preprocess_records(self, records):
        models = {"Measurements": [], "MeasurementOptions": []}
        for record in records:
            record_id = record.get("_id")
            measurement = {k: v for (k, v) in record.items() if k in self.columns}
            models["Measurements"].append(measurement)
            options = record.get("measurementOptions")
            if options:
                options = [dict(item, measurement=record_id) for item in options]
                models["MeasurementOptions"].extend(options)
        return models


class Assignments(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "_id",
            "excludeFromBank",
            "locked",
            "private",
            "coachingActivity",
            "creator",
            "user",
            "name",
            "type",
            "parent",
            "grade",
            "course",
            "created",
            "lastModified",
            "progress_percent",
            "progress_assigner",
            "progress_justification",
            "progress_date",
        ]

    def _preprocess_records(self, records):
        models = {"Assignments": [], "AssignmentTags": []}
        for record in records:
            record_id = record.get("_id")
            record["creator"] = record.get("creator").get("_id")
            record["user"] = record.get("user").get("_id")
            course = record.get("course") or {}
            record["course"] = course.get("_id")
            grade = record.get("grade") or {}
            record["grade"] = grade.get("_id")
            parent = record.get("parent") or {}
            record["parent"] = parent.get("_id")
            progress = record.get("progress")
            if progress:
                record["progress_percent"] = progress.get("percent")
                record["progress_assigner"] = progress.get("assigner")
                record["progress_justification"] = progress.get("justification")
                record["progress_date"] = progress.get("date")
            assignment = {k: v for (k, v) in record.items() if k in self.columns}
            models["Assignments"].append(assignment)

            tags = record.get("tags")
            if tags:
                tags = [dict(item, assignment=record_id) for item in tags]
                models["AssignmentTags"].extend(tags)
        return models


class Informals(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "_id",
            "shared",
            "private",
            "user",
            "creator",
            "district",
            "created",
            "lastModified",
        ]

    def _preprocess_records(self, records):
        models = {"Informals": [], "InformalTags": []}
        for record in records:
            record_id = record.get("_id")
            record["creator"] = record.get("creator").get("_id")
            record["user"] = record.get("user").get("_id")
            informal = {k: v for (k, v) in record.items() if k in self.columns}
            models["Informals"].append(informal)

            tags = record.get("tags")
            if tags:
                tags = [dict(item, assignment=record_id) for item in tags]
                models["InformalTags"].extend(tags)
        return models


class Rubrics(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "_id",
            "scaleMin",
            "scaleMax",
            "isPrivate",
            "name",
            "district",
            "created",
            "lastModified",
            "isPublished",
        ]

    def _preprocess_records(self, records):
        models = {
            "Rubrics": [],
            "RubricMeasurements": [],
            "RubricMeasurementGroups": [],
        }
        for record in records:
            record_id = record.get("_id")
            creator = record.get("creator") or {}
            record["creator"] = creator.get("_id")
            rubric = {k: v for (k, v) in record.items() if k in self.columns}
            models["Rubrics"].append(rubric)

        groups = record.get("measurementGroups")
        if groups:
            for group in groups:
                group_id = group.get("_id")
                measurements = group.get("measurements")
                if measurements:
                    measurements = [
                        dict(item, rubric=record_id, measurement_group=group_id)
                        for item in measurements
                    ]
                models["RubricMeasurements"].extend(measurements)
            groups = [
                dict(
                    id=item.get("_id"),
                    name=item.get("name"),
                    key=item.get("key"),
                    rubric=record_id,
                )
                for item in groups
            ]
            models["RubricMeasurementGroups"].extend(groups)
        return models


class Tag(Whetstone):
    def __init__(self, sql, tag_type):
        super().__init__(sql)
        self.columns = ["_id", "name", "district", "created", "lastModified"]
        self.tag = True
        self.modelname = self._snake_to_camel(tag_type)
        self.endpoint = tag_type.replace("_", "")

    def _preprocess_records(self, records):
        models = {self.modelname: []}
        for record in records:
            abbreviation = record.get("abbreviation")
            if abbreviation:
                record["abbreviation"] = abbreviation
                self.columns.append("abbreviation")
            model = {k: v for (k, v) in record.items() if k in self.columns}
            models[self.modelname].append(model)
        return models

    def _snake_to_camel(self, name):
        return "".join(word.title() for word in name.split("_"))

