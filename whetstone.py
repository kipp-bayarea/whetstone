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
        for record in records:
            record["school"] = record.get("defaultInformation").get("school")
            record["course"] = record.get("defaultInformation").get("course")
        return records


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

    def additional_imports(self, records):
        group_records = []
        group_member_records = []
        for record in records:
            school = record["_id"]
            observationGroups = record.get("observationGroups")
            if observationGroups:
                for group in observationGroups:
                    record = {
                        "id": group.get("_id"),
                        "name": group.get("name"),
                        "school": school,
                        "lastModified": group.get("lastModified"),
                    }
                    group_records.append(record)

                    observers = group.get("observers")
                    observees = group.get("observees")

                    if observers:
                        for member in observers:
                            member_record = {
                                "id": member.get("_id"),
                                "name": member.get("name"),
                                "role": "observer",
                                "observationGroup": group.get("_id"),
                                "school": school,
                            }
                            group_member_records.append(member_record)

                    if observees:
                        for member in observees:
                            member_record = {
                                "id": member.get("_id"),
                                "name": member.get("name"),
                                "role": "observee",
                                "observationGroup": group.get("_id"),
                                "school": school,
                            }
                            group_member_records.append(member_record)

        self.extract_subrecords(group_records, "ObservationGroups")
        self.extract_subrecords(group_member_records, "ObservationGroupMembers")


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
        for record in records:
            record["creator"] = record.get("creator").get("_id")
        return records

    def additional_imports(self, records):
        observation_records = []
        participant_records = []
        additional_field_records = []
        for record in records:
            observations = record.get("observations")
            if observations:
                for observation in observations:
                    observation_record = {
                        "meeting": record.get("_id"),
                        "observation": observation,
                    }
                    observation_records.append(observation_record)

            participants = record.get("participants")
            if participants:
                for participant in participants:
                    participant["meeting"] = record.get("_id")
                    participant_records.append(participant)

            additional_fields = record.get("additionalFields")
            if additional_fields:
                for field in additional_fields:
                    field["meeting"] = record.get("_id")
                    additional_field_records.append(field)

        self.extract_subrecords(observation_records, "MeetingObservations")
        self.extract_subrecords(participant_records, "MeetingParticipants")
        self.extract_subrecords(additional_field_records, "MeetingAdditionalFields")


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

