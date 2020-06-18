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
    @classmethod
    def classname(cls):
        return cls.__name__

    def __init__(self, sql, qa=False):
        self.endpoint = self.classname()
        self.table_name = f"whetstone_{self.endpoint}"
        self.filename = f"data/{self.endpoint}.json"
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.columns = []
        self.dates = []
        self.tag = False
        self.sql = sql
        if qa:
            subdomain = "api-qa"
        else:
            subdomain = "api"
        self.url = f"https://{subdomain}.whetstoneeducation.com"
        self.token = self._authorize()

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

    def _write_to_db(self, df):
        self.sql.insert_into(self.table_name, df, chunksize=10000, if_exists="replace")

    def _write_to_json(self, data):
        if os.path.exists(self.filename):
            os.remove(self.filename)
        with open(self.filename, "w") as f:
            json.dump(data, f, indent=2)

    def import_all(self):
        data = self.get_all()
        self._write_to_json(data)
        self.additional_imports(data)
        df = self._process_and_filter_records(data)
        self._write_to_db(df)

    def _preprocess_records(self, records):
        return records

    def _process_and_filter_records(self, records):
        new_records = self._preprocess_records(records)
        df = pd.json_normalize(new_records)
        df = df.reindex(columns=self.columns)
        df = df.astype("object")
        df = self._convert_dates(df)
        return df

    def _convert_dates(self, df):
        if self.dates:
            date_types = {col: "datetime64[ns]" for col in self.dates}
            df = df.astype(date_types)
        return df

    def additional_imports(self, records):
        pass

    def extract_subrecords(self, subrecords, tablename):
        df = pd.DataFrame(subrecords)
        self.sql.insert_into(
            f"whetstone_{tablename}", df, chunksize=10000, if_exists="replace"
        )


class Users(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "id",
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
        self.dates = ["created", "lastActivity", "lastModified"]

    def _preprocess_records(self, records):
        for record in records:
            record["id"] = record.get("_id")
            record["school"] = record.get("defaultInformation").get("school")
            record["course"] = record.get("defaultInformation").get("course")
        return records


class Schools(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "id",
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
        self.dates = ["lastModified"]

    def _preprocess_records(self, records):
        for record in records:
            record["id"] = record.get("_id")
        return records

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

        group_df = pd.DataFrame(group_records)
        self._convert_dates(group_df)
        group_member_df = pd.DataFrame(group_member_records)
        self.sql.insert_into(
            "whetstone_ObservationGroups",
            group_df,
            chunksize=10000,
            if_exists="replace",
        )
        self.sql.insert_into(
            "whetstone_ObservationGroupMembers",
            group_member_df,
            chunksize=10000,
            if_exists="replace",
        )


class Meetings(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "id",
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
        self.dates = ["created", "date", "lastModified"]

    def _preprocess_records(self, records):
        for record in records:
            record["id"] = record.get("_id")
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

