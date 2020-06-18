import os
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

    def import_all(self):
        data = self.get_all()
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


class Users(Whetstone):
    def __init__(self, sql):
        super().__init__(sql)
        self.columns = [
            "id",
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
        ]
        self.dates = ["created", "lastActivity", "lastModified"]

    def _preprocess_records(self, records):
        for record in records:
            record["id"] = record.get("_id")
            record["school"] = record.get("defaultInformation").get("school")
            record["course"] = record.get("defaultInformation").get("course")
        return records
