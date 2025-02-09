import duckdb
import os
import requests
import yaml
from utils import current_timestamp


class PokeApiClient:
    def __init__(
        self,
        duckdb_file_path: str,
        api_base_url: str,
        api_path_prefix: str,
        api_spec_url: str = None,
        api_spec_local_path: str = None,
        max_paginated_list_size: int = 1_000,
    ):
        if not api_spec_url and not api_spec_local_path:
            raise ValueError(
                "Must provide either api_spec_url or api_spec_local_path to init PokeApiClient"
            )
        self.open_api_spec: dict = self.get_api_spec(
            url=api_spec_url, file_path=api_spec_local_path
        )
        self.api_url: str = api_base_url + api_path_prefix
        self.base_url: str = api_base_url
        self.api_path_prefix: str = api_path_prefix
        self.duckdb_file_path: str = duckdb_file_path
        self.max_paginated_list_size: int = max_paginated_list_size

    def get_and_load_resource(self, resource_name: str) -> None:
        print(f"Getting and loading resource: {resource_name}")
        resource_id_list: list[int] = self._get_resource_id_list(
            resource_name=resource_name
        )

        with duckdb.connect(self.duckdb_file_path) as conn:
            conn.execute("create schema if not exists raw_api_data")
            conn.execute("use raw_api_data")
            conn.execute(
                f"""
            create or replace table raw_{resource_name} (
                id smallint,
                raw_data json,
                _requested_at_utc timestamp_ns,
                _loaded_at_utc timestamp_ns,
                _request_url varchar
            )"""
            )

            print(f"Created raw_{resource_name} table. Retrieving and loading individual records now...")

            for resource_id in resource_id_list:
                requested_at_utc = current_timestamp()
                request_url: str = f"{self.api_url}{resource_name}/{resource_id}/"
                res: requests.Response = requests.get(url=request_url)
                if res.status_code != 200:
                    res.raise_for_status()
                else:
                    loaded_at_utc = current_timestamp()
                    conn.execute(
                        f"""
                        insert into raw_{resource_name} (id, raw_data, _requested_at_utc, _loaded_at_utc, _request_url)
                        values (?, ?, ?, ?, ?
                    )""",
                        (
                            resource_id,
                            res.text,
                            requested_at_utc,
                            loaded_at_utc,
                            request_url,
                        ),
                    )
            
        print(f"Successfully loaded {len(resource_id_list):,} records into raw_{resource_name}!")

    def get_api_spec(self, url: str, file_path: str) -> dict:
        if not os.path.isfile(file_path):
            file_output_dir: str = os.path.dirname(file_path)
            if not os.path.exists(file_output_dir):
                os.makedirs(file_output_dir)

            response = requests.get(url=url)
            if response.status_code != 200:
                response.raise_for_status()
            else:
                response.encoding = "utf-8"
                with open(file=file_path, mode="w") as f:
                    f.write(response.text)

        return self._load_local_api_spec(file_path=file_path)

    def _load_local_api_spec(self, file_path: str) -> dict:
        with open(file=file_path, mode="r") as f:
            api_spec: dict = yaml.safe_load(f)

        return api_spec

    def _get_resource_id_list(
        self, resource_name: str, limit: int = None, offset: int = 0, announce_record_count: bool = True,
    ) -> list[int]:
        if not limit:
            limit = self.max_paginated_list_size
        resource_list_url: str = self.api_url + resource_name + "/"
        request_params: dict[str, int] = {"limit": limit, "offset": offset}
        res: requests.Response = requests.get(
            url=resource_list_url, params=request_params
        )
        if res.status_code != 200:
            res.raise_for_status()

        res_json: dict = res.json()
        if announce_record_count:
            print(f"This resource has {res_json['count']:,} records.")
        results: list[dict[str, str]] = res_json["results"]
        resource_id_list: list[int] = []

        for result in results:
            resource_id_list.append(
                int(result["url"].split("/")[-2])
            )  # Takes the ID from the URL

        print(f"Retrieved {len(resource_id_list):,} resource ID's.")

        # Recursively create and get from next URL if it exists
        if res_json.get("next", None):
            next_offset: str = (
                res_json["next"]
                .split("?")[1]
                .replace("offset=", "")
                .replace(f"limit={limit}", "")
            )
            print("Additional resource ID's remain. Progressing to next URL.")
            resource_id_list.extend(
                self._get_resource_id_list(
                    resource_name=resource_name, limit=limit, offset=next_offset, announce_record_count=False,
                )
            )

        return resource_id_list
