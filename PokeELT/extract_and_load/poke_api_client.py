import duckdb
import os
import requests
import yaml
from tqdm import tqdm
from utils import current_timestamp_utc


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
                "Must provide either api_spec_url or api_spec_local_path to initialize PokeApiClient"
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
        """Executes all necessary get requests to extract all records for a given resource and loads the raw json data into
        the duckdb database, along with some additional metadata (request and load timestamps, record source URL).

        Args:
            resource_name (str): The name of the resource to be extracted (e.g., 'pokemon').

        """
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

            for resource_id in tqdm(resource_id_list):
                requested_at_utc = current_timestamp_utc()
                request_url: str = f"{self.api_url}{resource_name}/{resource_id}/"
                res: requests.Response = requests.get(url=request_url)
                if res.status_code != 200:
                    res.raise_for_status()
                else:
                    loaded_at_utc = current_timestamp_utc()
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
        """Retrieve's an already-existing local API spec if it's available, and pulls it from the PokeAPI GitHub repo if it isn't.

        Args:
            url (str): URL to the Open API spec in the PokeAPI GitHub repo.
            file_path (str): Local path to the Open API spec that may have already been downloaded.

        Returns:
            dict: Open API spec for PokeAPI.
        """
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
        """Helper function that loads a local Open API spec if it exists.

        Args:
            file_path (str): Path to API spec.

        Returns:
            dict: The API spec.
        """
        with open(file=file_path, mode="r") as f:
            api_spec: dict = yaml.safe_load(f)

        return api_spec

    def _get_resource_id_list(
        self, resource_name: str, limit: int = None, offset: int = 0, announce_record_count: bool = True,
    ) -> list[int]:
        """Retrieves a list of all resource ID values for a resource. This is necessary, because the ID's
        don't increment in a consistent manner.

        Args:
            resource_name (str): Name of the resource for which to pull ID's.
            limit (int, optional): Maximum number of records to contain in a list pagination, passed as a request url parameter. Defaults to None.
            offset (int, optional): Starting point for subsequent pages, if needed; passed in a request url parameter. Defaults to 0.
            announce_record_count (bool, optional): Whether to print additional info during debugging. Will be removed for proper logs in a future version. Defaults to True.

        Returns:
            list[int]: List of ID's for this resource.
        """
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
