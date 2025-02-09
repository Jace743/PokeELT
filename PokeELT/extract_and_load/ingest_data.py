from os import path
from PokeELT.extract_and_load.poke_api_client import PokeApiClient
from time import perf_counter


OPEN_API_SPEC_URL: str = "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/openapi.yml"
DEFAULT_SPEC_LOCAL_PATH: str = path.join(path.dirname(__file__), "../openapi.yml")
API_BASE_URL: str = "https://pokeapi.co"
API_PATH_PREFIX: str = '/api/v2/'
DUCKDB_FILE_PATH: str = "pokemon_data_ingest.duckdb"
MAX_PAGINATED_LIST_SIZE: int = 1_000
# The website has A LOT of data. Can always go get more if we need it.
RESOURCES_OF_INTEREST: list[str] = ['pokemon', 'move', 'ability',]


if __name__=='__main__':
    client: PokeApiClient = PokeApiClient(
        duckdb_file_path=DUCKDB_FILE_PATH,
        api_base_url=API_BASE_URL,
        api_path_prefix=API_PATH_PREFIX,
        api_spec_url=OPEN_API_SPEC_URL,
        api_spec_local_path=DEFAULT_SPEC_LOCAL_PATH,
        max_paginated_list_size=MAX_PAGINATED_LIST_SIZE,
    )

    for resource in RESOURCES_OF_INTEREST:
        start = perf_counter()
        client.get_and_load_resource(resource_name=resource)
        end = perf_counter()
        print(f"Finished loading raw_{resource} table after {end - start:.4f} seconds.")
