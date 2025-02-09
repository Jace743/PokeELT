# PokeELT
Extracts data about Pokemon from a public API (thank you, [PokeAPI!](https://pokeapi.co/)) and stores it in a `duckdb` database. Currently, an early version of the API client (`PokeApiClient`) is available (see `ingest_data.py` for an example usage).

If you'd like to run the client yourself, install the `PokeELT` package from the root of this repo:

`python3 -m pip install -e .`

... and try running `ingest_data.py`. As a heads up, it will create a .duckdb database, and it'll be 200-300 MB once the full script runs (assuming you download only the three resources that are currently targeted in that script).

Future Plans:

- Data modeling using `dbt`
- Replication of modeled data marts to a SQLite database
- Creating a Pokedex (dashboard) using `streamlit`

Data for this project is sourced from https://pokeapi.co/.