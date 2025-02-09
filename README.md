# PokeELT
Extracts data about Pokemon from a public API (thank you, [PokeAPI!](https://pokeapi.co/)) and stores it in a `duckdb` database. Currently, an early version of the API client (`PokeApiClient`) is available (see `ingest_data.py` for an example usage).

Future Plans:

- Data modeling using `dbt`
- Replication of modeled data marts to a SQLite database
- Creating a Pokedex (dashboard) using `streamlit`

Data for this project is sourced from https://pokeapi.co/.