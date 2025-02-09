# Extract & Load

This folder contains code for making API calls to PokeAPI and loading the data into a duckdb database.

For now, I am only pulling a small number of resources. I will grab more if I decide I need it.

The only endpoint that will require any code additions is the `pokemon/{pokemon_id}/encounters` endpoint, as its path follows a different pattern than all the others. However, it's possible that the data from this endpoint is already available in a different endpoing that `PokeApiClient` can already parse.