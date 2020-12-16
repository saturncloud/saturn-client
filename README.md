# saturn-client
Python library for interacting with [Saturn Cloud](https://www.saturncloud.io/) API.

## Connect

```python
from saturn import SaturnConnection

# From outside of Saturn
conn = SaturnConnection(url="<SATURN_URL>", api_token="<API_TOKEN>")

# From inside of Saturn
conn = SaturnConnection()
```

## Create a Project

```python
project = conn.create_project(
    name,
    description,
    image_uri,
    start_script,
    environment_variables,
    working_dir,
)
```
