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

The minimal acceptable input is the name of the project:

```python
project = conn.create_project(name="my-project")
```

But all the other settings that are available in the UI can also be passed in:

```python
project = conn.create_project(
    name="my-project"
    description="My new project - created from outside of Saturn programatically!",
    image_uri="saturncloud/saturn-gpu:2020.11.30",
    start_script="pip install git+https://github.com/saturncloud/dask-saturn.git@main",
    environment_variables={"DATA_URL": "s3://my-bucket/data"},
    working_dir="/home/jovyan/project",
    workspace_settings={
        "size": "large",
        "auto_shutoff": "Never",
        "start_ssh": True,
    }
)
```
