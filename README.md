# saturn-client
Python library for interacting with [Saturn Cloud](https://www.saturncloud.io/) API.

## Connect
This library is intended primarily as a way to interact with Saturn Cloud from outside of the Saturn User Interface.

### External
To connect to Saturn you'll need the URL of your Saturn instance. It'll be something like: "https://app.community.saturnenterprise.io/". You'll also need your api token. You can get your token in the browser by logging in to saturn and then going to `/api/user/token`. So in the case of Saturn Cloud Hosted that'll be "https://app.community.saturnenterprise.io/api/user/token".

> Note that this library is new and experimental - we expect to make this process easier in the future.

```python
from saturn import SaturnConnection

# From outside of Saturn
conn = SaturnConnection(
    url="https://app.community.saturnenterprise.io/",
    api_token="fake_token_use_your_own"
)
```

### Internal
From inside of saturn on the other hand you can just run:

```python
from saturn import SaturnConnection

# From inside of Saturn
conn = SaturnConnection()
```

## Create a Project
Once you have your connection object (`conn`) you can create a project.

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
        "start_ssh": False,
    }
)
```
