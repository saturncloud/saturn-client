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
    name="my-project",
    description="My new project - created from outside of Saturn programatically!",
    image_uri="saturncloud/saturn-gpu:2020.11.30",
    start_script="pip install git+https://github.com/saturncloud/dask-saturn.git@main",
    environment_variables={"DATA_URL": "s3://my-bucket/data"},
    working_dir="/home/jovyan/project",
    jupyter_size="large",
    jupyter_disk_space="50Gi",
    jupyter_auto_shutoff="Never",
    jupyter_start_ssh=False,
)
```

## Other project methods

### List all projects
Get a list of all the projects that you have access to.

```python
conn.list_projects()
```

### Get a project
Get the details of a particular project by ID.

```python
project = conn.get_project("18ad47c81c5943ad9ae641b11367d1b1")
```

### Update a project
Update a particular project by ID. Any field that can be used in `create_project`
can also be used in `update_project` **except for name**.

```python
project = conn.update_project("18ad47c81c5943ad9ae641b11367d1b1", image="saturncloud/saturn:2020.12.11")
```

Use the `update_jupyter_server` option to keep the jupyter_server uptodate with the project - this is set to True by default:

> NOTE: If the jupyter server is running for this project it will be stopped and so will the dask cluster associated with that jupyter. You can start these back up using the `start_jupyter_server` method and the `start_dask_cluster` method.

### Delete a project
Delete a particular project by ID.

```python
conn.delete_project("18ad47c81c5943ad9ae641b11367d1b1")
```

## Jupyter server methods
Jupyter server methods act directly on the jupyter server and require the jupyter_server_id. This can be found on the response from any of the project methods.

### Get a jupyter server
Get the details of a particular jupyter server by ID.

```python
jupyter_server = conn.get_jupyter_server("acb4588d062d4d0ba0680a4d49c72cf8")
```

### Start a jupyter server
Start a particular jupyter server by ID. This method will return as soon as the start process has been triggered. It'll take longer for the jupyter server to be up, but you can check the status using `get_jupyter_server`.

```python
conn.start_jupyter_server("acb4588d062d4d0ba0680a4d49c72cf8")
```

### Stop a jupyter server
Stop a particular jupyter server by ID. This method will return as soon as the stop process has been triggered. It'll take longer for the jupyter server to actually shut down, but you can check the status using `get_jupyter_server`.

```python
conn.stop_jupyter_server("acb4588d062d4d0ba0680a4d49c72cf8")
```

## Dask cluster methods
Dask cluster methods act directly on the dask cluster and require the dask_cluster_id. This can be found on the response from `get_jupyter_server`.

### Start a dask cluster
Start a dask cluster by ID. This method will return as soon as the start process has been triggered. It'll take longer for the  dask cluster to be up. This is primarily useful when the dask cluster has been stopped as a side-effect of stopping a jupyter server or updating a project. For more fine-grain control over the dask cluster see [dask-saturn](https://github.com/saturncloud/dask-saturn).

```python
conn.start_dask_cluster("e59862cbde6647e09ec1202c21b8947a")
```

### Stop a dask cluster
Stop a particular dask cluster by ID. This method will return as soon as the stop process has been triggered. It'll take longer for the dask cluster to actually shut down.

```python
conn.stop_dask_cluster("e59862cbde6647e09ec1202c21b8947a")
```

## Miscelaneous
Some convenience properties are included on `SaturnConnection`. These include `conn.options` which describes the options available for workspace settings.
