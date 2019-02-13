# Installing `verta`
1. use Python3
1. cd into `${CLIENT_REPO}/verta/`
1. run `./fix-import.sh`
1. run `pip install .`
1. use the Python client with `from verta import ModelDBClient`

# Generating Data
1. install `verta`
1. instantiate backend
1. cd into `${CLIENT_REPO}/workflows/`
1. run `pip install -r requirements.txt`
1. cd into `${CLIENT_REPO}/workflows/datagen/`
1. edit scripts to enter user auth credentials
1. run scripts
