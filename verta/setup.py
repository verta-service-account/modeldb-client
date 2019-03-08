from setuptools import setup


with open("README.md", 'r') as f:
    long_description = f.read()

setup(
    name="verta",
    version="0.6.2",
    maintainer="Michael Liu",
    maintainer_email="miliu@verta.ai",
    description="Python client for interfacing with ModelDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://www.verta.ai/",
    packages=[
        "verta",
        "verta._protos.public.modeldb",
    ],
    install_requires=[
        "googleapis-common-protos==1.5.6",
        "grpcio==1.17.1",
        "protobuf==3.6.1",
        "requests==2.21.0",
    ],
)
