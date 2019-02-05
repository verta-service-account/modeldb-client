from setuptools import setup, find_packages

setup(
    name="verta",
    version="0.4.0",
    packages=find_packages(),
    install_requires=[
        "grpcio==1.17.1",
        "protobuf==3.6.1",
        "requests==2.21.0",
    ],
)
