from setuptools import setup, find_packages

setup(
    name="verta",
    version="0.2.2",
    packages=find_packages(),
    install_requires=[
        "grpcio==1.17.1",
    ],
)
