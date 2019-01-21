from setuptools import setup, find_packages

setup(
    name="verta",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "grpcio==1.17.1",
    ],
)
