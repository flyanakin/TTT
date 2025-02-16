from setuptools import find_packages, setup

setup(
    name="TTT",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "pandas",
        "matplotlib",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
