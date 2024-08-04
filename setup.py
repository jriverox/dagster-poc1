from setuptools import find_packages, setup

setup(
    name="dagster_mongodb_report",
    packages=find_packages(exclude=["dagster_mongodb_report_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "pandas",
        "pymongo",
        "openpyxl"

    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
