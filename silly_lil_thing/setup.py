from setuptools import find_packages, setup

setup(
    name="silly_lil_thing",
    packages=find_packages(exclude=["silly_lil_thing_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
