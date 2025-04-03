from setuptools import find_packages, setup

setup(
    name="dagster",
    packages=find_packages(include=["flowbyte_app", "flowbyte_app.*", "modules", "modules.*", "governance", "governance.*"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
    data_files=[
        ('.', ['dagster.yaml', 'workspace.yaml']),
    ],
)
