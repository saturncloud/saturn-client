import os
from setuptools import setup, find_packages

import versioneer

install_requires = ["requests"]


setup(
    name="saturn-client",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    maintainer="Saturn Cloud Developers",
    maintainer_email="dev@saturncloud.io",
    license="BSD-3-Clause",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    entry_points={
        "console_scripts": [
            "sc=saturn_client.cli.commands:entrypoint",
        ]
    },
    keywords="saturn cloud client library",
    description="Python library for interacting with Saturn Cloud API",
    long_description=(open("README.md").read() if os.path.exists("README.md") else ""),
    long_description_content_type="text/markdown",
    url="https://saturncloud.io/",
    project_urls={
        "Documentation": "http://docs.saturncloud.io",
        "Source": "https://github.com/saturncloud/saturn-client",
        "Issue Tracker": "https://github.com/saturncloud/saturn-client/issues",
    },
    packages=find_packages(),
    install_requires=install_requires,
    zip_safe=False,
)
