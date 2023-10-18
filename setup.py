#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(
    name="decs",
    version="0.1.0",
    description="Durer distributed Entity Component System implementation",
    author="Alexey Pankov",
    author_email="alexey@exactly.ai",
    url="https://exactly.ai/",
    packages=find_packages(exclude=["tests", "tests.*"]),
)
