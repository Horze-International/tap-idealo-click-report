#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name="tap-idealo-click-report",
      version="0.1.1",
      description="Singer.io tap for extracting data from the Zendesk Chat API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_idealo_click_report"],
      install_requires=[
          "singer-python==5.9.1",
          "requests==2.20.0",
      ],
      entry_points="""
          [console_scripts]
          tap-idealo-click-report=tap_idealo_click_report:main
      """,
      packages=["tap_idealo_click_report"],
      package_data = {
          "schemas": ["tap_idealo_click_report/schemas/*.json"]
      },
      include_package_data=True,
)
