#!/usr/bin/env python

from setuptools import setup

setup(name='tap-auth0',
      version='0.1.0',
      description='Singer.io tap for extracting data from the Auth0 Management API v2',
      author='Adilson Mendonca based on Jean-Nicholas Hould',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_auth0'],
      install_requires=[
          'auth0-python==3.9.1',
          'singer-python==2.1.4'
      ],
      entry_points='''
          [console_scripts]
          tap-auth0=tap_auth0:main
      ''',
      packages=['tap_auth0'],
      package_data = {
          'tap_auth0': ["schemas/*.json"],
      },
      include_package_data=True,
)