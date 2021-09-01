#!/usr/bin/env python

from setuptools import setup

setup(name='tap-gcs-csv',
      version='0.0.1',
      description='Singer.io tap for extracting CSV files from Google Cloud Storage',
      author='FIXD Automotive',
      url='http://www.fixdapp.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_gcs_csv'],
      install_requires=[
          'singer-python>=5.12.1',
          'voluptuous==0.10.5',
          'google-cloud-storage>=1.42.0',
          'xlrd==1.0.0',
          'inflection>=0.5.1'
      ],
      entry_points='''
          [console_scripts]
          tap-gcs-csv=tap_gcs_csv:main
      ''',
      packages=['tap_gcs_csv'])
