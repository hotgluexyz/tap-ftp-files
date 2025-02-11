#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-ftp-files',
    version='0.0.1',
    description='hotglue tap for importing files from FTP',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_ftp_files'],
    install_requires=[
        'argparse==1.4.0',
        'pytz==2025.1'
    ],
    entry_points='''
        [console_scripts]
        tap-ftp-files=tap_ftp_files:main
    ''',
    packages=['tap_ftp_files']
)
