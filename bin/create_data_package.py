#!/usr/bin/python3
#
# create a .zip file containing all the files in the manifest

import csv
import os

from sqlalchemy import create_engine

from engels.load import create_data_package

def main():
    create_data_package()

if __name__ == '__main__':
    main()
