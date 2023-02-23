#!/usr/bin/python3

import csv
import os

from sqlalchemy import create_engine

from engels.load import load_all

def main():
    load_all()

if __name__ == '__main__':
    main()
