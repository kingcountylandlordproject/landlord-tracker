#!/usr/bin/python3
#
# create a .zip file containing all the files in the manifest

import argparse
import csv
import os

from sqlalchemy import create_engine

from engels.load import create_data_package


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-a", "--all", help="include all files in the package", action="store_true"
    )
    parser.add_argument(
        "-p",
        "--include-preprocessed",
        help="include preprocessed files",
        action="store_true",
    )
    args = parser.parse_args()
    create_data_package(all=args.all, include_preprocessed=args.include_preprocessed)


if __name__ == "__main__":
    main()
