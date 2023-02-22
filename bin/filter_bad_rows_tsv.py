#!/usr/bin/python3
#
# drop lines that don't have the correct number of values
#
# this was written for Corporations.txt from WA SOS

import sys

def main():
    with open(sys.argv[1]) as f:
        column_names = []
        line_num = 1
        ignored_count = 0
        for line in f:
            pieces = line.split("\t")
            if line_num == 1:
                length = len(pieces)
            if length == len(pieces):
                print(line.strip("\n").strip("\r"))
            else:
                ignored_count += 1
            line_num += 1
        sys.stderr.write(f"dropped {ignored_count} lines from file\n")

if __name__ == '__main__':
    main()
