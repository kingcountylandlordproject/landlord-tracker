import os
import sys

from .common import normalize_address_in_file, NormalizeAddressParams, get_data_path


if __name__ == "__main__":
    data_path = get_data_path()
    source = os.path.join(data_path, "raw/wasos/2022_10_17/Corporations.txt")
    target = os.path.join(data_path, "preprocessed/wasos/2022_10_17/Corporations-patched.txt")
    print("Preprocessing, creating Corporations-patched.txt")
    os.makedirs(os.path.dirname(target), exist_ok=True)

    with open(source) as f:
        with open(target, "w") as output:
            column_names = []
            line_num = 1
            ignored_count = 0
            for line in f:
                pieces = line.split("\t")
                if line_num == 1:
                    length = len(pieces)
                if length == len(pieces):
                    output.write(line.strip("\n").strip("\r"))
                    output.write("\n")
                else:
                    ignored_count += 1
                line_num += 1
            sys.stderr.write(f"dropped {ignored_count} lines from file\n")
