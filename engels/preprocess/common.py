import csv
from dataclasses import dataclass
from functools import partial
from multiprocessing import Pool
import os
import os.path
from typing import Callable, List, Union

from ..common import get_data_path
from ..address import normalize


@dataclass
class NormalizeAddressParams:
    source_path: str
    target_path: str
    key_fields: List[str]
    address_field: Union[str, Callable]
    address_normalized_field: str


def normalize_address_for_row(params: NormalizeAddressParams, row):
    output_row = [row[k] for k in (params.key_fields)]
    if callable(params.address_field):
        address = params.address_field(row)
    else:
        address = row[params.address_field]
    output_row.append(normalize(address))
    return output_row


def normalize_address_in_file(params: NormalizeAddressParams):
    data_path = get_data_path()

    source = os.path.join(data_path, params.source_path)
    target = os.path.join(data_path, params.target_path)

    os.makedirs(os.path.dirname(target), exist_ok=True)

    with open(target, "w") as output_file:
        with open(source) as input_file:
            reader = csv.DictReader(input_file)
            writer = csv.writer(output_file)

            writer.writerow([*params.key_fields, params.address_normalized_field])

            with Pool() as p:                
                for output_row in p.imap(partial(normalize_address_for_row, params), reader, 10000):
                    writer.writerow(output_row)
