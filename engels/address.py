# address normalization 

from scourgify import normalize_address_record

def normalize(addr: str):
    try:
        d = normalize_address_record(addr)
    except Exception as e:
        return None
    pieces = [piece for piece in [d['address_line_1'], d['address_line_2'], d['postal_code']] if piece]
    return ",".join(pieces)
