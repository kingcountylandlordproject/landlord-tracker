from .common import normalize_address_in_file, NormalizeAddressParams


def extract_address(row):
    return " ".join([row[field] for field in ["AddrLine", "CityState", "ZipCode"] if row[field]])


if __name__ == "__main__":
    print("Preprocessing, creating pre_real_property_account_address.csv")
    params = NormalizeAddressParams(
        source_path="raw/kcda/2023_02_17/EXTR_RPAcct_NoName.csv",
        target_path="preprocessed/kcda/2023_02_17/pre_real_property_account_address.csv",
        key_fields=["Major", "Minor", "AddrLine", "CityState", "ZipCode"],
        address_field=extract_address,
        address_normalized_field="Address_Normalized",
    )
    normalize_address_in_file(params)
