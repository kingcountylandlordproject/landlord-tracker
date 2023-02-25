from .common import normalize_address_in_file, NormalizeAddressParams


if __name__ == "__main__":
    print("Preprocessing, creating pre_apartment_complex_address.csv")
    params = NormalizeAddressParams(
        source_path="raw/kcda/2023_02_17/EXTR_AptComplex.csv",
        target_path="preprocessed/kcda/2023_02_17/pre_apartment_complex_address.csv",
        key_fields=["Major", "Minor", "Address"],
        address_field="Address",
        address_normalized_field="Address_Normalized",
    )
    normalize_address_in_file(params)
