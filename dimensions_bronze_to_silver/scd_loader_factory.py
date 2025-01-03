# scd_loader_factory.py
from dimensions_bronze_to_silver.scd_loader import SCD2Loader, SCD1Loader, OverwriteLoader, SCDLoader


class SCDLoaderFactory:
    @staticmethod
    def get_loader(scd_type: str) -> SCDLoader:
        if scd_type == 'scd2':
            return SCD2Loader()
        elif scd_type == 'scd1':
            return SCD1Loader()
        else:
            return OverwriteLoader()  # For other types, you may need to implement logic for Overwrite
