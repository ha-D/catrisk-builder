# -*- coding: utf-8 -*-

from keys_server import CatrisksBaseKeysLookup 

from oasislmf.utils.log import oasis_log

import os

model_id = os.environ["OASIS_MODEL_ID"]
if not model_id:
    raise ValueError("Missing OASIS_MODEL_ID in environment variables")

class ModelKeysLookup(CatrisksBaseKeysLookup):
    """
    CatRisk CRSEQ model keys lookup logic - at present the CRSEQ lookup logic
    is identical to that of the Catrisks generic keys lookup,.
    """

    @oasis_log()
    def __init__(self, keys_data_directory=None, supplier='Catrisks', model_name=model_id, model_version=None,
                    complex_lookup_config_fp=None, output_directory=None):
        super(self.__class__, self).__init__(keys_data_directory, supplier, model_name, model_version)


globals()[f"{model_id}KeysLookup"] = ModelKeysLookup

__all__ = [
    f"{model_id}KeysLookup",
]