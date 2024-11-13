__all__ = [
    'CatrisksBaseKeysLookup'
]

_CATRISKS_COVERAGE_CODE = {
    1: {'coverage_type': 'Building', 'coverage_code': "B"},
    2: {'coverage_type': 'Other', 'coverage_code': "O"},
    3: {'coverage_type': 'Content', 'coverage_code': "C"},
    4: {'coverage_type': 'BI', 'coverage_code': "I"}
}

import csv
import io
import logging
import os
import math
import pandas as pd
import sys
import sqlite3
from itertools import islice
import zipfile

from oasislmf.model_preparation.lookup import (
    OasisBaseKeysLookup,
    # UNKNOWN_ID,
)
from .values import (
    to_float,
    to_int,
    to_string,
)
UNKNOWN_ID = -1

# from .read_csv import read_csv
from oasislmf.utils.log import oasis_log
from oasislmf.utils.status import OASIS_KEYS_FL


class CatrisksBaseKeysLookup(OasisBaseKeysLookup):
    """
    CatRisks base model keys lookup.
    """
    def __init__(self, keys_data_directory=None, supplier='Catrisks', model_name=None, model_version=None,
                 complex_lookup_config_fp=None, output_directory=None):
        """
        Initialise the static data required for the lookup.
        """
        super(CatrisksBaseKeysLookup, self).__init__(
            keys_data_directory,
            supplier,
            model_name,
            model_version,
            complex_lookup_config_fp,
            output_directory
        )

        self.vulnerability_index = {}
        self.construction_class_index = {}
        self.occupancy_scheme_index = {}
        self.occupancy_list=[]
        self.construction_list=[]


        filename = os.path.join(self.keys_data_directory, 'crseq_keysdata.dat')
        filelist = zipfile.ZipFile(filename).namelist()
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            with zip_ref.open(filelist[0], pwd=b'63297032190') as file:
                keyfiles_xlsx = io.BytesIO(file.read())

        Vul_dic = pd.ExcelFile(keyfiles_xlsx).parse(sheet_name='DICTVULNERABILITY')
        Const_class = pd.ExcelFile(keyfiles_xlsx).parse(sheet_name='OED_CONSTRUCTION_CLASS')
        Occup_scheme = pd.ExcelFile(keyfiles_xlsx).parse(sheet_name='OED_OCCUPANCY_SCHEME')

        self.vulnerabilities = [vd for vd in self.df_to_dict(Vul_dic, csv_meta=self._VULNERABILITY_RECORD_META)]
        self.construction_class = [cc for cc in self.df_to_dict(Const_class, csv_meta=self._CONSTRUCTION_CLASS_RECORD_META)]
        self.occupancy_scheme = [oc for oc in self.df_to_dict(Occup_scheme, csv_meta=self._OCC_SCHEME_VULNERABILITY_RECORD_META)]

        self.vulnerability_index = {x['code'].upper(): x for x in self.vulnerabilities}
        self.construction_class_index = {(x['class'].upper(), x['peril_code'].upper()): x for x in self.construction_class}
        self.occupancy_scheme_index = {x['code'].upper(): x for x in self.occupancy_scheme}
        self.occupancy_list = [i for i in (self.occupancy_scheme_index)]
        for row in self.construction_class_index: self.construction_list.append(row[0])


    @oasis_log()

    def process_locations(self, loc_df):
        """
        Process location rows - passed in as a pandas dataframe.
        """
        r1 = []
        for i in range(len(loc_df)):
            loc_row = loc_df.iloc[i]
            record = self._get_location_record(loc_row)
            record['model_name'] = self.model_name
            row_failed = False
            ap_id = record['ap_id']
            coverage_type = ''
            for cov_id in (1, 3, 4):
                coverage_code = _CATRISKS_COVERAGE_CODE[cov_id]['coverage_code']
                record['coverage_type'] = coverage_code
                r1.append(self._make_results_file(record, ap_id,loc_row, cov_id))
        return r1

    def _make_results_file(self, record, ap_id,loc_row, cov_id):
        row_failed = False
        vulnerability_message =""
        loc_message = record['loc_message']
        status="falied"
        skip_switch = record['dissag_switch']
        if skip_switch>=0:
            try:
                vul_id, vulnerability_message = self._get_vulnerability_id(record)
            except Exception as e:
                vul_id = 0,
                vulnerability_message = ""
                row_failed = True
                logging.exception("Error {} processing location: {}".format(str(e), record.to_json()))
                #status = "failed" if row_failed else self._get_custom_lookup_success(ap_id, vul_id)
            if (int(ap_id)> 1000 and int(vul_id)>1000): status="success"
        else:
            status = "falied"
            if skip_switch == -1:
                loc_message = '"{}" is not in the list of licenced countries'.format(record['country'])
            elif skip_switch == -2:
                loc_message = '"{}" is not a valid {} Name/Code in {}'.format(record['geoname_1'], record['geosch_1'],record['country'])
            else:
                loc_message = '"{}" is not in the list of modelled perils'.format(record['locperilscovered'])
            vul_id = ap_id = -1
        return {
            "loc_id": record['loc_id'],
            "peril_id": record['locperilscovered'],
            "coverage_type": cov_id,
            # "coverage_type": record['coverage_type'],
            "area_peril_id": ap_id,
            "vulnerability_id": vul_id,
            "message": "{} / {}".format(vulnerability_message,loc_message),
            "status": status
            #"cell_weigth": ap_weight,
            #"disagg_status": "Input Level: [{}] {}: [{}]".format(input_level, disag_message, mapped_level),

        }

    def _get_vulnerability_id(self, record):
        """
        Get the vulnerability ID for a particular location record.
        """
        return self.get_vulnerability_id(
            record,
            self.vulnerability_index,
            self.occupancy_scheme_index,
            self.construction_class_index
        )

    def get_vulnerability_id(self, record, vulner_index, occ_shc_index, const_class_index):
        if record['constructioncode'].upper() not in self.construction_list:
            if record['occupancycode'].upper() not in self.occupancy_list:
                return -1, '%s & %s are not valid Construction and Occupancy Codes' % (record['constructioncode'],record['occupancycode'])
            else:
                return -1, '%s is not a valid ConstructionCode' % record['constructioncode']
        if record['occupancycode'].upper() not in self.occupancy_list:
            return -1, '%s is not a valid OccupancyCode' % record['occupancycode']
        const_rec = const_class_index[record['constructioncode'].upper(), record['locperilscovered'].upper()]
        occ_rec = occ_shc_index[record['occupancycode'].upper()]
        vul_cons_key = 0
        vul_occp_key = 0

        if const_rec:
            peril_code = const_rec['peril_code'].upper()
            # structural_quality = const_rec['quality_code'].upper()
            structural_type = const_rec['structural_type'].upper()
            if (record['no_storeys']) in [None, '', 'n/a', 'N/A', 'null', 'Null', 'NULL', 'nan', 'NaN']:
               (record['no_storeys'])='0'
            nostoreys=int(float(record['no_storeys']))
            if (record['year_built']) in [None, '', 'n/a', 'N/A', 'null', 'Null', 'NULL', 'nan', 'NaN']:
               (record['year_built'])='0'
            yearbuilt=int(float(record['year_built']))

            if nostoreys <= 0:
                # structural_height=const_rec['structural_height'].upper()
                structural_height = "MR"
            elif nostoreys > 0 and nostoreys <= 3:
                structural_height = "LR"
            elif nostoreys > 3 and nostoreys <= 7:
                structural_height = "MR"
            elif nostoreys > 7 and nostoreys <= 120:
                structural_height = "HR"
            elif nostoreys > 120:
                structural_height = "MR"
            if structural_type == "XXX":
                structural_height = "XX"
            if structural_type == "TIM" or structural_type == "MAS" or structural_type == "ADB":
                structural_height = "LR"

            if yearbuilt <= 0 or  yearbuilt > 2030:
                structural_quality = const_rec['quality_code'].upper()
            elif yearbuilt > 0 and yearbuilt <= 1960:
                structural_quality = "LQU"
            elif yearbuilt > 1960 and yearbuilt <= 1990:
                structural_quality = "MQU"
            elif yearbuilt > 1990:
                structural_quality = "GQU"
            vul_cons_key = 1
            
        if occ_rec:
            risk_type = occ_rec['risk_code'].upper()
            vul_occp_key = 1

        if vul_cons_key == 0:
            return -1, '. Wrong or missing Construction code'
        if vul_occp_key == 0:
            return -1, '. Wrong or missing Occupancy code'
        if (vul_cons_key == 0 and vul_occp_key == 0):
            return -1, '. Wrong or missing Constrcution and Occupancy codes'
        cov_type = record['coverage_type']
        if risk_type in ['A', 'M', 'E']:
            cov_type = "B"
        countrycode = record['orig_locnumber'][0:3].upper()
        code = '%s-%s-%s-%s-%s-%s-%s' % (countrycode, peril_code, risk_type, cov_type,
                                         structural_type, structural_height, structural_quality)

        vul_rec = vulner_index[code]
        if not vul_rec:
            return -1, '. There is no Vul-ID for %s' % code
        return vul_rec['id'], 'VulRef: {}'.format(code)

    def df_to_dict( self, df, csv_meta=None):
        for i in range(len(df)):
            r = df.iloc[i].to_dict()
            if not csv_meta:
                yield r
            else:
                yield {
                    k: csv_meta[k]['validator'](r[csv_meta[k]['csv_header']]) for k in csv_meta
                }

                def _get_location_record(self, loc_item):

                    meta = self._LOCATION_RECORD_META
                    result = {}
                    for k in meta:
                        validator = meta[k]['validator']
                        # the headers for OED data in loc_df are lower case
                        header = meta[k]['csv_header']
                        value = loc_item.get(header)
                        if value is None:
                            if not meta[k].get('optional'):
                                raise KeyError("Missing field '%s' in location record" % k)
                        else:
                            value = validator(value)
                        result[k] = value
                    return result

    def _get_location_record(self, loc_item):

        meta = self._LOCATION_RECORD_META
        result = {}
        for k in meta:
            validator = meta[k]['validator']
            # the headers for OED data in loc_df are lower case
            header = meta[k]['csv_header']
            value = loc_item.get(header)
            if value is None:
                if not meta[k].get('optional'):
                    raise KeyError("Missing field '%s' in location record" % k)
            else:
                value = validator(value)
            result[k] = value
        return result
    _VULNERABILITY_RECORD_META = {
        'id': {'csv_header': 'VULNERABILITY_ID', 'csv_data_type': int, 'validator': to_int, 'desc': 'Vulnerability ID'},
        'code': {'csv_header': 'REF', 'csv_data_type': str, 'validator': to_string, 'desc': 'Reference code'}
    }

    # A dictionary mapping location area level mapping record CSV fields to
    # internal Python fields used by the model keys lookup.
    _LOCATION_AREA_LEVEL_RECORD_META = {
        'area_level_names': {'csv_header': 'AREA_LEVEL_NAMES', 'csv_data_type': str, 'validator': to_string, 'desc': 'Area level names'},
        'country_key': {'csv_header': 'COUNTRY_KEY', 'csv_data_type': str, 'validator': to_string, 'desc': 'Country key'},
        'area_level_model_names': {'csv_header': 'AREA_LEVEL_MODEL_NAMES', 'csv_data_type': str, 'validator': to_string, 'desc': 'Area level model names'}
    }

    # A dictionary mapping occupancy scheme vulnerability record CSV fields to
    # internal Python fields used by the model keys lookup.
    _OCC_SCHEME_VULNERABILITY_RECORD_META = {
    #    'OED': {
            'code': {'csv_header': 'OED_OCCUPANCY_CODE', 'csv_data_type': str, 'validator': to_string, 'desc': 'OED_occupancy_code'},
            'risk_code': {'csv_header': 'VULNERABILITY_RISK_CODE', 'csv_data_type': str, 'validator': to_string, 'desc': 'Vulnerability_risk_code'},
    #        'quality_code': {'csv_header': 'VULNERABILITY QUALITY CODE', 'csv_data_type': str, 'validator': to_string, 'desc': 'Vulnerability quality code'}
    #    }
    }

    # A dictionary mapping construction class record CSV fields to internal Python
    # fields used by the model keys lookup.
    _CONSTRUCTION_CLASS_RECORD_META = {
        'class': {'csv_header': 'CONSTRUCTION_CLASS', 'csv_data_type': str, 'validator': to_string, 'desc': 'Construction_class'},
        'peril_code': {'csv_header': 'PERIL_CODE', 'csv_data_type': str, 'validator': to_string, 'desc': ' Peril_code'},
        'structural_type': {'csv_header': 'VULNERABILITY_STRUCTURAL_TYPE', 'csv_data_type': str, 'validator': to_string, 'desc': 'Vulnerability_structural_type'},
    #    'structural_height': {'csv_header': 'VULNERABILITY STRUCTURAL HEIGHT', 'csv_data_type': str, 'validator': to_string, 'desc': 'Vulnerability structural height'},
        'quality_code': {'csv_header': 'VULNERABILITY_QUALITY_CODE', 'csv_data_type': str, 'validator': to_string, 'desc': 'Vulnerability_quality_code'}
    }

    _LOCATION_RECORD_META = {
        'loc_id': {'csv_header': 'LocNumber', 'csv_data_type': int, 'validator': to_int, 'desc': 'Locnumber'},
        # Py Charm
        #'loc_id': {'csv_header': 'loc_id', 'csv_data_type': int, 'validator': to_int, 'desc': 'Locnumber'},# Oasis inbuilt
        'geosch_1': {'csv_header': 'GeogScheme1', 'csv_data_type': str, 'validator': to_string, 'desc': 'Geogscheme1'},
        'geoname_1': {'csv_header': 'GeogName1', 'csv_data_type': str, 'validator': to_string, 'desc': 'Geogname1'},
        #'latitude': {'csv_header': 'Latitude', 'csv_data_type': float, 'validator': to_float, 'desc': 'Latitude'},
        #'longitude': {'csv_header': 'Longitude', 'csv_data_type': float, 'validator': to_float, 'desc': 'Longitude'},
        'country': {'csv_header': 'CountryCode', 'csv_data_type': str, 'validator': to_string, 'desc': 'Country code'},
        'constructioncode': {'csv_header': 'ConstructionCode', 'csv_data_type': str, 'validator': to_string,
                             'desc': 'Construction Code'},
        'occupancycode': {'csv_header': 'OccupancyCode', 'csv_data_type': str, 'validator': to_string,
                          'desc': 'Occupancy Code'},
        'locperilscovered': {'csv_header': 'LocPerilsCovered', 'csv_data_type': str, 'validator': to_string,
                             'desc': 'LocPerilsCovered'},
        'no_storeys': {'csv_header': 'NumberOfStoreys', 'csv_data_type': str, 'validator': to_string,
                       'desc': 'NumberOfStoreys', 'optional': True},
        'dissag_switch': {'csv_header': 'FlexiLocDisaggKey', 'csv_data_type': int, 'validator': to_int, 'desc': 'FlexiLocDisaggKey',
                          'optional': True},
        'year_built': {'csv_header': 'YearBuilt', 'csv_data_type': str, 'validator': to_string, 'desc': 'YearBuilt',
                       'optional': True},
        'ap_id': {'csv_header': 'FlexiLocAP_ID', 'csv_data_type': str, 'validator': to_string,'desc': 'FlexiLocAP_ID'},
        'orig_locnumber': {'csv_header': 'FlexiLocNumber', 'csv_data_type': str, 'validator': to_string,'desc': 'FlexiLocNumber'},
        'loc_message': {'csv_header': 'FlexiLocMessage', 'csv_data_type': str, 'validator': to_string,'desc': 'FlexiLocMessage'}

    }

