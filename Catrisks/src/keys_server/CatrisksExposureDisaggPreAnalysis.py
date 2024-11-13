# -*- coding: utf-8 -*-

__all__ = [

    'ExposurePreAnalysis'
]

import collections
import csv
import os
import math
import logging
import itertools
import operator
import sqlite3
import sys
import pandas as pd
from itertools import islice,groupby
from operator import itemgetter
import zipfile
import io
from oasislmf.model_preparation.lookup import OasisBaseKeysLookup
from oasislmf.utils.log import oasis_log
from oasislmf.utils.status import OASIS_KEYS_FL

# Equatorial radius in kilometres - see
#    https://nssdc.gsfc.nasa.gov/planetary/factsheet/earthfact.html
EARTH_RADIUS = 6378.137
UNKNOWN_ID = -1
#from .values import (
#    to_float,
#    to_int,
#    to_string,
#)

NULL_VALUES = [None, '', 'n/a', 'N/A', 'null', 'Null', 'NULL','nan']


def to_string(val):
    """
    Converts value to string, with possible additional formatting.
    """
    return '' if val is None else str(val)


def to_int(val):
    """
    Parse a string to int
    """
    return None if val in NULL_VALUES else int(val)


def to_float(val):
    """
    Parse a string to float
    """
    return None if val in NULL_VALUES else float(val)

_AREA_LEVEL_MAPPING = {
    1: {'crs_level': 'CRSVG', 'area_level':'VRG'},
    2: {'crs_level': 'CRSL2', 'area_level':'area_level_2'},
    3: {'crs_level': 'CRSL3', 'area_level':'area_level_3'},
    4: {'crs_level': 'CRSL4', 'area_level':'area_level_4'},
    5: {'crs_level': 'CRSL5', 'area_level':'area_level_5'},
    6: {'crs_level': 'CRSL6', 'area_level':'area_level_6'},
    7: {'crs_level': 'CRSL7', 'area_level':'area_level_7'}

}

# A dictionary mapping raw area record CSV fields to internal Python fields
# used by the model keys lookup.
_AREA_RECORD_META = {
    'area_peril_id': {'csv_header': 'AREA_PERIL_ID', 'csv_data_type': int, 'validator': to_int,
                      'desc': 'Area peril ID'},
    'area_id': {'csv_header': 'AREA_ID', 'csv_data_type': int, 'validator': to_int, 'desc': 'Area ID'},
    'peril_code': {'csv_header': 'PERIL_CODE', 'csv_data_type': str, 'validator': to_string, 'desc': 'Peril Code'},
    'lon': {'csv_header': 'LON', 'csv_data_type': float, 'validator': to_float, 'desc': 'Longitude'},
    'lat': {'csv_header': 'LAT', 'csv_data_type': float, 'validator': to_float, 'desc': 'Latitude'},
    'population': {'csv_header': 'POPULATION', 'csv_data_type': float, 'validator': to_float, 'desc': 'Population'},
    'area_level_0': {'csv_header': 'AREA_LEVEL_0', 'csv_data_type': str, 'validator': to_string,
                     'desc': 'Area level #0'},
    'area_level_1': {'csv_header': 'AREA_LEVEL_1', 'csv_data_type': str, 'validator': to_string,
                     'desc': 'Area level #1'},
    'area_level_2': {'csv_header': 'AREA_LEVEL_2', 'csv_data_type': str, 'validator': to_string,
                     'desc': 'Area level #2'},
    'area_level_3': {'csv_header': 'AREA_LEVEL_3', 'csv_data_type': str, 'validator': to_string,
                     'desc': 'Area level #3'},
    'area_level_4': {'csv_header': 'AREA_LEVEL_4', 'csv_data_type': str, 'validator': to_string,
                     'desc': 'Area level #4'},
    'area_level_5': {'csv_header': 'AREA_LEVEL_5', 'csv_data_type': str, 'validator': to_string,
                     'desc': 'Area level #5'},
    'area_level_6': {'csv_header': 'AREA_LEVEL_6', 'csv_data_type': str, 'validator': to_string,
                     'desc': 'Area level #6'},
    'area_level_7': {'csv_header': 'AREA_LEVEL_7', 'csv_data_type': str, 'validator': to_string,
                     'desc': 'Area level #7'},
    'area_level_8': {'csv_header': 'AREA_PERIL_ID', 'csv_data_type': str, 'validator': to_string,
                     'desc': 'Area peril ID'},
    'aggregation_level': {'csv_header': 'AGGREGATION_LEVEL', 'csv_data_type': str, 'validator': to_string,
                          'desc': 'Aggregation level'}
}

# A dictionary mapping location area level mapping record CSV fields to
# internal Python fields used by the model keys lookup.
_LOCATION_AREA_LEVEL_RECORD_META = {
    'area_level_names': {'csv_header': 'AREA_LEVEL_NAMES', 'csv_data_type': str, 'validator': to_string,
                         'desc': 'Area level names'},
    'country_key': {'csv_header': 'COUNTRY_KEY', 'csv_data_type': str, 'validator': to_string, 'desc': 'Country key'},
    'area_level_model_names': {'csv_header': 'AREA_LEVEL_MODEL_NAMES', 'csv_data_type': str, 'validator': to_string,
                               'desc': 'Area level model names'}
}

# A dictionary mapping raw location record CSV fields to internal Python fields
# used by the model keys lookup.
_LOCATION_RECORD_META = {
    #'loc_id': {'csv_header': 'LocNumber', 'csv_data_type': int, 'validator': to_int, 'desc': 'Locnumber'},  # Py Charm
    'loc_id': {'csv_header': 'loc_id', 'csv_data_type': int, 'validator': to_int, 'desc': 'Locnumber'},# Oasis inbuilt
    'geosch_1': {'csv_header': 'GeogScheme1', 'csv_data_type': str, 'validator': to_string, 'desc': 'Geogscheme1'},
    'geoname_1': {'csv_header': 'GeogName1', 'csv_data_type': str, 'validator': to_string, 'desc': 'Geogname1'},
    'latitude': {'csv_header': 'Latitude', 'csv_data_type': float, 'validator': to_float, 'desc': 'Latitude'},
    'longitude': {'csv_header': 'Longitude', 'csv_data_type': float, 'validator': to_float, 'desc': 'Longitude'},
    'country': {'csv_header': 'CountryCode', 'csv_data_type': str, 'validator': to_string, 'desc': 'Country code'},
    'locperilscovered': {'csv_header': 'LocPerilsCovered', 'csv_data_type': str, 'validator': to_string,
                         'desc': 'LocPerilsCovered'},
    'dissag_switch': {'csv_header': 'FlexiLocDisaggKey', 'csv_data_type': int, 'validator': to_int, 'desc': 'FlexiLocDisaggKey',
                      'optional': True}
}
#class CatrisksBaseKeysLookup(OasisBaseKeysLookup):
class ExposurePreAnalysis:
    """
    This module assign AreaPeril-ID to all locations at various geographic CRS level.
    Locations with valid GeogScheme1 and GeogName1 codes get mapped to their AreaPeril_IDs.
    The module also performs disaggregation process based on target level set under FlexiLocDisaggKey filed in OED file.
    If no FlexiLocDisaggKey filed is given or for records with FlexiLocDisaggKey=0, no disaggregation is made and AreaPeril_ID 
    associated with CRS Level is used. Records with FlexiLocDisaggKey=1 get disaggregated to VRG cells with associated AreaPeril_IDS.
    Records with FlexiLocDisaggKey=2 to 7 get disaggregated to CRSL2 to 7 respectively.
    TIVs, NumberOf Buildings, deductibles abd limits (in monetary form) get disaggregated porportional to population
    """


    def __init__(self, exposure_data,exposure_pre_analysis_setting, **kwargs):
        self.exposure_data = exposure_data
        self.exposure_pre_analysis_setting = exposure_pre_analysis_setting
        
        
    def run(self):
        loc_df = self.exposure_data.location.dataframe
        keys_data_directory = self.exposure_pre_analysis_setting['keys_data_directory']
         
        self.location_map = {}
        self.areas_index = {}
        self.location_map_index = {}
        self.location_country_index = {}
        self.proxygrid = {}
        self.grouped_areas = {}
        self.adminname_list = {}


        self.proxygrid = self._read_proxy_grid(os.path.join(keys_data_directory, 'crseq_apgrid.dat'))

        filename = os.path.join(keys_data_directory, 'crseq_keysdata.dat')
        filelist = zipfile.ZipFile(filename).namelist()
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            with zip_ref.open(filelist[0], pwd=b'63297032190') as file:
                keyfiles_xlsx = io.BytesIO(file.read())

        AP_dic = pd.ExcelFile(keyfiles_xlsx).parse(sheet_name='DICTAREAPERIL')

        self.areas = [area for area in self.df_to_dict(AP_dic, csv_meta=_AREA_RECORD_META)]

        for i in range(1, 8):
            area_level_key = "AREA_LEVEL_{}".format(i)
            Loc_Level = pd.ExcelFile(keyfiles_xlsx).parse(sheet_name=area_level_key)
            self.location_map[area_level_key] = [ll for ll in
                                                 self.df_to_dict(Loc_Level, csv_meta=_LOCATION_AREA_LEVEL_RECORD_META)]

        for x in self.areas:
            p_code = x['peril_code'].lower()
            agg_level = x['aggregation_level'].lower()
            country = x['area_level_1'].upper()
            if country in self.grouped_areas:
                self.grouped_areas[country].append(x)
            else:
                self.grouped_areas[country] = [x]
            if agg_level.startswith('area_level_'):
                key = (country, x[agg_level].upper(), p_code.upper())
            else:
                key = (str(x['area_peril_id']).upper(), p_code.upper())
            self.areas_index[key] = x

        for l in self.location_map:
            self.adminname_list[l]=[]
            for r in self.location_map[l]:
                if l == 'AREA_LEVEL_1':
                    c_key = (r['area_level_names'].upper())
                    self.location_country_index[c_key] = r['area_level_model_names'].upper()
                self.adminname_list[l].append(r['area_level_names'].upper())
                key = (l, r['area_level_names'].upper(), r['country_key'].upper())
                self.location_map_index[key] = r

        self.exposure_data.location.dataframe = self.OED_dsiaggregation(loc_df)

    def OED_dsiaggregation(self, loc_df):
        """ Process to disaggregate input OED, using dsiaggregation switch."""
        rec = []
        i_rec = 0
        tiv_types = ['1Building', '2Others', '3Contents', '4BI', '5PD', '6All']
        oed_fields = list(loc_df.keys())
        ded_field_list = [fn for fn in oed_fields if "Ded" in fn]
        limit_field_list = [fn for fn in oed_fields if "Limit" in fn]
        for i in range(len(loc_df)):
            loc_row = loc_df.iloc[i]
            record = self._get_location_record(loc_row)
            #record['model_name'] = self.model_name
            row_failed = False
            ap_id = vul_id_1 = UNKNOWN_ID
            area_peril_message = vulnerability_message = ''
            skip_switch = 0
            PerilList= list(record['locperilscovered'].upper().split(";"))
            
            if record['country'] not in self.adminname_list['AREA_LEVEL_1']:
                skip_switch=-1
            if record['geosch_1'] in ("CRSL2","CRSL3", "CRSL4","CRSL5", "CRSL6","CRSL7"):
                adminlevel='AREA_LEVEL_'+record['geosch_1'][4:5]
                if record['geoname_1'].upper() not in self.adminname_list[adminlevel]:
                    skip_switch=-2
            if 'QEQ' not in PerilList and 'QQ1' not in PerilList and 'AA1' not in PerilList:
                skip_switch=-3            
            
            if skip_switch >=0:
                loc_row['LocPerilsCovered'] = record['locperilscovered'] = "QEQ"
                record['country'] = self.location_country_index[record['country']]
                if (record['country'] == "MAR" or record['country'] == "MA" or record['country'] == "MOR" and record['geoname_1'][:4] == "MAR-"):
                    new_geoname_1 = record['geoname_1'].replace("MAR-", "MOR-")
                    record['geoname_1'] = new_geoname_1
                if (record['country'] == "NA"):
                    record['country'] = "NAM"
                self.fix_locations_by_dictionary(record, self.location_map_index)
                input_level = record['geosch_1']
                if input_level == "": input_level = "CRSL1"
                if not self.no_latlon(record):
                    input_level = "Lat/Lon"
                    record['geosch_1'] = "CRSVG"
                ap_id, area_peril_message, mapped_level = self._get_area_peril_id(record)

            record['iso-code'] = record['country']
            if record['dissag_switch'] in [None, '', 'n/a', 'N/A', 'null', 'Null', 'NULL', 'nan']:
                record['dissag_switch'] = 0
            if record['dissag_switch'] not in [1, 2, 3, 4, 5, 6, 7] or record['geosch_1'] in ["CRSVG", "CRSL6","CRSL7"] or skip_switch <<0:
                IsDisaggregaetd = 0
                #if record['geosch_1'] in ["CRSVG"]:
                i_rec += 1
                loc_row['GeogName1'] = record['geoname_1']
                loc_row['FlexiLocAP_ID']= ap_id
                loc_row['CountryCode'] = record['iso-code']
                loc_row['GeogScheme1'] = record['geosch_1']
                loc_row['FlexiLocDisaggKey'] = skip_switch
                loc_row['LocNumber'] = i_rec             # replace original LocNumber with sequential location number 
                loc_row['FlexiLocNumber'] =record['country']+'_'+str(loc_row['LocNumber'])    # to keep original LocNumber
                loc_row['IsAggregate'] = 0 #  0 (default) represents a single site with multiple buildings, 1: represents aggregate data
                loc_row['FlexiLocMessage'] ="%s" % area_peril_message
                rec.append(dict(loc_row))
            else:
                disagg, to_level, disag_message = self.get_disaggregation(record, self.grouped_areas)
                IsDisaggregaetd = 1
                loc_row_it = dict(loc_row)
                for row in disagg:
                    if to_level=="CRSVG":
                        ap_id = row['area_id']
                    else:
                        try:
                            area = self.areas_index[(record['country'].upper(), row['area_id'].upper(), record['locperilscovered'].upper())]
                        except Exception as e:
                            ap_id=area['area_peril_id']
                            '''
                            This exception skips those zones for which there are VRG cells in DicAreaPeril but there is no record
                            in under Area-Level-1 to 7, for example PAK-03553
                            print (row['area_id'].upper(),area['area_peril_id']
                            '''
                        ap_id=area['area_peril_id']
                    weight = row['weight']
                    if weight>0:
                        i_rec += 1
                        loc_row_it['GeogName1'] = row['area_id']
                        loc_row_it['FlexiLocAP_ID'] = ap_id
                        loc_row_it['GeogScheme1'] = to_level
                        loc_row_it['CountryCode'] = record['iso-code']
                        loc_row_it['LocNumber'] = i_rec
                        loc_row_it['FlexiLocDisaggKey'] = int(disag_message)
                        loc_row_it['IsAggregate'] = 0  # 0 (default) represents a single site with multiple buildings, 1: represents aggregate data
                        loc_row_it['FlexiLocNumber'] = record['country'] + '_' + str(loc_row['LocNumber'])  # to keep original LocNumber
                        loc_row_it['FlexiLocMessage'] ="Disaggregated to: %s" % to_level

                        loc_row_it['ContentsTIV'] = (loc_row['ContentsTIV']) * weight
                        loc_row_it['BuildingTIV'] = (loc_row['BuildingTIV']) * weight
                        loc_row_it['BITIV'] = (loc_row['BITIV']) * weight
                        loc_row_it['OtherTIV'] = (loc_row['OtherTIV']) * weight

                        if str(loc_row['NumberOfBuildings']) not in [None, '', 'n/a', 'N/A', 'null', 'Null', 'NULL', 'nan', 'NaN']:
                            loc_row_it['NumberOfBuildings'] = max( int((loc_row['NumberOfBuildings']) * weight), 1)

                        for j in range(len(tiv_types)):
                            ded_f_type = 'LocDedType' + tiv_types[j]
                            ded_f = 'LocDed' + tiv_types[j]
                            ded_f_min = 'LocMinDed' + tiv_types[j]
                            ded_f_max = 'LocMaxDed' + tiv_types[j]
                            limit_f_type = 'LocLimitType' + tiv_types[j]
                            limit_f = 'LocLimit' + tiv_types[j]
                            if ded_f_type in ded_field_list and ded_f in ded_field_list:
                                if (loc_row[ded_f_type]) == 0:
                                    loc_row_it[ded_f] = (loc_row[ded_f]) * weight
                            if ded_f_min in ded_field_list:
                                loc_row_it[ded_f_min] = (loc_row[ded_f_min]) * weight
                            if ded_f_max in ded_field_list:
                                loc_row_it[ded_f_max] = (loc_row[ded_f_max]) * weight
                            if limit_f_type in limit_field_list and limit_f in limit_field_list:
                                if (loc_row[limit_f_type]) == 0:
                                    loc_row_it[limit_f] = (loc_row[limit_f]) * weight
                        rec.append(dict(loc_row_it))
        sorted_rec=sorted(rec, key=itemgetter('PortNumber', 'AccNumber', 'LocNumber'))
        for i, d in enumerate(sorted_rec, 1):
            d['LocNumber'] = i
        return pd.DataFrame(sorted_rec)

    def df_to_dict( self, df, csv_meta=None):
        for i in range(len(df)):
            r = df.iloc[i].to_dict()
            if not csv_meta:
                yield r
            else:
                yield {
                    k: csv_meta[k]['validator'](r[csv_meta[k]['csv_header']]) for k in csv_meta
                }

    def valid_latitude(self,latitude):
        """
        Validates a latitude value
        """
        return -90 <= latitude <= 90

    def valid_longitude(self,longitude):
        """
        Validates a longitude value
        """
        return -180 <= longitude <= 180

    def valid_lonlat(self, longitude, latitude):
        """
        Validates a longitude-latitude value pair
        """
        return self.valid_longitude(longitude) and self.valid_latitude(latitude)

        _AREA_PERIL_LOCATION_MAPPING_META = {
            1: {'agg_level': 'VRG', 'peril_lookup_basis': 'lonlat', 'peril_func': get_area_peril_id_based_on_latLon},
            2: {'agg_level': 'VRG', 'peril_lookup_basis': 'CRSVG', 'peril_func': get_area_peril_id_based_on_CRSVG},
            3: {'agg_level': 'AREA_LEVEL_7', 'peril_lookup_basis': 'CRSL7', 'peril_func': get_area_peril_id_based_on_CRSL7},
            4: {'agg_level': 'AREA_LEVEL_6', 'peril_lookup_basis': 'CRSL6', 'peril_func': get_area_peril_id_based_on_CRSL6},
            5: {'agg_level': 'AREA_LEVEL_5', 'peril_lookup_basis': 'CRSL5', 'peril_func': get_area_peril_id_based_on_CRSL5},
            6: {'agg_level': 'AREA_LEVEL_4', 'peril_lookup_basis': 'CRSL4', 'peril_func': get_area_peril_id_based_on_CRSL4},
            7: {'agg_level': 'AREA_LEVEL_3', 'peril_lookup_basis': 'CRSL3', 'peril_func': get_area_peril_id_based_on_CRSL3},
            8: {'agg_level': 'AREA_LEVEL_2', 'peril_lookup_basis': 'CRSL2', 'peril_func': get_area_peril_id_based_on_CRSL2},
            9: {'agg_level': 'AREA_LEVEL_1', 'peril_lookup_basis': 'CRSL1', 'peril_func': get_area_peril_id_based_on_CRSL1}
        }

    def get_area_peril_id_based_on_CRSVG(record, areas_index):
        if record['geosch_1'] == 'CRSVG':
            try:
                area = areas_index[record['geoname_1'].upper(), record['locperilscovered'].upper()]
                if record['country'].upper() == area['area_level_1'].upper():
                    return area['area_peril_id'], "Mapped by VRG: '{}'".format(area['area_peril_id'])
                else:
                    return area['area_peril_id'], "Warning-Mapped by VRG in another country:'{}' ".format(
                        area['area_level_1'].upper())
                # return None, "Given VRG ID is in another Country! found AreaPeril_ID '{}' : ".format(area['area_peril_id'])
            except:
                return None, "'{}' is not a valid VRG".format(record['geoname_1'])
            StopIteration

    def get_area_peril_id_based_on_CRSL7(record, areas_index):
        if record['geosch_1'] == 'CRSL7':
            try:
                area = areas_index[
                    (record['country'].upper(), record['geoname_1'].upper(), record['locperilscovered'].upper())]
                if record['country'].upper() == area['area_level_1'].upper():
                    return area['area_peril_id'], 'Mapped by L7 Cell:  {}'.format(area['area_peril_id'])
                return None, "L7 Cell is in another country: '{}' ".format(area['area_peril_id'])
            except:
                return None, "'{}' is not a valid L7 ID in: '{}'".format(record['geoname_1'], record['country'])
            StopIteration

    def get_area_peril_id_based_on_CRSL6(record, areas_index):
        if record['geosch_1'] == 'CRSL6':
            try:
                area = areas_index[
                    (record['country'].upper(), record['geoname_1'].upper(), record['locperilscovered'].upper())]
                if record['country'].upper() == area['area_level_1'].upper():
                    return area['area_peril_id'], 'Mapped by L6 Cell: {}'.format(area['area_peril_id'])
                return None, "L6 Cell is in another country: '{}'".format(area['area_peril_id'])
            except:
                return None, "'{}' is not a valid L6 ID in: '{}'".format(record['geoname_1'], record['country'])
            StopIteration

    def get_area_peril_id_based_on_CRSL5(record, areas_index):
        if record['geosch_1'] == 'CRSL5':
            try:
                area = areas_index[
                    (record['country'].upper(), record['geoname_1'].upper(), record['locperilscovered'].upper())]
                if record['country'].upper() == area['area_level_1'].upper():
                    return area['area_peril_id'], 'Mapped by City name: {}'.format(area['area_peril_id'])
                return None, "Given City is in another country: '{}'".format(area['area_peril_id'])
            except:
                return None, "'{}' is not a valid City name in:'{}'".format(record['geoname_1'], record['country'])
            StopIteration

    def get_area_peril_id_based_on_CRSL4(record, areas_index):
        if record['geosch_1'] == 'CRSL4':
            try:
                area = areas_index[
                    (record['country'].upper(), record['geoname_1'].upper(), record['locperilscovered'].upper())]
                if record['country'].upper() == area['area_level_1'].upper():
                    return area['area_peril_id'], 'Mapped by Municipality ID: {} : '.format(area['area_peril_id'])
                return None, "Given Municipality is in another country:'{}'".format(
                    area['area_peril_id'])
            except:
                return None, "'{}' is not a valid Municipality name in: '{}' / ".format(record['geoname_1'],
                                                                                       record['country'])
            StopIteration

    def get_area_peril_id_based_on_CRSL3(record, areas_index):
        if record['geosch_1'] == 'CRSL3':
            try:
                area = areas_index[
                    (record['country'].upper(), record['geoname_1'].upper(), record['locperilscovered'].upper())]
                if record['country'].upper() == area['area_level_1'].upper():
                    return area['area_peril_id'], 'Mapped by L3 Admin: {}'.format(area['area_peril_id'])
                return None, "Given L3 Admin ID is in another country: '{}'".format(
                    area['area_peril_id'])
            except:
                return None, "'{}' is not a valid L3 Admin in:'{}'".format(record['geoname_1'], record['country'])
            StopIteration

    def get_area_peril_id_based_on_CRSL2(record, areas_index):
        if record['geosch_1'] == 'CRSL2':
            try:
                area = areas_index[
                    (record['country'].upper(), record['geoname_1'].upper(), record['locperilscovered'].upper())]
                if record['country'].upper() == area['area_level_1'].upper():
                    return area['area_peril_id'], 'Mapped by L2 Admin: {}'.format(area['area_peril_id'])
                return None, "Given L2 Admin ID is in another country: '{}'".format(
                    area['area_peril_id'])
            except:
                return None, "'{}' is not a valid L2 Admin in:'{}'".format(record['geoname_1'], record['country'])
            StopIteration

    def get_area_peril_id_based_on_CRSL1(record, areas_index):
        try:
            area = areas_index[(record['country'].upper(), record['country'].upper(), record['locperilscovered'].upper())]
            return area['area_peril_id'], 'Mapped by country name: {}'.format(record['country'].upper())
        except:
            return None, '{} is not a valid country name : '.format(record['country'])
        StopIteration

    def get_area_peril_id_based_on_latLon(self,record, proxygrid, areas_index):
        c_key = record['country'].upper()
        pixel_x = int(round(((record['longitude'] - proxygrid['XLLCENTER']) / proxygrid['DX']), 0))
        pixel_y = proxygrid['NROWS'] + int(round(((proxygrid['YLLCENTER'] - record['latitude']) / proxygrid['DY']), 0)) - 1
        if pixel_x < proxygrid['NCOLS'] and pixel_y < proxygrid['NROWS'] and pixel_x >= 0 and pixel_y >= 0:
            apid = proxygrid['data'][pixel_y][pixel_x]
        else:
            apid = proxygrid['NODATA_VALUE']
        if apid == proxygrid['NODATA_VALUE']:
            return None, "No valid VRG cell for the given location"
        area = areas_index[(str(apid), record['locperilscovered'].upper())]
        if area['area_peril_id'] == apid and record['country'].upper() == area['area_level_1'].upper():
            return apid, "Mapped by Lon/Lat to VRG in: {}, distance of {} km".format(area['area_level_1'],
                                                                                    round(self.get_distance(record, area), 2))
        else:
            return apid, "Warning-Mapped by Lon/Lat to VRG but in another country:'{}', distance of {} km".format(
                area['area_level_1'], round(self.get_distance(record, area), 2))
                
    def fix_locations_by_dictionary(self, record, location_map_index):
        level = 0
        for i in range(2, 10):
            if record['geosch_1'] == self._AREA_PERIL_LOCATION_MAPPING_META[i]['peril_lookup_basis']:
                level = i
                break
        if level == 0 or level == 9:
            record['geosch_1'] = 'CRSL1'
            record['geoname_1'] = record['country'].upper()

        if not level in (0, 2):
            agg_level = self._AREA_PERIL_LOCATION_MAPPING_META[level]['agg_level'].upper()
            key =(agg_level, record['geoname_1'].upper(), record['country'].upper())
            r= location_map_index[key]
            record['geoname_1'] =r[ 'area_level_model_names']

    def get_area_peril_id(self, record, areas_index, proxygrid):
        if not record['country']:
            return UNKNOWN_ID, 'The country code should not be empty!'
        message = ''
        valid_geoshc = 0
        level_priorities = sorted(self._AREA_PERIL_LOCATION_MAPPING_META.keys())
        if not self.no_latlon(record) and self.valid_lonlat(record['longitude'], record['latitude']):
            peril_func = self._AREA_PERIL_LOCATION_MAPPING_META[1]['peril_func']
            area_peril_id, mes = self.get_area_peril_id_based_on_latLon(record, proxygrid, areas_index)
            message += mes
            if area_peril_id:
                message = mes
                mapped_level = 'CRSVG'
                return area_peril_id, message, mapped_level
            else:
                message = ' No valid Lat/Lon  '
        for priority in level_priorities:
            if priority != 1:
                agg_level = self._AREA_PERIL_LOCATION_MAPPING_META[priority]['agg_level']
                peril_func = self._AREA_PERIL_LOCATION_MAPPING_META[priority]['peril_func']
                if self._AREA_PERIL_LOCATION_MAPPING_META[priority]['peril_lookup_basis'] == record['geosch_1']:
                    valid_geoshc = 1
                    # sub_areas = list(filter(lambda area: area['aggregation_level'] == agg_level, areas))
                    area_peril_id, mes = peril_func(record, areas_index)
                    if area_peril_id:
                        message += mes
                        mapped_level = record['geosch_1']
                        break
                    else:
                        message = mes
                        record['geosch_1'] = 'CRSL1';
                        record['geoname_1'] = record['country'].upper()
                        peril_func = self._AREA_PERIL_LOCATION_MAPPING_META[9]['peril_func']
                        # sub_areas = list(filter(lambda area: area['aggregation_level'] == 'AREA_LEVEL_1', areas))
                        area_peril_id, mes = peril_func(record, areas_index)
                        message += mes
                        mapped_level = 'CRSL1'
                        break
        if valid_geoshc == 0:
            record['geosch_1'] = 'CRSL1';
            record['geoname_1'] = record['country'].upper()
            peril_func = self._AREA_PERIL_LOCATION_MAPPING_META[9]['peril_func']
            # sub_areas = list(filter(lambda area: area['aggregation_level'] == 'AREA_LEVEL_1', areas))
            area_peril_id, mes = peril_func(record, areas_index)
            message = " '{}' is not a valid GeoScheme ".format(record['geosch_1']) + mes
            mapped_level = 'CRSL1'
        return area_peril_id, message, mapped_level

    def get_disaggregation(self, record, grouped_areas):
        From_CRSLevel = record['geosch_1']
        country=record['country'].upper()
        if record['dissag_switch'] == 1:
            # Input levels gets disaggregated to VRG cell
            from_area_level = _AREA_LEVEL_MAPPING[int(From_CRSLevel[-1])]['area_level']
            if From_CRSLevel == "CRSL1": from_area_level = "area_level_1"
            To_CRSLevel = "CRSVG"
            sub_areas = [area for area in grouped_areas[country] if area['aggregation_level'] == "VRG"
                         and area[from_area_level] == record['geoname_1'] and area['peril_code'] == record['locperilscovered']]

            tot_pop = sum(map(lambda a: a['population'], sub_areas))
            tot_num = len(sub_areas)
            if tot_pop > 0.0:
                d_rows = map(lambda r: {'weight': r['population'] / tot_pop, 'area_id': r['area_peril_id']}, sub_areas)
                message = "101" #'Proportionally Disaggregated to'
            elif tot_num > 0:
                d_rows = map(lambda r: {'weight': 1 / tot_num, 'area_id': r['area_peril_id']}, sub_areas)
                message = "102" #'Uniformly Disaggregated to'
            else:
                To_CRSLevel = From_CRSLevel
                d_rows = map(lambda r: {'weight': 1.0, 'area_id': record['geoname_1']}, [1])
                message = "100" #'Could not be Disaggregated and remains at'
        elif record['dissag_switch'] in [2, 3, 4, 5, 6, 7]:
            # Input levels gets disaggregated to level 2 to 7
            from_area_level = _AREA_LEVEL_MAPPING[int(From_CRSLevel[-1])]['area_level']
            if From_CRSLevel == "CRSL1": from_area_level = "area_level_1"
            To_CRSLevel = "CRSL" + str(record['dissag_switch'])
            to_area_level = _AREA_LEVEL_MAPPING[int(To_CRSLevel[-1])]['area_level']

            #it frist selects all common VRGs between to_area_level and from_area_level zones
            sub_areas = [area for area in grouped_areas[country] if area['aggregation_level'] == "VRG" and area[to_area_level] != "Null"
                         and area[from_area_level] == record['geoname_1'] and area['peril_code'] == record['locperilscovered']]

            # The selected VRGs and their population get grouped by to_area_level tag
            tot_pop=tot_num =0
            key = lambda datum: datum[to_area_level]
            sub_areas.sort(key=key)
            geouped_sub_areas = [{'area_id': key, 'population': sum(int(item['population']) for item in group)}
                      for key, group in itertools.groupby(sub_areas, key=key)]
            for k in geouped_sub_areas:
                tot_pop +=int(k['population'])
                tot_num += 1            
                
            #if int(To_CRSLevel[4:5]) <= int(From_CRSLevel[4:5]):
                # Chekcs if input level is the same or higher than output level
            #    To_CRSLevel = From_CRSLevel
            #    d_rows = map(lambda r: {'weight': 1.0, 'area_id': record['geoname_1']}, [1])
            #    message = "100" #'is the same or higher Res, therefore remains at'
            if tot_pop > 0.0:
                # Disaggregation is made proportionally by population
                d_rows = map(lambda r: {'weight': r['population'] / tot_pop, 'area_id': r['area_id']}, geouped_sub_areas)
                message = "101" #'Proportionally Disaggregated to'
            elif tot_num > 0:
                # Disaggregation is made uniformly to all units
                d_rows = map(lambda r: {'weight': 1 / tot_num, 'area_id': r['area_id']}, geouped_sub_areas)
                message = "102" #'Uniformly Disaggregated to'
            else:
                To_CRSLevel = From_CRSLevel
                d_rows = map(lambda r: {'weight': 1.0, 'area_id': record['geoname_1']}, [1])
                message = "100" #'Could not be Disaggregated and remains at'
        return d_rows, To_CRSLevel, message

    def get_area_peril_id_builder(match_func, found_message, not_found_message):
        def get_area_peril_id_with_match(record, areas):
            try:
                area = next(a for a in areas if match_func())
                return area['area_peril_id'], found_message
            except StopIteration:
                return None, not_found_message

        return get_area_peril_id_with_match

    def no_latlon(self, record):
        long_valid = math.isnan(record['longitude']) or record['longitude'] in [None, float('nan'), '', ]
        lat_valid = math.isnan(record['latitude']) or record['latitude'] in [None, float('nan'), '', ]
        return lat_valid and long_valid

    def get_distance(self, record, area):
        lon1, lat1 = record['longitude'], record['latitude']
        lon2, lat2 = area['lon'], area['lat']

        sPhi, sTeta = map(math.radians, [lat1, lon1])
        ePhi, eTeta = map(math.radians, [lat2, lon2])

        sX = EARTH_RADIUS * math.cos(sPhi) * math.cos(sTeta)
        sY = EARTH_RADIUS * math.cos(sPhi) * math.sin(sTeta)
        sZ = EARTH_RADIUS * math.sin(sPhi)
        eX = EARTH_RADIUS * math.cos(ePhi) * math.cos(eTeta)
        eY = EARTH_RADIUS * math.cos(ePhi) * math.sin(eTeta)
        eZ = EARTH_RADIUS * math.sin(ePhi)

        lengthxyz = math.sqrt((sX - eX) ** 2 + (sY - eY) ** 2 + (sZ - eZ) ** 2)
        return 2 * math.asin(lengthxyz / 2 / EARTH_RADIUS) * EARTH_RADIUS

    def group_sum(self, key, list_of_dicts, sum_key):
        d = {}
        for dct in list_of_dicts:
            if dct[key] not in d:
                d[dct[key]] = {}
            for k, v in dct.items():
                if k == sum_key:
                    if k not in d[dct[key]]:
                        d[dct[key]][k] = v
                    else:
                        d[dct[key]][k] += v
        final_list = []
        for k, v in d.items():
            temp_d = {key: k}
            for k2, v2 in v.items():
                temp_d[k2] = v2
            final_list.append(temp_d)
        return final_list

    def _read_proxy_grid(self,apgridzip_file):
        filelist = zipfile.ZipFile(apgridzip_file).namelist()
        grid_file = zipfile.ZipFile(apgridzip_file, 'r').extract(filelist[0], pwd=b'63297032190')
        types = {
            'NCOLS': int,
            'NROWS': int,
            'XLLCENTER': float,
            'YLLCENTER': float,
            'DX': float,
            'DY': float,
            'NODATA_VALUE': int,
        }
        grid = {'data': []}

        def read_values(f):
            while True:
                for x in f.readline().split():
                    yield int(x)

        with open(grid_file) as f:
            for _ in range(7):
                line = f.readline()
                key, val = line.split()
                grid[key] = types[key](val)
            values = read_values(f)
            cols, rows = grid['NCOLS'], grid['NROWS']
            grid['data'] = [list(islice(values, cols)) for _ in range(rows)]
        return grid

    def _get_area_peril_id(self, record):
        """
        Get the area peril ID for a particular location record.
        """
        return self.get_area_peril_id(record, self.areas_index, self.proxygrid)

    def _get_location_record(self, loc_item):

        meta = _LOCATION_RECORD_META
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

    _AREA_PERIL_LOCATION_MAPPING_META = {
        1: {'agg_level': 'VRG', 'peril_lookup_basis': 'lonlat', 'peril_func': get_area_peril_id_based_on_latLon},
        2: {'agg_level': 'VRG', 'peril_lookup_basis': 'CRSVG', 'peril_func': get_area_peril_id_based_on_CRSVG},
        3: {'agg_level': 'AREA_LEVEL_7', 'peril_lookup_basis': 'CRSL7', 'peril_func': get_area_peril_id_based_on_CRSL7},
        4: {'agg_level': 'AREA_LEVEL_6', 'peril_lookup_basis': 'CRSL6', 'peril_func': get_area_peril_id_based_on_CRSL6},
        5: {'agg_level': 'AREA_LEVEL_5', 'peril_lookup_basis': 'CRSL5', 'peril_func': get_area_peril_id_based_on_CRSL5},
        6: {'agg_level': 'AREA_LEVEL_4', 'peril_lookup_basis': 'CRSL4', 'peril_func': get_area_peril_id_based_on_CRSL4},
        7: {'agg_level': 'AREA_LEVEL_3', 'peril_lookup_basis': 'CRSL3', 'peril_func': get_area_peril_id_based_on_CRSL3},
        8: {'agg_level': 'AREA_LEVEL_2', 'peril_lookup_basis': 'CRSL2', 'peril_func': get_area_peril_id_based_on_CRSL2},
        9: {'agg_level': 'AREA_LEVEL_1', 'peril_lookup_basis': 'CRSL1', 'peril_func': get_area_peril_id_based_on_CRSL1}
    }

