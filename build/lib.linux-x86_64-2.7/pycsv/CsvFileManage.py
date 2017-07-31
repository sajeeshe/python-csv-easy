from datetime import datetime
import collections
import hashlib
import pandas as pd
from sqlalchemy import Column, Integer, DateTime, String, BigInteger, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy import create_engine
import configparser
import shelve
from psycopg2.extensions import register_adapter, AsIs
import numpy as np


class CsvManage():

    def __init__(self, file_name=None):
        self.header = None
        self.header_list = []
        self.ignore_fields = []
        self.replace_data_type = []
        self.replace_fields = []
        self.field_list = {}
        self.header_lower = {}
        self.data_row_list = []
        self.checksumvalues = set()
        self.field_value = 0
        # Initial all required fields
        self.file_name = file_name
        self.file_pointer = None
        self.extra_fields = []
        self.extra_field_values = []
        self.__read_file()

    def __read_file(self):
        if self.file_name is None:
            print("Error: File not found")
            return
        try:
            self.file_pointer = pd.read_csv(self.file_name,  parse_dates=True)
        except Exception as e:
            print("File Error %s" % (e))

    def __convert_headers_tolower(self):
        headers = self.__get_headers()
        for fields in headers:
            self.header_lower[fields] = fields.lower()

        self.__tolower_all_columns()

    def get_headers(self):
        # Convert all headers to lower case and update the dataframe
        self.__convert_headers_tolower()
        # Drop column if any
        self.__drop_columns()
        # Get the modifield headers 
        headers = self.__get_headers()
        # Get the data types
        data_types = self.__get_data_types()

        for fields, datatype in zip(headers, data_types):
            field_name = fields.lower()
            str_datatype = str(datatype)
            replace_data_type = self.__replace_data_types(field_name)
            if replace_data_type:
                str_datatype = replace_data_type
            field_type_val = self.__get_field_type(str_datatype)
            replaced_val = self.__replace_key(field_name)
            if replaced_val:
                field_name = replaced_val

            self.field_list[field_name] = field_type_val
            self.header_list.append(field_name)
        self.field_list = collections.OrderedDict(
            sorted(self.field_list.items()))
        self.field_list['checksum'] = ("String", 70)
        return self.field_list

    def parse_csv_file(self):
        for j, i in enumerate(self.file_pointer.index):
            data_row = collections.OrderedDict()
            for header in self.header_list:
                data_row[header] = self.file_pointer[header][i] if \
                 (self.file_pointer[header][i] != "" and not pd.isnull(
                    self.file_pointer[header][i])) else None
            data_row['checksum'] = self.__create_checksum(data_row)
            if len(self.extra_fields) > 0:
                for i, extra_key in enumerate(self.extra_fields):
                    data_row[extra_key] = self.extra_field_values[i]
            if self.__check_existing_checksum(data_row['checksum']):
                self.data_row_list.append(data_row)
                self.checksumvalues.add(data_row['checksum'])

            if j == 1:
                break
        return self.data_row_list

    def drop_columns(self, fields):
        if len(fields) > 0:
            self.ignore_fields = fields

    def __drop_columns(self):
        if len(self.ignore_fields) > 0:
            for drop_col in self.ignore_fields:
                self.file_pointer.drop(drop_col, axis=1, inplace=True)

    def __tolower_all_columns(self):
        self.file_pointer.rename(columns=self.header_lower, inplace=True)

    def __get_headers(self):
        # header = pd.read_csv(self.file_name, nrows=1, parse_dates=True)
        # for i, drop_col in enumerate(self.ignore_fields):
        #     self.file_pointer.drop(drop_col, axis=1, inplace=True)
        return self.file_pointer.columns

    def __get_data_types(self):
        # header = pd.read_csv(self.file_name, nrows=1, parse_dates=True)
        # for drop_col in self.ignore_fields:
        #     self.file_pointer.drop(drop_col, axis=1, inplace=True)
        return self.file_pointer.dtypes

    def __get_field_type(self, field_type):
        if field_type == "int64":
            return "Integer"
        if field_type == "float64":
            return "Float"
        if field_type == "object":
            return "String"
        if field_type == "DateTime":
            return "DateTime"
        if field_type == "Integer":
            return "Integer"
        if field_type == "Float":
            return "Float"
        if field_type == "BigInteger":
            return "BigInteger"
        if field_type == "String":
            return "String"
        return "String"

    def replace_field_type(self, fields):
        if len(fields) > 0:
            self.replace_data_type = fields

    def replace_field(self, fields):
        if len(fields) > 0:
            self.replace_fields = fields

    def __replace_key(self, key_search):
        if len(self.replace_fields) > 0:
            for key, value in self.replace_fields.items():
                if key_search.lower() == key.lower():
                    return value
        return False

    def __replace_data_types(self, key_search):
        if len(self.replace_data_type) > 0:            
            for key, value in self.replace_data_type.items():
                if key_search.lower() == key.lower():
                    return value
        return False
 
    def __create_checksum(self, data_array, join_val=False):
        """Return the checksum of the given array."""
        if join_val:
            checksumval = ''.join(str(e) for e in data_array)
        else:
            checksumval = data_array
        return hashlib.sha256(str(checksumval).encode('utf-8')).hexdigest()

    def set_checksum(self, checksum):
        self.checksumvalues = set(checksum)

    def __check_existing_checksum(self, checksum):
        if checksum in self.checksumvalues:
            return False
        return True

    def set_extra_field_values(self, **kwargs):
        if kwargs is not None:
            for key, value in kwargs.items():
                setattr(self, key, value)