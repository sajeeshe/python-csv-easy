from datetime import datetime
import collections
import hashlib
import pandas as pd
import sqlalchemy.exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, DateTime, String, BigInteger, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy import create_engine
import configparser
import shelve
from psycopg2.extensions import register_adapter, AsIs
import numpy as np


class CsvManage():

    def __init__(self):
        self.file_name = None
        self.header = None
        self.header_list = []
        self.db_config_file = "database.ini"
        self.log_table_name = "system_log"
        self.engine = None
        self.ModelClass = None
        self.LogTable = None
        self.primary_key = None
        self.table_name = None
        self.data = None
        self.ignore_fields = []
        self.replace_data_type = []
        self.replace_fields = []
        self.field_list = {}
        self.header_lower = {}
        self.schema = None
        self.env = "dev"
        self.shelve_file_name = "checksum.db"
        self.shelf_db = None
        self.data_row_list = []
        self.checksumvalues = []
        self.field_name = "No of records"
        self.field_value = 0
        self.module_name = None
        self.tstart = datetime.now()

    def init_settings(self):
        self.db_connect()
        self.Session = sessionmaker(bind=self.engine)
        self.load_schema()
        self.set_schema()
        self.open_shelve_file()
        if self.primary_key:
            self.field_list[self.primary_key] = Column(BigInteger,
                                                       primary_key=True)
        else:
            self.field_list['id'] = Column(BigInteger,
                                           primary_key=True)

    def generate_model(self):
        headers = self.get_headers()
        data_types = self.get_data_types()
        for fields, datatype in zip(headers, data_types):
            field_name_org = fields
            field_name = fields.lower()
            str_datatype = str(datatype)
            replace_data_type = self.search_data_type(field_name)
            if replace_data_type:
                str_datatype = replace_data_type
            field_type_val = self.get_field_type_names(str_datatype)
            replaced_val = self.search_key(field_name)
            if replaced_val:
                field_name = replaced_val

            if fields.lower() not in self.ignore_fields:
                self.field_list[field_name] = Column(field_type_val)
                self.header_list.append(field_name)
                """ Convert all header fields into lower case """
                self.header_lower[field_name_org] = field_name.lower()
        self.create_table()
        self.create_log_table()
        self.load_checksum()
        self.parse_csv_file()
        self.close_shelf_file()

    def parse_csv_file(self):
        self.data = pd.read_csv(self.file_name, parse_dates=True)
        self.drop_columns()
        """ This will convert all headers into lower case """
        self.rename_columns()

        for j, i in enumerate(self.data.index):
            data_row = collections.OrderedDict()
            for header in self.header_list:
                data_row[header] = self.data[header][i] if \
                 (self.data[header][i] != "" and not pd.isnull(
                    self.data[header][i])) else None
            data_row['checksum'] = self.create_checksum(data_row)
            checksum, value_exist = self.read__shelf_file(data_row['checksum'])
            if value_exist:
                self.data_row_list.append(data_row)
                self.append_shelf_file(data_row['checksum'])
            # if j == 20:
            #     break

        self.inget_data()
        self.field_value = len(self.data_row_list)
        self.inget_log()
        tend = datetime.now()
        self.field_name = "Execution time"
        self.field_value = tend - self.tstart
        self.inget_log()

    def set_schema(self):
        if self.env == "Pro":
            self.schema = self.prod_schema
        elif self.env == "Dev":
            self.schema = self.stage_schema

    def drop_columns(self):
        for drop_col in self.ignore_fields:
            self.data.drop(drop_col, axis=1, inplace=True)

    def rename_columns(self):
        self.data.rename(columns=self.header_lower, inplace=True)

    def get_headers(self):
        header = pd.read_csv(self.file_name, nrows=1, parse_dates=True)
        for drop_col in self.ignore_fields:
            header.drop(drop_col, axis=1, inplace=True)
        return header.columns

    def get_data_types(self):
        header = pd.read_csv(self.file_name, nrows=1, parse_dates=True)
        for drop_col in self.ignore_fields:
            header.drop(drop_col, axis=1, inplace=True)
        return header.dtypes

    def create_table(self):
        Base = declarative_base()
        attr_dict = {'__tablename__': self.table_name,
                     '__table_args__': {"schema": self.stage_schema},
                     'checksum': Column(String(70)),
                     'created_at': Column('created_at', DateTime(
                        timezone=True), server_default=func.now(),
                      nullable=True)
                     }
        attr_dict.update(self.field_list)
        self.ModelClass = type('ModelClass', (Base,), attr_dict)
        Base.metadata.create_all(self.engine)

    def inget_data(self):
        try:
            session = self.Session()
            session.bulk_insert_mappings(self.ModelClass, self.data_row_list)
            session.commit()
        except (Exception, sqlalchemy.exc.IntegrityError) as exc:
            print('Database Error: %s' % (exc))

            session.rollback()
        finally:
            session.close()

    def get_field_type_names(self, field_type):
        if field_type == "int64":
            return Integer
        if field_type == "float64":
            return Float
        if field_type == "object":
            return String
        if field_type == "DateTime":
            return DateTime
        if field_type == "Integer":
            return Integer
        if field_type == "Float":
            return Float
        if field_type == "BigInteger":
            return BigInteger
        if field_type == "String":
            return String
        return String

    def search_key(self, key_search):
        if len(self.replace_fields) > 0:
            for key, value in self.replace_fields.items():
                if key_search.lower() == key.lower():
                    return value
        return False

    def search_data_type(self, key_search):
        if len(self.replace_data_type) > 0:            
            for key, value in self.replace_data_type.items():
                if key_search.lower() == key.lower():
                    return value
        return False

    def db_connect(self):
        """
        Performs database connection
        Returns sqlalchemy engine instance
        """
        config = configparser.ConfigParser()
        config.read(self.db_config_file)
        self.engine = create_engine(config.get('DB', 'sqlalchemy.url'))

    def load_schema(self):
        config = configparser.ConfigParser()
        config.read(self.db_config_file)
        self.stage_schema = config.get('DB', 'stage_schema')
        self.prod_schema = config.get('DB', 'prod_schema')

    def create_checksum(self, data_array, join_val=False):
        """Return the checksum of the given array."""
        if join_val:
            checksumval = ''.join(str(e) for e in data_array)
        else:
            checksumval = data_array
        return hashlib.sha256(str(checksumval).encode('utf-8')).hexdigest()

    def open_shelve_file(self):
        self.shelf_db = None
        try:
            self.shelf_db = shelve.open(self.shelve_file_name, flag='n')
        except Exception as e:
            print("Shell Error  Opening  file%s" % (e))

    def close_shelf_file(self):
        try:
            self.shelf_db.close()
        except Exception as e:
            print("Shell File Error %s" % (e))

    def append_shelf_file(self, checksum):
        try:
            self.shelf_db[checksum] = True
        except Exception as e:
            print("Shell Error %s" % (e))

    def read__shelf_file(self, checksum):
        with shelve.open(self.shelve_file_name) as s:
            existing_checksum = None
            value_exist = True    
            try:
                existing_checksum = s[checksum]
                value_exist = False

            except Exception:
                value_exist = True

            return existing_checksum, value_exist

    def load_checksum(self):
        self.get_all_checksums_from_db()
        for checksum in self.checksumvalues:
            self.append_shelf_file(checksum)
        del self.checksumvalues

    def get_all_checksums_from_db(self):
        try:
            session = self.Session()
            checksumvalues = []
            checksumvalues = session.query(self.ModelClass.checksum)
            self.checksumvalues = [r for (r, ) in checksumvalues.all()]
        except (Exception, sqlalchemy.exc.IntegrityError) as exc:
            print('Database Error: %s' % (exc))

            session.rollback()
        finally:
            session.close()

    def create_log_table(self):
        Base = declarative_base()
        attr_dict = {'__tablename__': self.log_table_name,
                     '__table_args__': {"schema": self.stage_schema},
                     'id': Column(BigInteger, primary_key=True),
                     'field_name': Column(String),
                     'field_value': Column(String),
                     'module_name': Column(String),
                     'created_at': Column('created_at', DateTime(
                        timezone=True), server_default=func.now(),
                      nullable=True)
                     }
        self.LogTable = type('LogTable', (Base,), attr_dict)
        Base.metadata.create_all(self.engine)

    def inget_log(self):
        try:
            session = self.Session()
            item_inserted = (
                {"field_name": self.field_name,
                 "field_value": self.field_value,
                 "module_name": self.module_name})
            item_inserted_log = self.LogTable(**item_inserted)
            session.add(item_inserted_log)
            session.commit()
        except (Exception, sqlalchemy.exc.IntegrityError) as exc:
            print('Database Error: %s' % (exc))

            session.rollback()
        finally:
            session.close()


def adapt_numpy_int64(numpy_int64):
    """Integer type cast for Pandas."""
    return AsIs(numpy_int64)

register_adapter(np.int64, adapt_numpy_int64)


def addapt_numpy_bool(numpy_bool):
    """Boolean type cast for Pandas."""
    return AsIs(numpy_bool)

register_adapter(np.bool_, addapt_numpy_bool)

