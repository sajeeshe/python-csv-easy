=============
pycsvdb
=============
Read csv and insert into DB

Requirements
-----------------

- Python 3.x
- hashlib
- pandas
- sqlalchemy
- shelve
- numpy

Installation
-----------------


    $ pip install git+https://gitlab.com/ies-suventure/utility/pycsvdb


Example
-----------------



   from pycsvdb import CsvManage

   csv_obj = CsvManage("filename.csv")

Field replacement 
------------------------
    csv_obj.replace_field({"existing_field_name": 'new_field_name'})

Field Datatype replacement 
------------------------
    csv_obj.replace_field_type({"existing_field_name": 'new data type'})
    new_data_type options are :

                            String
                            Integer
                            BigInteger
                            Float
                            DateTime

Adding some extra fields and values 
------------------------

    csv_obj.set_extra_field_values(extra_fields=['year','month'],
                               extra_field_values=['2000','jan'])

Setting checksum 
------------------------
    csv_obj.set_checksum([])

Parse Data
--------------------------
    data = csv_obj.parse_csv_file()
