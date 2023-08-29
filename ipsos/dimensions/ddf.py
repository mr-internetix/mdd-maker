import csv, datetime, itertools, ntpath, os, pandas, shutil, sqlite3, sys, re
import functools as ft
from collections import defaultdict, OrderedDict
from time import time, strftime, gmtime
# from slugify import slugify
import ipsos, ipsos.logs
import json, uuid, hashlib

import ipsos.dimensions.mdd
from ipsos.models.Document import Document
from ipsos.models.metadata_model.Variable import Variable

sys.path.append(os.path.dirname(ipsos.__file__))


class DDF:
    """ 
    This class contains methods for reading and manipulating ddf files. 

    Usage:
        ddf = ipsos.dimensions.ddf.DDF( path_to_mdd, path_to_ddf, verbose_mode )

        example:
            path_to_mdd = "./assets/Multithreadtest/Input Data/python_test_001.mdd"
            path_to_ddf = "./assets/Multithreadtest/Input Data/python_test_001.ddf"
            verbose_mode = True
            ddf = ipsos.dimensions.ddf.DDF( path_to_mdd, path_to_ddf, verbose_mode )
            cnt = ddf.count()

    Args: 
        path_to_mdd (str): The path to the mdd file. 
        path_to_ddf (str): The path to the ddf file.
        verbose (boolean): When True - generates extensive logging of the process (default = False)

    Attributes: 
        _metadata_tables (list): List of the standard metadata tables in a ddf file
        _metadata_tables_matches (list): List of the standard metadata tables in a ddf file 

    Methods:
        count( where = None ): Count the number of records in a ddf file using SQLite.
        dim_count( where = None ): Count the number of records in a ddf file using ADO.
        get_category_dict( variable_fullname ): Return a dictionary of category names/labels for a specified categorical variable.
        get_connection_string( mode = 3, mdsc_access = 2, mdm_access = 0, use_category_names = 1, use_category_values = 0, overwrite = 0 ): Get the connection string to the mdd/ddf.
        matches( ddf_files ): Check that the tables/columns in a list of ddf files matches the base ddf file.
        merge_identical_parts( ddf_files, output_mdd, output_ddf ): Merge case data for multiple identical ddf files.
        split( n, output_dir, split_into_folders = True ): Split a ddf file into n number of new ddf files.
        split_on_variable( variable_fullname, output_folder = ".\\" ): Split a ddf file into 1 file per response from a categorical variable.
        to_txt( txt_file = None, message = '' ): Write a string to a text file.
        to_csv( csv_file = None, use_category_names = 1, sep = ',', na_rep = '', float_format = None, columns = None, header = True, mode = 'w', encoding = None, compression = 'infer', quoting = csv.QUOTE_MINIMAL, quotechar = "\"", line_terminator = None, chunksize = None, date_format = None, doublequote = True, escapechar = None, decimal = "." ): Export VDATA to csv file.
        to_dataset( use_category_names = 1 ): Generate a .Net dataset from VDATA.
        to_df( use_category_names = 1 ): Generate a Pandas DataFrame from VDATA.
        to_excel( xlsx_file = None, use_category_names = 1, sheet_name = 'VDATA', na_rep = '', float_format = None, columns = None, header = True, startrow = 0, startcol = 0, engine = None, merge_cells = True, encoding = None, inf_rep = 'inf', verbose = True, freeze_panes = None ): Export VDATA to an Excel file.
        to_feather( feather_file = None, use_category_names = 1 ): Export VDATA to a feather file.
        extract_category_name( source_column_name, new_column_name, new_column_label = None, function = None )
        merge_csv(self, path_to_csv, ddf_join_column, csv_join_column, csv_column_name, ddf_variable_fullname, create_new_text_field=False, overwrite=False, new_text_field_label=None, sep=',', encoding='utf-8')
        set_of_variable_names(self, *patterns, collapse=False)
    """

    _metadata_tables = ["DataVersion", "Levels", "SchemaVersion"]
    _metadata_tables_matches = ["Levels"]

    # Holds the list of cached functions returning Data or Metadata
    _cached_methods = set()

    # Regex Patterns
    SIMPLE_VAR = r"([A-Z_][A-Z0-9_\.])*$"
    GRID = r"([A-Z_][A-Z0-9_\.]*[A-Z0-9_]+\[{?([A-Z_][A-Z0-9_]*)}?\]\.)[A-Z_][A-Z0-9_\.]*[A-Z0-9_]+$"
    VALUE_GRID = r"([A-Z_][A-Z0-9_\.]*[A-Z_]\[{?([A-Z_][A-Z0-9_]*)}?\]\.){2}[A-Z_][A-Z0-9_\.]*$"
    INDEX_PATTERN = r'\[{?([A-Z_][A-Z0-9_]*)}?\]'

    # Misc. constants
    _DATATYPE_TEXT = 2
    _DATATYPE_CATEGORY = 3

    # holds the EXPLICIT DATA CACHE
    _data_cache = {}

    def __init__(self, path_to_mdd, path_to_ddf = None, verbose=False):
        self.ddf = ""
        self.mdd = path_to_mdd
        self.verbose = verbose

        self.mdm = Document( )
        self.mdm.Open( self.mdd )

        # Set up the logger
        self.log = ipsos.logs.Logs(name='ddf', verbose=verbose)
        self._register_cache()
        return

    @property
    def ddf(self):
        return self.__ddf

    @ddf.setter
    def ddf(self, path_to_ddf):
        self.__ddf = path_to_ddf
        self._clear_cache()

    @property
    def mdd(self):
        return self.__mdd

    @mdd.setter
    def mdd(self, path_to_mdd):
        self.__mdd = path_to_mdd
        self._clear_cache()

    def _file_hash(self, path_to_file):
        BLOCKSIZE = 1048576  # 1MB
        hasher = hashlib.sha1()
        with open(path_to_file, 'rb') as f:
            buf = f.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = f.read(BLOCKSIZE)
        return hasher.hexdigest()

    def _register_cache(self):
        """
        This method stores in the _cached_methods set at class level the names of the methods 
        decorated with LRU_cache

        Args:
            None

        Returns:
            None
        """
        lst = dir(self)
        for method in lst:
            if hasattr(getattr(self, method), "cache_info"):
                self._cached_methods.add(method)
        return

    def _clear_cache(self):
        """
        This method calls <method>.cache_clear() for all class methods 
        decorated with LRU_cache

        Args:
            None

        Returns:
            None
        """
        for method in self._cached_methods:
            getattr(self, method).cache_clear()
        return

    def _resp_pk_minmax(self):
        """
        This method checks the respondent primary key in a ddf (:P0 in L1) and returns 
        the (min, max) tuple of that key.

        Args:
            none

        Returns:
            The (min, max) tuple of :P0 in table L1 of <self>
        """

        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()

        sql = "select min([:P0]) as minimum, max([:P0]) as maximum from L1"
        cur.execute(sql)
        row = cur.fetchone()

        cur.close()
        conn.close()

        return row

    def _get_new_pk_value(self, ddf):
        """
        This method checks the primary key from 2 sources being merged to see if there could be a conflict,
        if there could be, it returns the MAX value of the master (self) file which will be used to update the 
        primary key from the second (ddf) file.

        Args:
            ddf (ddf object): The ddf that is being copied from.

        Returns:
            Max primary key if there is a potential primary key violation
            0 if there is no violation
        """

        min1, max1 = self._resp_pk_minmax()     # pylint: disable=unused-variable
        min2, max2 = ddf._resp_pk_minmax()

        result = 0
        if ( max1 is not None and min2 is not None ):
            if max1 >= min2:
                result = int(max(max1, max2))

        return result

    def _append_to_table(self, table, ddf, new_pk_value):
        """
        This method copies data from one ddf file and appends it to the same table in another ddf file.

        Args:
            table (str): The table to copy data from/to.
            ddf (ddf object): The ddf that is being copied from.
            new_pk_value (int): The value to add to the primary key for the ddf being merged.

        Returns:
            None
        """
        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()
        cur.execute("attach database ? as db", [ddf.ddf])

        if (new_pk_value > 0):
            # Potential primary key violation, update the primary key
            pkcol = ddf._get_pk_col(cur, table)
            cur.execute("update db." + table + " set [" + str(pkcol) + "] = [" + str(pkcol) + "] + " + str(new_pk_value))

        cur.execute("insert into " + table + " select * from db." + table)
        conn.commit()
        cur.close()
        conn.close()

    def _casedata_table_columns_match(self, ddf_files):
        """
        This method compares the case data table schemas of a list of ddf files against the master ddf.

        Args:
            ddf_files (list of ddf objects): The ddf(s) that is/are being checked.

        Returns:
            True if the tables are identical
            False if the tables are not identical
        """
        # Get a list of all of the case data tables
        tables = self._get_casedata_tables()

        for table in tables:
            self.log.logs.info("Checking case data table " + table + " column names")
            master_columns = self._get_sqlite_table_columns(table)

            for ddf in ddf_files:
                # Check to see if the table matches the same table in the main ddf file
                compare_columns = ddf._get_sqlite_table_columns(table)

                if (not master_columns == compare_columns):
                    self.log.logs.warning("Case data columns do not match")
                    return False
                else:
                    self.log.logs.info("Case data (" + ddf.ddf + ") table " + table + " matches")

        self.log.logs.info("Case data columns match")

        return True

    def _copy_casedata_tables(self, path, ids, pk_dict, last_split, offset, limit=None):
        """
        This method copies case data from one ddf to another.

        Args:
            path (str): The path to the ddf file that the data will be copied to.
            ids (list): List of respondent ids.
            pk_dict (dictionary): Dictionary of the primary keys for each table.
            last_split (boolean): If this is the last split for the ddf.
            offset (int): The starting point for returning rows from the result set.
            limit (int - optional): When set, the number of records to return.

        Returns:
            None
        """
        self.log.logs.info("Copying casedata tables and inserting data with offset of " + str(offset) + " and limit of " + str(limit))
        # Determine which ids to use in the query
        if (last_split):
            self.log.logs.info("Identified range for slice: starting id " + str(ids[offset]) + ", ending id " + str(ids[-1]))
        else:
            self.log.logs.info("Identified range for slice: starting id " + str(ids[offset]) + ", ending id " + str(ids[offset + limit - 1]))

        try:
            conn = sqlite3.connect(self.ddf)
            cur = conn.cursor()
            cur.execute('PRAGMA cache_size = 30000')
            cur.execute("select name, sql from sqlite_master where type = 'table' and name not in (" + ", ".join("'{0}'".format(t) for t in self._metadata_tables) + ")")
            tables = cur.fetchall()

            cur.execute("attach database ? as db", [path])
            cur.execute('PRAGMA db.synchronous = OFF')
            cur.execute('PRAGMA db.journal_mode = OFF')
            cur.execute("BEGIN TRANSACTION;")

            for table in tables:
                sql = table[1].replace("CREATE TABLE ", "create table db.")
                self.log.logs.info("Executing " + sql)
                cur.execute(sql)

                pkcol = pk_dict.get(table[0])
                self.log.logs.info("Identified primary key column " + pkcol + " in table " + table[0])

                sql = "insert into db." + table[0] + " select * from " + table[0] + " where [" + pkcol + "] >= ? and [" + pkcol + "] <= ?"

                # Determine which ids to use in the query
                if (last_split):
                    cur.execute(sql, (ids[offset], ids[-1]))
                else:
                    cur.execute(sql, (ids[offset], ids[offset + limit - 1]))

                self.log.logs.info("Executing " + sql)

            cur.execute("COMMIT;")
            conn.commit()
            cur.close()
            conn.close()
        except:
            cur.close()
            conn.close()
            if (os.path.exists(path)): os.remove(path)
            self.log.logs.error("There was an error copying data to " + path + ".  This file has been deleted.")
            self.log.logs.error(sys.exc_info()[0])
            raise

    def _copy_casedata_tables_from_IDs(self, path, ids, pk_dict):
        """
        This method copies case data from one ddf to another, limiting the extract
        to the list of respondent IDs passed as parameter.

        Args:
            path (str): The path to the ddf file that the data will be copied to.
            ids (list): List of respondent ids copied over.
            pk_dict (dictionary): Dictionary of the primary keys for each table.

        Returns:
            None
        """
        self.log.logs.info(f"Copying casedata tables and inserting data for a total of {len(ids)} respondents")

        # Determine which ids to use in the query
        ids_string = ", ".join(str(id) for id in ids)
        mdt_string = ", ".join("'{0}'".format(t) for t in self._metadata_tables)

        sql_get_create = f"select name, sql from sqlite_master where type = 'table' and name not in ( {mdt_string} )"

        try:
            conn = sqlite3.connect(self.ddf)
            cur = conn.cursor()
            cur.execute('PRAGMA cache_size = 30000')
            cur.execute(sql_get_create)
            tables = cur.fetchall()

            cur.execute("attach database ? as db", [path])
            cur.execute('PRAGMA db.synchronous = OFF')
            cur.execute('PRAGMA db.journal_mode = OFF')
            cur.execute("BEGIN TRANSACTION;")

            for table in tables:
                # Create the table
                sql = table[1].replace("CREATE TABLE ", "create table db.")
                self.log.logs.info("Executing " + sql)
                cur.execute(sql)
                # Populate the table
                pkcol = pk_dict.get(table[0])
                self.log.logs.info("Identified primary key column " + pkcol + " in table " + table[0])
                sql = f"insert into db.{table[ 0 ]} select * from {table[ 0 ]} where [{pkcol}] in ({ids_string})"
                cur.execute(sql)
                self.log.logs.info("Executing " + sql)

            cur.execute("COMMIT;")
            conn.commit()
            cur.close()
            conn.close()

        except:
            cur.close()
            conn.close()
            if (os.path.exists(path)): os.remove(path)
            self.log.logs.error("There was an error copying data to " + path + ".  This file has been deleted.")
            self.log.logs.error(sys.exc_info()[0])
            raise

    def _copy_metadata_file(self, dest_folder, part):
        """
        This method copies the mdd file when doing a simple split of the ddf.

        Args:
            dest_folder (str): The location that you want to copy the mdd file to.
            part (str): An unique identifier for this segment of the mdd/ddf.

        Returns:
            None
        """
        original_filename = ntpath.basename(self.mdd)
        original_filename_without_extension = os.path.splitext(original_filename)[0]
        newfile = os.path.normpath(os.path.join(dest_folder, original_filename_without_extension + ".part-" + str(part) + ".mdd"))
        self.log.logs.info("Copying metadata file " + self.mdd + " to " + newfile)
        shutil.copyfile(self.mdd, newfile)

    def _copy_metadata_tables(self, path):
        """
        This method copies the metadata tables from one ddf to another where the metadata tables
        are defined in the _metadata_tables attribute.

        Args:
            path (str): The path to the ddf file that the data will be copied to.

        Returns:
            None
        """
        self.log.logs.info("Copying metadata tables")
        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()
        cur.execute("select name, sql from sqlite_master where type = 'table'")
        tables = cur.fetchall()
        cur.execute("attach database ? as db", [path])

        for table in tables:

            if (table[0] in self._metadata_tables):
                sql = table[1].replace("CREATE TABLE ", "create table db.")
                cur.execute(sql)
                cur.execute("insert into db." + table[0] + " select * from " + table[0])

        conn.commit()
        cur.close()
        conn.close()

    def count(self, where=None):
        """
        This method, using an optional WHERE clause, returns the number of respondents in a ddf.  The
        WHERE clause should refernce the variable name and value as they are in SQLite, not as they
        are in the ddf.  As this method goes directly against the L1 table of the SQLite database, you can only
        filter using 'simple' questions.

        Usage:
            cnt = ddf.count()
            cnt = ddf.count(where = '[D1a:C1] = 202')

        Args:
            where (str - optional): A WHERE clause to filter the count.

        Returns:
            The number of respondents in the ddf.
        """
        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()
        sql = "select count( a.[:P0] ) from L1 a"

        if (where is not None):
            if ( where.lower().find( 'where' ) > -1 ):
                sql += where
            else:
                sql += " where " + where

        cur.execute(sql)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return rows[0][0]

    def _get_casedata_tables(self):
        """
        This method gets a list of the case data tables in a ddf.

        Args:
            None

        Returns:
            A list of the case data table names.
        """
        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()
        cur.execute("select name from sqlite_master where type = 'table' and name not in (" + ", ".join("'{0}'".format(t) for t in self._metadata_tables) + ")")
        tables = cur.fetchall()

        cur.close()
        conn.close()

        return([name for tple in tables for name in tple])

    def get_category_dict(self, variable_fullname):
        """
        This method gets a dictionary of a variables category names/labels.

        Usage:
            d = ddf.get_category_dict( variable_fullname = "Q5")
            d = ddf.get_category_dict( variable_fullname = "Q9[{_5}].inn1")

        Args:
            variable_fullname (str): The categorical variable (as it is in the mdd) that we want the categories for.

        Returns:
            A dictionary of the category names/labels for the specified variable.
        """
        self.log.logs.info("Retrieving category names and labels for " + variable_fullname)
        d = {}

        v = self.mdm.VariableInstances[variable_fullname]

        # Create the category name/label dictionary
        for _, c in v.Categories.items():
            
            d[c.Name] = c.Label

        return d
    


    def get_only_category(self,variable_fullname):
        
        """
        This method gets a dictionary of a variables category names/labels.

        Usage:
            d = ddf.get_only_cartegory( variable_fullname = "Q5")
            d = ddf.get_only_cartegory( variable_fullname = "Q9[{_5}].inn1")

        Args:
            variable_fullname (str): The categorical variable (as it is in the mdd) that we want the categories for.

        Returns:
            A returns a list with precodes of all categories
        """
        self.log.logs.info("Retrieving category names and labels for " + variable_fullname)
        d = []

        v = self.mdm.VariableInstances[variable_fullname]

        # Create the category name/label dictionary
        for _, c in v.Categories.items():

            d.append(c.Name)

        return d



    @ft.lru_cache(maxsize=256)
    def _get_value_part_dict(self, variable_fullname):
        """
        This method gets a dictionary of a variables category value/part names.

        Usage:
            d = ddf.get_category_dict( variable_fullname = "Q5")
            d = ddf.get_category_dict( variable_fullname = "Q9[{_5}].inn1")

        Args:
            variable_fullname (str): The categorical variable (as it is in the mdd) that we want the categories for.

        Returns:
            A dictionary of the category value/part names for the specified variable.

        """
        try:
            self.log.logs.info("Retrieving value/part names for " + variable_fullname)
            d = {}

            v = self.mdm.VariableInstances[variable_fullname]

            # Create the category value/part dictionary
            for _, c in v.Categories.items():
                d[c.Value] = c.Name + "__" + str.encode(c.Label)

            return d
        except Exception as e:
            return {}
        



    def _split_varname_components(self, variable_fullname):
        """
        This method splits a full VDAT variable/column name into its components

        Args:
            variable_fullname (str): The categorical variable (as it is in the mdd) that we want to split into components.

        Returns:
            The list of components. 
        """
        tmp = variable_fullname.split(sep=".")
        result = []
        s = ""
        for c in tmp:
            s += c
            if c.endswith("]"):
                result.append(s)
                s = ""
            else:
                s += "."
        if s.endswith("."): s = s[:-1]
        result.append(s)

        return result

    def _get_table_filters(self, variable_fullname):
        """
        This method gets a list of DDF/SQLITE table filters from a variable full name.

        Args:
            variable_fullname (str): The categorical variable (as it is in the mdd) that we want the categories for.

        Returns:
            A list of the DDF/SQLITE filters required to extract the relevant set of respondents to split the data 
            according to the variable passed as an argument. For the filter variable itself, we extract the list 
            of category items and the effective_max_val.
        """

        # Check that the variable name is present in the MDD. Exit with an empty result if not.
        if variable_fullname.lower() not in self._list_of_all_var_names():
            return []

        # Connect to the DDF with SQLite
        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()

        # Retrieve the list of component fields and selectors that make up the variable
        lst = self._split_varname_components(variable_fullname)

        # Initialize our loop through the components
        sql = "SELECT TableName, ParentName, DSCTableName FROM levels WHERE ParentName = ? AND DSCTableName = ?"
        result_lst = []
        parentName = "L1"
        tableName = "L1"
        fieldName = ""

        data_type = self.mdm.VariableInstances[variable_fullname].DataType

        # Loop through the components starting from the left, excluding the last one
        for component in lst[:-1]:
            DSCTableName = component[:component.find('[')]
            filterName = component[component.find('[') + 1:component.find(']')]
            filterName = "".join([c for c in filterName if c not in "{}"])
            cur.execute(sql, (parentName, DSCTableName))
            tableName = cur.fetchone()[0]

            if ( component.find('[') > -1 ):
                fieldName += DSCTableName
            else:
                fieldName += component

            d = { c.Name: c.Value for _, c in self.mdm.Fields[fieldName].Categories.items() }
            filterValue = d.get(filterName)

            result_lst.append(("FILTER", tableName, DSCTableName, filterName, filterValue))
            parentName = tableName

            if ( component.find('[') > -1 ):
                fieldName += '[..]'
            fieldName += "."

        # Finally, we just add the last component as the SPLIT variable. Rather than the filter
        # name and value, we store the max_effective_val for the variable and the list of category items.
        value_name_dict = { c.Value: c.Name for _, c in self.mdm.VariableInstances[variable_fullname].Categories.items() }
        result_lst.append(("SPLIT", tableName, lst[-1], value_name_dict, int( self.mdm.VariableInstances[variable_fullname].MaxValue ) if data_type == self._DATATYPE_CATEGORY else 1))

        # Clean up
        conn.close()
        return result_lst

    def _update_datasource(self, new_mdd, new_ddf):
        """
        This method will update the data source to point to the correct ddf file.

        Args:
            new_mdd (str): The mdd that we want to update.
            new_ddf (str): The ddf that we want to update the data source with.

        Returns:
            None
        """
        self.log.logs.info("Updating the data source in " + new_mdd)
        original_filename = ntpath.basename(new_ddf)
        original_filename_without_extension = os.path.splitext(original_filename)[0]

        # Open the mdd file as text instead of with MDM.Document, this saves a lot of time on larger mdd files.
        with open(self.mdd, 'r', encoding='utf-8') as f:
            mdd_ds = f.read()

        # Find the current ddf file in the mdd.
        i = mdd_ds.find('.ddf')
        if ( i == -1 ): i = mdd_ds.find('.dzf')
        i2 = mdd_ds.rfind('"', 0, i)

        # Replace the ddf file name with the new name.
        ds = mdd_ds[i2 + 1:i]
        mdd_ds = mdd_ds.replace(ds, original_filename_without_extension)

        with open(new_mdd, 'w', encoding='utf-8') as f:
            f.write(mdd_ds)

    def _get_variable_max_value(self, variable_fullname):
        """
        This method gets the effective max value for a variable so that we can determine if it
        is single or multi-punch.

        Usage:
            d = ddf._get_variable_max_value( variable_fullname = "Q5")
            d = ddf._get_variable_max_value( variable_fullname = "Q9[{_5}].inn1")

        Args:
            variable_fullname (str): The categorical variable (as it is in the mdd) that we want the categories for.

        Returns:
            max_val (int): An integer representing the number of responses allowed for the variable.
        """
        self.log.logs.info("Retrieving effective max value for " + variable_fullname)
        try:
            v = self.mdm.VariableInstances[variable_fullname]

            max_val = int( v.MaxValue )

            return max_val  
        except Exception as e:
            return None
    
    def _get_variable_min_value(self, variable_fullname):
        """
        This method gets the effective min value for a variable so that we can determine if it
        is single or multi-punch.

        Usage:
            d = ddf._get_variable_min_value( variable_fullname = "Q5")
            d = ddf._get_variable_min_value( variable_fullname = "Q9[{_5}].inn1")

        Args:
            variable_fullname (str): The categorical variable (as it is in the mdd) that we want the categories for.

        Returns:
            min_val (int): An integer representing the number of responses allowed for the variable.
        """

        try:
            v = self.mdm.VariableInstances[variable_fullname]

            min_val = int( v.MinValue )

            return min_val  
        except Exception as e:
            return None
    

    def _get_variable_label(self, variable_fullname):
        """
        This method gets the effective min value for a variable so that we can determine if it
        is single or multi-punch.

        Usage:
            d = ddf._get_variable_min_value( variable_fullname = "Q5")
            d = ddf._get_variable_min_value( variable_fullname = "Q9[{_5}].inn1")

        Args:
            variable_fullname (str): The categorical variable (as it is in the mdd) that we want the categories for.

        Returns:
            min_val (int): An integer representing the number of responses allowed for the variable.
        """

        try:
        
            return self.mdm.VariableInstances[variable_fullname].Label
        except Exception as e:
            return None
        
         



    def _get_variable_datatype(self, variable_fullname):
        """
        This method gets the datatype of a variable

        Usage:
            d = ddf._get_variable_datatype( variable_fullname = "Q5")
            d = ddf._get_variable_datatype( variable_fullname = "Q9[{_5}].inn1")

        Args:
            variable_fullname (str): The categorical variable (as it is in the mdd) that we want the categories for.

        Returns:
            datatype (int): An integer representing the datatype for the variable (see _DATATYPE_* constants).
        """
        self.log.logs.info("Retrieving datatype for " + variable_fullname)
        try:
            dt = self.mdm.VariableInstances[variable_fullname].DataType
            return int( dt )
        except Exception as e:
            return None

    def get_connection_string(self, mode=3, mdsc_access=2, mdm_access=0, use_category_names=1, use_category_values=0, overwrite=0):
        """
        This method gets a connection string for an mdd/ddf.

        Usage:
            s = ddf.get_connection_string( )
            s = ddf.get_connection_string( mdm_access = 1 )

        Args:
            mode (int - optional): 1 = read, 2 = write, 3 = read/write [DEFAULT].
            mdsc_access (int - optional): 0 = read only, 1 = read/write - changes will be written to mdd, 2 = read/write [DEFAULT] - changes will NOT be written to mdd.
            mdm_access (int - optional): 0 = read only [DEFAULT], 1 = read/write - changes will be written to mdd, 2 = read/write - changes will NOT be written to mdd.
            use_category_names (int - optional): 1 = use category names [DEFAULT], 0 = use category values.
            use_category_values (int - optional): 0 = use mapped category values [DEFAULT], 1 = use native values.
            overwrite (int - optional): 0 = append to existing data if it exists [DEFAULT], 1 = delete output data/schema, 2 = delete output data but keep the schema.

        Returns:
            The connection string for an mdd/ddf.
        """
        # Check that arguements passed are valid
        not_valid = False
        are_equal = False
        if (mode < 1 or mode > 3): not_valid = True
        if (mdsc_access < 0 or mdsc_access > 2): not_valid = True
        if (mdm_access < 0 or mdm_access > 2): not_valid = True
        if (use_category_names < 0 or use_category_names > 1): not_valid = True
        if (use_category_values < 0 or use_category_values > 1): not_valid = True
        if (use_category_names == use_category_values): are_equal = True
        if (overwrite < 0 or overwrite > 2): not_valid = True

        if (are_equal):
            self.log.logs.error("ERROR: use_category_names and use_category_values cannot have the same value.")

            sys.exit()
        if (not_valid):
            self.log.logs.error("ERROR: One or more arguments contain an invalid value.")
            help(DDF.get_connection_string)

            sys.exit()

        s = "Provider = mrOleDB.Provider.2; Persist Security Info = False; Data Source = mrDataFileDsc"
        s += "; Location = " + self.ddf
        s += "; Initial Catalog = " + self.mdd
        s += "; Mode = " + str(mode)
        s += "; MR Init MDSC Access = " + str(mdsc_access)
        s += "; MR Init MDM Access = " + str(mdm_access)
        s += "; MR Init Category Names = " + str(use_category_names)
        s += "; MR Init Category Values = " + str(use_category_values)
        s += "; MR Init Overwrite = " + str(overwrite)
        s += "; MR Init Allow Dirty = False; MR Init Validation = True"

        return s

    def _get_pk_col(self, cur, table_name):
        """
        This method gets a primary key for the table specified.

        Args:
            cur (sqlite connection): SQLite connection to a ddf.
            table_name (str): Name of the table we are searching for the primary key.

        Returns:
            The highest index primary key of the table which holds the respondent index.
        """
        sql = "select * from " + table_name + " limit 1"
        cur.execute(sql)
        col_names = [column[0] for column in cur.description]
        idx = -1
        pkcol = None

        for column in col_names:

            # Find the larges :P variable, this refers to table L1
            if (column[0:2] == ":P"):
                pkidx = int(''.join(c for c in column if c.isdigit()))

                if (pkidx > idx):
                    idx = pkidx
                    pkcol = column

        return pkcol

    def _get_primary_key_group(self):
        """
        This method gets the indexes.

        Returns:
            The respondent indexes.
        """
        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()
        sql = "select [:P0] from L1 order by [:P0]"
        cur.execute(sql)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return([id for tple in rows for id in tple])

    def _get_sqlite_table(self, table):
        """
        This method gets all of the data for the specified table.

        Args:
            table (str): The table in the ddf that we want the records from.

        Returns:
            A result set containing all of the records in the specified table.
        """
        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()
        sql = "select * from " + table
        cur.execute(sql)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return rows

    def _get_sqlite_table_columns(self, table):
        """
        This method gets all of the column names for the specified table.

        Args:
            table (str): The table in the ddf that we want the column names from.

        Returns:
            A list of column names from the specified table.
        """
        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()
        cur.execute("select * from " + table + " limit 1")
        names = list(map(lambda x: x[0], cur.description))

        cur.close()
        conn.close()

        return names

    def _initialize_sqlite_db(self, path):
        """
        This method initializes a sqlite database.

        Args:
            path (str): The path to a ddf.

        Returns:
            None
        """
        self.log.logs.info("Initializing SQLite database at " + path)
        new_conn = sqlite3.connect(path)
        new_conn.close()

    def matches(self, ddf_files):
        """
        This method checks to see if ddf files are identical schema wise.

        Usage:
            # Create a DDF object for each ddf to be matched
            ddf1_mdd = "./assets/Multithreadtest/Input Data/python_test_001.mdd"
            ddf1_ddf = "./assets/Multithreadtest/Input Data/python_test_001.ddf"
            ddf_p1 = ipsos.dimensions.ddf.DDF( ddf1_mdd, ddf1_ddf, verbose_mode )

            ddf2_mdd = "./assets/Multithreadtest/Input Data/python_test_002.mdd"
            ddf2_ddf = "./assets/Multithreadtest/Input Data/python_test_002.ddf"
            ddf_p2 = ipsos.dimensions.ddf.DDF( ddf2_mdd, ddf2_ddf, verbose_mode )

            ddf3_mdd = "./assets/Multithreadtest/Input Data/python_test_003.mdd"
            ddf3_ddf = "./assets/Multithreadtest/Input Data/python_test_003.ddf"
            ddf_p3 = ipsos.dimensions.ddf.DDF( ddf3_mdd, ddf3_ddf, verbose_mode )

            b = ddf.matches( [ddf_p1, ddf_p2, ddf_p3] )

        Args:
            ddf_files (list): A list of one or more ddf objects.

        Returns:
            A boolean value indicating whether the metadata and case data table schemas match between ddfs.
        """
        self.log.logs.info("Check to make sure that the files are ddf objects.")
        fr = sys._getframe(1)
        for ddf in ddf_files:
            if (isinstance(ddf, ipsos.dimensions.ddf.DDF) == False):
                self.log.logs.error("ERROR: One or more of the ddf's is not of the correct type.")
                if (fr.f_code.co_name == 'merge_identical_parts'):
                    help(DDF.merge_identical_parts)
                else:
                    help(DDF.matches)

                return False

        self.log.logs.info("Evaluating ddf files to confirm that they are identical")
        metadata_tables_match = self._metadata_tables_match(ddf_files)
        casedata_table_columns_match = self._casedata_table_columns_match(ddf_files)

        return metadata_tables_match and casedata_table_columns_match

    def merge_identical_parts(self, ddf_files, output_mdd, output_ddf):
        """
        This method merges the data from multiple ddfs into a new ddf.

        Usage:
            # Create a DDF object for each ddf to be merged
            ddf1_mdd = "./assets/Multithreadtest/Input Data/python_test_001.mdd"
            ddf1_ddf = "./assets/Multithreadtest/Input Data/python_test_001.ddf"
            ddf_p1 = ipsos.dimensions.ddf.DDF( ddf1_mdd, ddf1_ddf, verbose_mode )

            ddf2_mdd = "./assets/Multithreadtest/Input Data/python_test_002.mdd"
            ddf2_ddf = "./assets/Multithreadtest/Input Data/python_test_002.ddf"
            ddf_p2 = ipsos.dimensions.ddf.DDF( ddf2_mdd, ddf2_ddf, verbose_mode )

            ddf3_mdd = "./assets/Multithreadtest/Input Data/python_test_003.mdd"
            ddf3_ddf = "./assets/Multithreadtest/Input Data/python_test_003.ddf"
            ddf_p3 = ipsos.dimensions.ddf.DDF( ddf3_mdd, ddf3_ddf, verbose_mode )

            ddf4_mdd = "./assets/Multithreadtest/Input Data/python_test_004.mdd"
            ddf4_ddf = "./assets/Multithreadtest/Input Data/python_test_004.ddf"
            ddf_p4 = ipsos.dimensions.ddf.DDF( ddf4_mdd, ddf4_ddf, verbose_mode )

            ddf5_mdd = "./assets/Multithreadtest/Input Data/python_test_005.mdd"
            ddf5_ddf = "./assets/Multithreadtest/Input Data/python_test_005.ddf"
            ddf_p5 = ipsos.dimensions.ddf.DDF( ddf5_mdd, ddf5_ddf, verbose_mode )

            # Specify the name and path for the merged mdd/ddf
            output_mdd = os.path.join( path_to_output_folder, "merged.mdd" )
            output_ddf = os.path.join( path_to_output_folder, "merged.ddf" )

            ddf_merged = ddf_p1.merge_identical_parts( [ ddf_p2,ddf_p3,ddf_p4,ddf_p5 ], output_mdd, output_ddf )

        Args:
            ddf_files (list): A list of one or more ddf objects.
            output_mdd (str): The output mdd path and name.
            output_ddf (str): The output ddf path and name.

        Returns:
            A new ddf object containing data from multiple ddfs.
            A message if the ddfs do not match.
        """
        if (self.matches(ddf_files)):
            start = datetime.datetime.now()
            self.log.logs.info("Verified that inputs are identical, starting merge_identical_parts")

            try:
                self.log.logs.info("Merging ddf files into " + output_ddf)
                if (os.path.exists(output_mdd)): os.remove(output_mdd)
                if (os.path.exists(output_ddf)): os.remove(output_ddf)
                shutil.copyfile(self.mdd, output_mdd)
                shutil.copyfile(self.ddf, output_ddf)
                self._update_datasource(output_mdd, output_ddf)
                new_ddf = DDF(output_mdd, output_ddf, self.verbose)
                tables = new_ddf._get_casedata_tables()

                for ddf in ddf_files:
                    # Check to see if ddf has any records
                    ddf_count = ddf.count()
                    self.log.logs.info("Count " + str(ddf_count))

                    if (ddf_count > 0):
                        self.log.logs.info("Adding data from " + ddf.ddf)
                        new_pk_value = new_ddf._get_new_pk_value(ddf)

                        for table in tables:
                            new_ddf._append_to_table(table, ddf, new_pk_value)
                    else:
                        self.log.logs.warning("No data in " + ddf.ddf)
            except:
                if (os.path.exists(output_mdd)): os.remove(output_mdd)
                if (os.path.exists(output_ddf)): os.remove(output_ddf)
                raise

            end = datetime.datetime.now()
            elapsed = end - start
            self.log.logs.info("Completed merge_identical_parts in " + str(elapsed))

            return new_ddf
        else:
            self.log.logs.warning("Inputs are not identical - cannot continue with merge_idential parts - aborting!!!")
            sys.exit()

    def _metadata_tables_match(self, ddf_files):
        """
        This method checks to see if the metadata tables between ddfs match where the metadata tables are
        defined in the _metadata_tables_matches attribute.

        Args:
            ddf_files (list): A list of one or more ddfs.

        Returns:
            A boolean value indicating whether the metadata tables schema match.
        """
        for table in self._metadata_tables_matches:
            self.log.logs.info("Checking metadata table " + table)
            master = self._get_sqlite_table(table)

            for ddf in ddf_files:
                compare = ddf._get_sqlite_table(table)

                if (not(master == compare)):
                    self.log.logs.warning("Metadata tables do not match")
                    return False
                else:
                    self.log.logs.info("Metadata (" + ddf.ddf + ") table " + table + " matches")

        self.log.logs.info("Metadata tables match")

        return True

    def _select_into_new_db(self, path, ids, pk_dict, last_split, offset, limit=None):
        """
        This method copies all of the data into a new ddf.

        Args:
            path (str): The path to the ddf file that the data will be copied to.
            ids (list): List of respondent ids.
            pk_dict (dictionary): Dictionary of the primary keys for each table.
            last_split (boolean): If this is the last split for the ddf.
            offset (int): The starting point for returning rows from the result set.
            limit (int - optional): When set, the number of records to return.

        Returns:
            None
        """
        self.log.logs.info("Selecting data into new SQLite database at " + path)
        self._initialize_sqlite_db(path)
        self._copy_metadata_tables(path)
        self._copy_casedata_tables(path, ids, pk_dict, last_split, offset, limit)

    def _select_into_new_db_from_IDs(self, path, ids, pk_dict):
        """
        This method copies the data into a new ddf for a given list of internal resp ids.

        Args:
            path (str): The path to the ddf file that the data will be copied to.
            ids (list): List of respondent ids to copy
            pk_dict (dictionary): Dictionary of the primary keys for each table.

        Returns:
            None
        """
        self.log.logs.info("Selecting data into new SQLite database at " + path)
        self._initialize_sqlite_db(path)
        self._copy_metadata_tables(path)
        self._copy_casedata_tables_from_IDs(path, ids, pk_dict)

    def _get_pk_dict(self):
        """
        This method returns a dictionary where the key is the name of each data table in the 
        ddf and the value is the internal respondent id column name (:P0 in L1).

        Args:
            None
        
        Returns:
            Dictionary as described above
        """
        pk_dict = {}
        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()
        cur.execute("select name, sql from sqlite_master where type = 'table' and name not in (" + ", ".join("'{0}'".format(t) for t in self._metadata_tables) + ")")
        tables = cur.fetchall()

        # Create a dictionary of each table and its primary key.
        for table in tables:
            pkcol = self._get_pk_col(cur, table[0])
            pk_dict[table[0]] = pkcol

        cur.close()
        conn.close()

        return pk_dict

    def _get_split_ids(self, table_filters):
        """
        This method returns a dictionary of respondent lists associated to the values
        defined by the table_filters structure passed as a parameter (see also _get_table_filters).

        Args:
            table_filters : a structure describing the filtering and split needed to split a DDF only via SQLite.

        Returns:
            A dictionary of DDF internal values pointing to lists of internal respondent keys
        """
        assert table_filters[-1][0] == "SPLIT", "Non-SPLIT item in last table_filters item"

        # Get the list of IDs that answered the question we're splitting on along with thevalue answered
        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()
        ids = []
        # Complex questions, not in L1
        if (len(table_filters) > 1):
            prevTable = ""
            for f in table_filters[:-1]:
                col1 = self._get_pk_col(cur, f[1])
                col2 = ":P" + str(int(col1[2])-1)
                if prevTable:
                    sql = f"CREATE TEMP TABLE {f[1]} AS SELECT * "
                    sql += f"FROM {f[1]} AS main JOIN {prevTable} AS prev ON main.[{col1}] = prev.[{pcol1}] AND main.[{col2}] = prev.[{pcol2}] "
                    sql += f"WHERE main.[levelID:C1] = {f[4]}"
                    # , (f[1], f[1], prevTable, col1, pcol1, col2, pcol2, f[4]))
                    cur.execute(sql)
                else:
                    sql = f"CREATE TEMP TABLE {f[1]} AS SELECT * FROM {f[1]} WHERE [levelID:C1] = {f[4]}"
                    cur.execute(sql)  # , (f[1], f[1], f[4]))
                prevTable = f"temp.{f[1]}"
                pcol1 = col1
                pcol2 = col2

            # prevtable now contains only the respondents that have answered the split variable and their answers to the question we're splitting on
            sql = f"SELECT * FROM {prevTable} LIMIT 1"
            for col_name in [cn[0] for cn in cur.execute(sql).description]:
                if col_name.lower().startswith(table_filters[-1][2].lower() + ":"):
                    break
            sql = f"SELECT DISTINCT [{pcol1}] as resp, [{col_name}] as answer FROM {prevTable}"
            ids = [(r[0], r[1]) for r in cur.execute(sql)]  # , (pcol1, prevTable))
        # Simple questions, just look in L1
        else:
            sql = f"SELECT * FROM L1 LIMIT 1"
            for col_name in [cn[0] for cn in cur.execute(sql).description]:
                if col_name.lower().startswith(table_filters[0][2].lower() + ":"):
                    break
            sql = f"SELECT DISTINCT [:P0] as resp, [{col_name}] as answer FROM L1 WHERE [{col_name}] IS NOT NULL"
            ids = ((r[0], r[1]) for r in cur.execute(sql))

        has_multiple_answers = (table_filters[-1][4] > 1)
        split_ids = defaultdict(list)
        if has_multiple_answers:
            for resp, val in ids:
                for s in val[0:-1].split(';'):
                    split_ids[int(s)].append(resp)
        else:
            for resp, val in ids:
                if not val == -1:
                    split_ids[val].append(resp)

        conn.close()
        return split_ids

    def split(self, n, output_dir, split_into_folders=True):
        """
        This method splits a ddf into n number of equal sized ddfs.

        Usage:
            ddf.split( 3, path_to_output_folder )

        Args:
            n (int): The number of files to split the ddf into.
            output_dir (str): The folder where the new ddfs should be placed.
            split_into_folders (boolean - optional): Whether to put each new ddf into it's own folder.

        Returns:
            None
        """
        start = datetime.datetime.now()
        self.log.logs.info("Splitting " + self.ddf + " into " + str(n) + " approximately equal parts")
        self.log.logs.info("Base output directory: " + output_dir)
        count = self.count()
        self.log.logs.info("Total records: " + str(count))
        group_size = int(round(count / n, 0))
        self.log.logs.info("Record count approximately " + str(group_size) + " records per file")
        if (not os.path.exists(output_dir)): os.mkdir(output_dir)
        original_filename = ntpath.basename(self.ddf)
        original_filename_without_extension = os.path.splitext(original_filename)[0]

        pk_dict = self._get_pk_dict()

        # Get a list of all id's from the L1 table.
        ids = self._get_primary_key_group()

        for i in range(1, n + 1):
            last_split = False
            if (i == n): last_split = True
            if (split_into_folders):
                split_folder = os.path.join(output_dir, "part-" + str(i))
                if (not os.path.exists(split_folder)): os.mkdir(split_folder)
            else:
                split_folder = output_dir

            self.log.logs.info("Using " + split_folder + " for split operation")
            split_filename = os.path.normpath(os.path.join(split_folder, original_filename_without_extension + "_part-" + str(i) + ".ddf"))
            if (os.path.exists(split_filename)):
                self.log.logs.info("Removing existing file before split: " + split_filename)
                os.remove(split_filename)

            offset = (i - 1) * group_size
            self._select_into_new_db(split_filename, ids, pk_dict, last_split, offset, group_size)
            #self._copy_metadata_file( split_folder, i )
            self._update_datasource(split_filename.replace('.ddf', '.mdd'), split_filename)
            self._add_index( split_filename )

        end = datetime.datetime.now()
        elapsed = end - start
        self.log.logs.info("Elapsed time for split operation: " + str(elapsed))

    def split_on_variable(self, variable_fullname, output_folder=".\\"):
        """
        This method splits a ddf based on the categories in the specified field. This method uses the SQLITE tables in the DDF,
        you must specify a fully-qualified variable name as shown in the second example under Usage below.

        Usage:
            ddf.split_on_variable( "D1a", path_to_output_folder )
            ddf.split_on_variable( "Q9[{_5}].inn1", path_to_output_folder )

        Args:
            variable_fullname (str): The categorical variable (as it is in the mdd) to use when splitting a ddf. 
            One ddf/mdd is created for each NON EMPTY category.

            output_folder (str - optional): The folder where the new ddfs should be placed. Default is the 
            current working directory.

        Returns:
            None
        """

        start = datetime.datetime.now()
        self.log.logs.info(f"Starting split operation - splitting {self.mdd} on {variable_fullname}")

        # Get the map of filtering and split operations we need to perform
        table_filters = self._get_table_filters(variable_fullname)

        if not table_filters:
            self.log.logs.error(f"Variable: {variable_fullname} is not a recognized variable of {self.mdd} - No data written.")
            return None
        # <----------------

        self.log.logs.info(f"Extracted {len(table_filters)} filters and split descriptors")

        if (table_filters[-1][4] > 1):
            # This is a multi-punch question, warn the user.
            self.log.logs.warning(f"Variable: {variable_fullname} is a multi-punch variable.  Respondents may be in more than one output file.")

        # Use the filter description to get respondents for each value
        split_ids = self._get_split_ids(table_filters)

        self.log.logs.info(f"Extracted {len(split_ids)} different values for the split")

        # Get the labels associated with each value
        parts = self._get_value_part_dict(variable_fullname)

        original_ddf = ntpath.basename(self.ddf)
        original_ddf_without_extension = os.path.splitext(original_ddf)[0]
        original_mdd = ntpath.basename(self.mdd)
        original_mdd_without_extension = os.path.splitext(original_mdd)[0]
        pk_dict = self._get_pk_dict()

        for value, ids in split_ids.items():

            part = parts[value]

            try:
                newddf = os.path.join(output_folder, original_ddf_without_extension + "_" + part + ".ddf")
                newmdd = os.path.join(output_folder, original_mdd_without_extension + "_" + part + ".mdd")
                shutil.copyfile(self.mdd, newmdd)
                self._select_into_new_db_from_IDs(newddf, ids, pk_dict)
                self._update_datasource(newmdd, newddf)
                self._add_index( newddf )

            except (Exception) as error :
                print( str(error) )
                if (os.path.exists(newddf)): os.remove(newddf)
                if (os.path.exists(newmdd)): os.remove(newmdd)
                raise

        end = datetime.datetime.now()
        elapsed = end - start
        self.log.logs.info("Elapsed time for split operation: " + str(elapsed))

        return

    def to_txt(self, txt_file=None, message=''):
        """
        This method will export a message to a text file.

        Usage:
            ddf.to_txt( txt_file = "test.txt", message = "Hello World!" )

        Args:
            txt_file (str - optional): The path and name of the txt file.
            message (str - optional): The text to output.

        Outputs:
            txt file.

        Returns:
            None
        """
        start = datetime.datetime.now()
        if (txt_file is None):
            original_filename = ntpath.basename(self.ddf)
            original_filename_without_extension = os.path.splitext(original_filename)[0]
            txt_file = os.path.normpath(original_filename_without_extension + ".txt")

        with open(txt_file, 'w') as out_file:
            out_file.write(message)

        end = datetime.datetime.now()
        elapsed = end - start
        self.log.logs.info("Elapsed time for to_txt operation: " + str(elapsed))

    def to_csv(self, csv_file=None, use_category_names=1, columns=None, sep=',', na_rep='', float_format=None, header=True, mode='w', encoding=None, compression='infer', quoting=csv.QUOTE_MINIMAL, quotechar="\"", line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal="."):
        """
        This method will export VDATA to a csv file.

        Usage:
            ddf.to_csv( csv_file = "test.csv" )
            ddf.to_csv( csv_file = "test.csv", sep = ";" )

        Args:
            csv_file (str - optional): The path and name of the csv file.
            use_category_names (int - optional): 1 = use category names, 0 = use category values.
            columns ( list - optional): The columns to export. Defaults to the exportable columns in the DDF.

            See pandas.DataFrame.to_csv for additional information about each arg below.

            sep (str - optional): The separator to use in the csv file.
            na_rep (str - optional): The value to use for missing data.
            float_format (str - optional): The format to use for real numbers.
            header (bool/str - optional): Write out column headers or a list with column header aliases. 
            mode (str - optional): Python write mode.
            encoding (str - optional): encoding to use - default is utf-8.
            compression (str - optional): Compression mode to use.
            quoting (constant - optional): 
            quotechar (str - optional): Character used in quote fields.
            line_terminator (str - optional): Newline character to use in output file.
            chunksize (int - optional): Number of rows to write at a time.
            date_format (str - optional): Format string for dates.
            doublequote (boolean - optional): Control quoting of quotechar inside a field.
            escapechar (str - optional): Character used to escape sep and quotechar when appropriate.
            decimal (str - optional): Character recognized as decimal separator.

        Outputs:
            csv file.

        Returns:
            None
        """
        start = datetime.datetime.now()
        self.log.logs.info("Converting Dimensions VDATA to CSV file")

        if (csv_file is None):
            original_filename = ntpath.basename(self.ddf)
            original_filename_without_extension = os.path.splitext(original_filename)[0]
            csv_file = os.path.normpath(original_filename_without_extension + ".csv")

        df = self.to_df(use_category_names, columns=columns)
        self.log.logs.info("Writing " + csv_file)
        df.to_csv(csv_file, sep=sep, na_rep=na_rep, float_format=float_format, columns=columns, header=header, index=False, mode=mode, encoding=encoding, compression=compression, quoting=quoting, quotechar=quotechar, line_terminator=line_terminator, chunksize=chunksize, date_format=date_format, doublequote=doublequote, escapechar=escapechar, decimal=decimal)
        end = datetime.datetime.now()
        elapsed = end - start
        self.log.logs.info("Elapsed time for to_csv operation: " + str(elapsed))

    @ft.lru_cache(maxsize=1)
    def _list_of_all_var_names(self):
        """
        This function will query the .MDD and return a set of all variable names.

        Usage:
            _list_of_all_var_names()

        Args:
            none

        Outputs:
            none

        Returns:
            list of var names (list of strings)
        """


        # var_lst = [ var.FullName  for var in self.mdm.VariableInstances ]
        var_lst = [var.FullName for var in self.mdm.VariableInstances if var.IsSystem == False ]
        return var_lst

    @ft.lru_cache(maxsize=1)
    def _list_of_exportable_var_names(self):
        """
        This function will query the .MDD and return a set of names of 
        variables ok to export.

        Usage:
            _list_of_exportable_var_names()

        Args:
            none

        Outputs:
            none

        Returns:
            set of var names (set of strings)
        """
        var_lst = []
        for var in self.mdm.VariableInstances:
            skip_q = False
            # Check the properties to see if it is a CORTEX question or a shell question or a question flagged not exportable.
            #   if so, do not upload to the database.
            if (var.FullName not in ['resp_age', 'resp_gender']):
                use_name = var.FullName
                if ( var.FullName.find( '[' ) > -1 ):
                    use_name = ''
                    parts = var.FullName.split( '.' )
                    for part in parts:
                        if ( part.find( '[' ) > -1 ):
                            use_name += part[:part.find( '[' )] + '[..].'
                        else:
                            use_name += part + '.'
                    use_name = use_name[:-1]

                for prop in self.mdm.Fields[use_name].Properties._items:
                    if ( prop.Name in ['IIS_StandardShellTranslated', 'IIS_CortexQuestionDescription'] ):
                        skip_q = True
                        break
                    elif ( ( prop.Name == 'IIS_ExcludeFromDataExport') and prop.Value ):
                        skip_q = True
                        break

            if ( not ( skip_q ) and var.HasCaseData ):
                var_lst.append( var.FullName.lower() )

        return var_lst

    def to_df(self, use_category_names=1, columns=None):
        """
        This method will create a Pandas DataFrame from VDATA.

        Usage:
            df = ddf.to_df( )
            df = ddf.to_df( use_category_names = 0 )

        Args:
            use_category_names (int - optional): 1 = use category names, 0 = use category values.
            columns: (list - optional): list of columns to export. Defaults to the list of exportable columns in the DDF

        Returns:
            Pandas DataFrame.
        """
        start = datetime.datetime.now()
        self.log.logs.info("Extracting Dimensions VDATA to Pandas Data Frame")
        self.log.logs.info("Retrieving Dimensions VDATA.")

        if columns:
            var_string = ', '.join(columns)
        else:
            var_string = ', '.join(self._list_of_exportable_var_names())

        self._get_casedata( var_string.lower(), use_category_names )

        self.log.logs.info("Final Count : " + str( len( self.resp_dict ) ) )
        df = pandas.DataFrame.from_dict( self.resp_dict ).transpose()

        end = datetime.datetime.now()
        elapsed = end - start
        self.log.logs.info("Elapsed time for to_df operation: " + str(elapsed))
        
        return df

    def to_excel(self, xlsx_file=None, use_category_names=1, sheet_name='VDATA', na_rep='', float_format=None, columns=None, header=True, startrow=0, startcol=0, engine=None, merge_cells=True, encoding=None, inf_rep='inf', verbose=True, freeze_panes=None):
        """
        This method will create an Excel file from VDATA.

        Usage:
            ddf.to_excel( xlsx_file = "test.xlsx" )
            ddf.to_excel( xlsx_file = "test.xlsx", sheet_name = 'ProjectData' )

        Args:
            xlsx_file(str - optional): Path and name of Excel file.
            use_category_names (int - optional): 1 = use category names, 0 = use category values.

            See pandas.DataFrame.to_excel for additional information about each arg below.

            sheet_name (str - optional): Name of the excel sheet to store VDATA in.
            na_rep (str - optional): The value to use for missing data.
            float_format (str - optional): The format to use for real numbers.
            columns (str - optional): The columns to export.
            header (bool/str - optional): Write out column headers or a list with column header aliases. 
            startrow (int - optional): The upper left cell row to dump the data.
            startcol (int - optional): The upper left cell column to dump the data.
            engine (str - optional): The write engine to use.
            merge_cells (boolean - optional): Write multiindex and hierarchical rows as merged cells.
            encoding (str - optional): Encoding of the Excel file.
            inf_rep (str - optional): Representation for infinity.
            verbose (boolean - optional): Display more information in the error logs.
            freeze_panes (tuple of int [length 2] - optional): Specifies the one-based bottommost row and rightmost column that is to be frozen.

        Outputs: 
            Excel file.

        Returns:
            None
        """
        start = datetime.datetime.now()
        self.log.logs.info("Converting Dimensions VDATA to excel file")

        if (xlsx_file is None):
            original_filename = ntpath.basename(self.ddf)
            original_filename_without_extension = os.path.splitext(original_filename)[0]
            xlsx_file = os.path.normpath(original_filename_without_extension + ".xlsx")

        df = self.to_df(use_category_names)
        self.log.logs.info("Writing " + xlsx_file)
        df.to_excel(xlsx_file, sheet_name=sheet_name, na_rep=na_rep, float_format=float_format, columns=columns, header=header, index=False, startrow=startrow, startcol=startcol, engine=engine, merge_cells=merge_cells, encoding=encoding, inf_rep=inf_rep, verbose=verbose, freeze_panes=freeze_panes)
        end = datetime.datetime.now()
        elapsed = end - start
        self.log.logs.info("Elapsed time for to_excel operation: " + str(elapsed))

    def to_feather(self, feather_file=None, use_category_names=1):
        """
        This method will create an Apache Arrow-based Feather file from VDATA.

        Usage:
            ddf.to_feather( feather_file = "test.feather" )
            ddf.to_feather( feather_file = "test.feather", use_category_names = 0 )

        Args:
            feather_file (str - optional): Path and name of feather file.
            use_category_names (int - optional): 1 = use category names, 0 = use category values.

        Outputs:
            Feather file.

        Returns:
            None
        """
        start = datetime.datetime.now()
        self.log.logs.info("Converting Dimensions VDATA to feather file")

        if (feather_file is None):
            original_filename = ntpath.basename(self.ddf)
            original_filename_without_extension = os.path.splitext(original_filename)[0]
            feather_file = os.path.normpath(original_filename_without_extension + ".feather")

        df = self.to_df(use_category_names)
        self.log.logs.info("Writing " + feather_file)
        df.reset_index().to_feather(feather_file)
        end = datetime.datetime.now()
        elapsed = end - start
        self.log.logs.info("Elapsed time for to_feather operation: " + str(elapsed))

    def _vacuum(self):
        """
        This method rebuilds the ddf, repacking it into a minimal amount of disk space.

        Args:
            None

        Returns:
            None
        """
        self.log.logs.info("Optimizing storage (rebuilding and repacking database file)")
        conn = sqlite3.connect(self.ddf)
        conn.execute("VACUUM")
        conn.close()

    def _add_index( self, newddf ):
        """
        This method adds a respondent serial index to table L1.
        
        Args:
            newddf (sqlite database): Name of the database to update
            
        Returns:
            None
        """
        self.log.logs.info( "Adding index Respondent_Serial_idx to table L1" )
        conn = sqlite3.connect( newddf )
        conn.execute( "CREATE INDEX Respondent_Serial_idx on L1([Respondent.Serial:L])" )
        conn.close()
        conn = None

    def extract_variable(self, source_column_name, extract_column_name, extract_column_label=None, create_new_text_field=False, overwrite=False, function=None):
        """
        This method extract a unique categorical variable or a text variable, with an optional translation using a user-defined function.

        Usage:
            ddf.extract_variable( r"Q9[{_5}].inn1", r"_NEWCOL", new_column_label = "New Column", function = myfunc )

        Args:
            source_column_name : Source VDATA Column - Must be a single, categorical Variable or a Text Variable.
            new_column_name : Destination TEXT column name. MUST be a simple column, not part of a grid.
            new_column_label (optional) : Destination column label [Defaults to column name]
            create_new_column (optional) : True if the new column doesn't already exists and should be created. Defaults to False.
            overwrite (optional) : True if the new column can be overwritten when it already exists. Defaults to False.
            function (optional) : f(<str>) -> <str> function to remap category names or the text value of source_column_name

        Outputs:
            Creates and populates the new column in the DDF file.

        Returns:
            None
        """
        start = datetime.datetime.now()
        self.log.logs.info(f"Starting extract operation - extracting {source_column_name} to {extract_column_name}")

        # Check that the columns specified as parameters exist in the DDF - exit with error when it is an issue
        err_msg = ""
        list_of_all_vars = self._list_of_all_var_names()

        if source_column_name.lower() not in list_of_all_vars:
            err_msg += f"Source column {source_column_name} not found in the MDD. "

        data_type = self._get_variable_datatype(source_column_name)
        if data_type not in [self._DATATYPE_CATEGORY, self._DATATYPE_TEXT]:
            err_msg += f"Source column {source_column_name} is not of type Categorical or Text. "

        # Is the new extract column a simple L1 table variable?
        if len(self._split_varname_components(extract_column_name)) > 1:
            err_msg += f"Destination variable {extract_column_name} is part of a grid, and should be a standalone text variable. "

        # Does the new extract column already exist - and is it a problem?
        var_exists = extract_column_name.lower() in list_of_all_vars
        if var_exists and (not overwrite):
            err_msg += f"MDD variable {extract_column_name} already exists and overwrite is set to False. "
        if (not var_exists) and (not create_new_text_field):
            err_msg += f"MDD variable {extract_column_name} doesn't exist and create_new_text_field is set to False. "

        if err_msg:
            err_msg = f"Error in MDD file : " + err_msg
            self.log.logs.error(err_msg)
            raise RuntimeError(err_msg)

        # Add or clear the destination text field, as appropriate
        if create_new_text_field and (not var_exists):
            self._add_text_field(extract_column_name, extract_column_label)
        elif overwrite and var_exists:
            self._clear_text_field(extract_column_name)

        if data_type == self._DATATYPE_CATEGORY:
            self._extract_category_name(source_column_name, extract_column_name, function)
        elif data_type == self._DATATYPE_TEXT:
            self._extract_text(source_column_name, extract_column_name, function)

        end = datetime.datetime.now()
        elapsed = end - start
        self.log.logs.info("Elapsed time for extract operation: " + str(elapsed))

    def _add_text_field(self, column_name, column_label=None):
        """
        This method adds an empty text field to a mdd/ddf pair.

        Args:
            column_name: Name of the column to add.
            column_label (optional): Label of the column to add, defaults to the columùn name.

        Returns:
            None
        """
        if (column_label is None):
            column_label = column_name
        # Need to add to the mdd and the ddf
        if ( not self.mdm.Fields[ column_name ] ):
            conn = sqlite3.connect(self.ddf)
            cur = conn.cursor()
            cur.execute( "ALTER TABLE L1 ADD COLUMN [" + column_name + ":X] text;" )
            conn.commit()
            conn.close()

            var = Variable( column_name, str( uuid.uuid4() ),2 , '1', '4000', 0, 0, self.mdm.Languages.Base, self.mdm.Contexts.Base )
            self.mdm.Fields._items[ column_name ] = var

            filename = ntpath.basename( self.mdd )
            location = ntpath.dirname( self.mdd )
            filename_only = os.path.splitext( filename )[0]
            new_filename = os.path.join( location, filename_only )

            shutil.move( self.mdd, new_filename + '.orig.mdd' )

            mdd_save = ipsos.dimensions.mdd.MDD( self.mdm, new_filename + '.mdd' )
            mdd_save.download_metadata_to_mdd( )
            mdd_save = None
        else:
            self.log.logs.warn( column_name + ' already exists in the mdd.' )

        self._list_of_all_var_names.cache_clear()    # pylint: disable=no-member
        self._list_of_exportable_var_names.cache_clear() # pylint: disable=no-member

        return

    def _extract_category_name(self, source_column, new_column_name, function):
        """
        This method extracts a unique categorical variable to an existing text variable,
        with an optional translation applying a user-defined function.

        Args:
            source_column_name : Source VDATA Column - Must be a single, categorical Variable
            new_column_name : Destination TEXT column name. MUST be an existing simple column, not part of a grid
            function (optional) : f(<str>) -> <str> function to remap category names of source_column_name

        Outputs:
            Populates the new column in the DDF file.

        Returns:
            None
        """
        category_map = self._get_column_category_map(source_column, function)
        self._update_new_field_from_category(source_column, new_column_name, category_map)

    def _extract_text(self, source_column, new_column_name, function):
        """
        This method extracts a unique text variable, with an optional translation using a user-defined function.

        Args:
            source_column_name : Source VDATA Column - Must be a single, categorical Variable or a Text Variable.
            new_column_name : Destination TEXT column name. MUST be a simple column, not part of a grid.
            function (optional) : f(<str>) -> <str> function to remap the text value of source_column_name

        Outputs:
            Populates the new column in the DDF file.

        Returns:
            None
        """
        self._update_new_field_from_text(source_column, new_column_name, function)

    def _update_new_field_from_category(self, source_column, new_column_name, category_map):
        """
        This method updates an existing text variable, mapping the internal values of a single
        categorical variable to text values as specified by a mapping dictionary.

        Args:
            source_column_name : Source VDATA Column - Must be a single, categorical Variable
            new_column_name : Destination TEXT column name. MUST be an existing simple column, not part of a grid
            category_map: Dictionary of internal category values (keys) pointing to strings (values)

        Outputs:
            Populates the new column in the DDF file.

        Returns:
            None
        """
        # Get the map of table and filters to get at the values we need to extract/convert
        table_filters = self._get_table_filters(source_column)
        if (table_filters[-1][4] > 1):
            # This is a multi-punch question, tell the user and leave.
            self.log.logs.error(f"Variable: {source_column} is a multi-punch variable - Extraction is not possible")
            return
        # <----------------

        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()

        # for grids and hierarchical columns
        if len(table_filters) > 1:
            # Use the filter description to get respondents for each value
            split_ids = self._get_split_ids(table_filters)

            # Update the new column
            for (value, ids) in split_ids.items():
                id_string = ", ".join([str(c) for c in ids])
                sql = f"UPDATE L1 SET [{new_column_name}:X] = '{category_map[value]}' WHERE [:P0] IN ({id_string})"
                self.log.logs.info(f"Executing SQL: {sql}.")
                cur.execute(sql)
                conn.commit()
        else:
            sql = f"SELECT DISTINCT [{source_column}:C1] FROM L1"
            cur.execute(sql)
            keys = set(r[0] for r in cur.fetchall())
            for (key, value) in category_map.items():
                if key in keys:
                    sql = f"UPDATE L1 SET [{new_column_name}:X] = '{value}' WHERE [{source_column}:C1] = {key}"
                    self.log.logs.info(f"Executing SQL: {sql}.")
                    cur.execute(sql)
                    conn.commit()

        cur.close()
        conn.close()

    def _update_new_field_from_text(self, source_column, new_column_name, function):
        """
        This method updates an existing text variable, mapping the values of a single
        text variable to values derived by an optional function. Defaults to the 
        original values if <function> is None.

        Args:
            source_column_name : Source VDATA Column - Must be a single, text Variable
            new_column_name : Destination TEXT column name. Must be an existing simple column, not part of a grid
            function (optional) : f(<str>) -> <str> function to remap values of source_column_name

        Outputs:
            Populates the new column in the DDF file.

        Returns:
            None
        """
        # Get the map of table and filters to get at the values we need to extract/convert
        table_filters = self._get_table_filters(source_column)
        if (table_filters[-1][4] > 1):
            # This is a multi-punch question, tell the user and leave.
            self.log.logs.error(f"Variable: {source_column} is a multi-punch variable - Extraction is not possible")
            return
        # <----------------

        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()

        # for grids and hierarchical columns
        if len(table_filters) > 1:
            # Use the filter description to get respondents for each value
            split_ids = self._get_split_ids(table_filters)

            # Update the new column
            for (value, ids) in split_ids.items():
                id_string = ", ".join([str(c) for c in ids])
                if function:
                    sql = f"UPDATE L1 SET [{new_column_name}:X] = '{function(value)}' WHERE [:P0] IN ({id_string})"
                else:
                    sql = f"UPDATE L1 SET [{new_column_name}:X] = '{value}' WHERE [:P0] IN ({id_string})"                
                self.log.logs.info(f"Executing SQL: {sql}.")
                cur.execute(sql)
                conn.commit()
        else:
            if function:
                sql = f"SELECT DISTINCT [{source_column}:X] FROM L1"
                cur.execute(sql)
                values = set(r[0] for r in cur.fetchall())
                for value in values:
                    sql = f"UPDATE L1 SET [{new_column_name}:X] = '{function(value)}' WHERE [{source_column}:X] = '{value}' "
                    self.log.logs.info(f"Executing SQL: {sql}.")
                    cur.execute(sql)
                    conn.commit()
            else:
                sql = f"UPDATE L1 SET [{new_column_name}:X] = [{source_column}:X] "
                self.log.logs.info(f"Executing SQL: {sql}.")
                cur.execute(sql)
                conn.commit()
        cur.close()
        conn.close()

    def _get_column_category_map(self, column_name, function=None):
        """
        This method returns a dictionary mapping the category item internal values of a categorical column 
        to their category name. If the optional <function> is specified, it maps instead the values to the 
        result of the expression <function>(<category item name>)

        Args:
            column name: A categorical column in the DDF
            function (optional) : f(<str>) -> <str> function to remap values of source_column_name

        Returns:
            The mapping dictionary as described above

        """
        if function:
            d = { c.Value: function(c.Name) for _, c in self.mdm.Fields[column_name].Categories.items() }
        else:
            d = { c.Value: c.Name for _, c in self.mdm.Fields[column_name].Categories.items() }

        return d

    def merge_csv(self, path_to_csv, ddf_join_column, csv_join_column, csv_column_name, ddf_variable_fullname, create_new_text_field=False, overwrite=False, new_text_field_label=None, sep=',', encoding='utf-8'):
        """
        This method extracts a unique categorical variable or a text variable to another text variable, using a CSV to remap
        the values of the original variable.

        Usage:
            ddf.merge_csv("C:/DATA/ASSETS/test.csv", "resp_age", "Age", "Age_redux", "AR3", create_new_text_field=True, overwrite=True )

        Args:
            path_to_csv: Path to the remapping CSV file
            ddf_join_column : Source VDATA Column - Must be a single, categorical Variable or a Text Variable.
            csv_join_column : Name of the column in the CSV with the original values that we are remapping FROM
            csv_column_name : Name of the column in the CSV with the result values that we are remapping TO
            ddf_variable_fullname : Destination TEXT column name. MUST be a simple column, not part of a grid.
            create_new_column (optional) : True if the new column should be created if it doesn't already exists. Defaults to False.
            overwrite (optional) : True if the new column can be overwritten when it already exists. Defaults to False.
            new_text_field_label (optional) : Label of the new taxt field, defaults to the colum name
            sep (optional) : CSV field separator defaults to ","
            encoding (optional) : CSV file encoding, defaults to UTF-8

        Outputs:
            Creates and populates the new column in the DDF file.

        Returns:
            None
        """
        # Load the CSV - exit with error if it doesn't exist
        try:
            df = pandas.read_csv(path_to_csv, sep=sep, encoding=encoding)
        except:
            self.log.logs.error(f"Unable to load file {path_to_csv}")
            raise

        # Check that the columns specified as parameters all exist in the CSV - exit with error if not
        # Note: Check needs to be case insensitive. We load the case found in the file to the name.
        err_msg = ""
        for c in df.columns:
            if csv_join_column.lower() == c.lower():
                csv_join_column = c
                break
        else:
            err_msg += f"Column {csv_join_column} not found.\n"

        for c in df.columns:
            if csv_column_name.lower() == c.lower():
                csv_column_name = c
                break
        else:
            err_msg += f"Column {csv_column_name} not found.\n"

        if err_msg:
            err_msg = f"Error reading CSV file {path_to_csv} :\n" + err_msg
            self.log.logs.error(err_msg)
            raise RuntimeError(err_msg)

        # Get a remapping dictionary from the CSV file
        remap_dict = dict(zip(df[csv_join_column].apply(str), df[csv_column_name].apply(str)))

        # Extract the remapped values from ddf_join_column to ddf_variable_fullname
        self.extract_variable(ddf_join_column, ddf_variable_fullname, new_text_field_label, create_new_text_field, overwrite, function=lambda s: remap_dict.get(s, ""))
            
        return

    def _clear_text_field(self, text_field_fullname):
        """
        This method clears the given text field, replacing every value with "".

        Args:
            text_field_fullname : Name of the text field. It must be a simple field, NOT part of a grid.

        Results:
            None
        """
        conn = sqlite3.connect(self.ddf)
        cur = conn.cursor()

        sql = f"UPDATE L1 SET [{text_field_fullname}:X] = ''"
        self.log.logs.info(f"Executing SQL: {sql}.")
        cur.execute(sql)

        cur.close()
        conn.close()

        return

    @ft.lru_cache(maxsize=256)
    def set_of_variable_names(self, *patterns, collapse=False):
        """
        This method returns the set of variable names that match the patterns specified, optionally
        collapsing the variable names to their "generic" form ('var[{_1}]' and 'var[{_2}]' are collapsed
        to 'var[..]'). Matches are CASE INSENSITIVE.

        Args:
            *patterns: 
                One or several patterns to match.
                Some useful, predefined patterns:
                SIMPLE_VAR matches the name of simple vars, i.e. NOT grid variables
                GRID matches grid variables, e.g. my[_22]._grid
                VALUE_GRID matches value/3d grid variables, e.g. my[_22].value[_1]._grid
                For the regex syntax see: https://docs.python.org/3/howto/regex.html
            collapse (optional):
                When True, returns the "collapsed" version of variable names, as described above.
                Defaults to False.

        Usage:
            s = set_of variable_names("respondent_")   # lists all variable names starting with "respondent_"
            s = set_of variable_names(GRID, VALUE_GRID, collapse = True)    # lists all grids and 3D grids
            s = set_of variable_names(collapse = True)    # lists all variables in generic form

        Results:
            Set of matching variable names
        """
        if collapse:
            # track indices in names if (collapse = True)
            index_regx = re.compile(self.INDEX_PATTERN, flags=re.IGNORECASE)

        result_set = set()
        if patterns:
            compiled_patterns = [re.compile(p, re.IGNORECASE) for p in patterns]

        for s in self._list_of_exportable_var_names():
            if patterns:
                for cp in compiled_patterns:
                    if cp.match(s):
                        result_set.add(index_regx.sub(r'[..]', s) if collapse else s)
                        break
            else:
                result_set.add(index_regx.sub(r'[..]', s) if collapse else s)

        return result_set

    def _convert_date( self, value ):
        date_val = ''
        dt = str( value ).split('.')
        
        if dt[0] != 'nan':
            date_val = datetime.date.fromordinal(693594 + int(dt[0])).strftime('%Y-%m-%d')
        
            if int(dt[1]) > 0:
                # If there is a decimal (time) convert it to time (h:m:s)
                date_val += ' ' + strftime( '%H:%M:%S', gmtime( int(86400 * float('.' + dt[1])) ) )

        return date_val

    def _export_data( self, col, rows, cat_map_dict, questions, use_category_names ):
        if ( rows ):
            try:
                for row in rows:
                    use_var = row[1]
                    value = None

                    if ( use_var.lower() == 'datacollection.finishtime' ):
                        # Required field in the respondent table
                        value = self._convert_date( row[ 2 ] )
                    else:
                        if ( str( col[0] ).endswith(':S') ):
                            # Multi-punch - convert value to name
                            if ( use_category_names == 1 ):
                                if ( row[2] != '-1' and row[2] is not None ):
                                    values = row[2].split( ';' )
                                    value = ''
                                    for response in values:
                                        if ( response != '' ):
                                            value += cat_map_dict.get( int( response ) ) + ';'
                            else:
                                value = row[2]
                        elif ( str( col[0] ).endswith(':C1') ):
                            # Simgle punch - convert value to name
                            if ( use_category_names == 1 ):
                                if ( row[2] != -1 ):
                                    value = cat_map_dict.get( int( row[2] ) )
                            else:
                                value = int( row[2] )
                        elif ( str( col[0] ).endswith(':T') ):
                            # Date values
                            value = self._convert_date( row[ 2 ] )
                        else:
                            value = row[ 2 ]

                    if ( use_var.find( '[' ) > -1 ):
                        if ( use_var.find( '{' ) > -1 ):
                            parts = use_var.split( '{' )
                            for part in parts:
                                if ( part.find( '}' ) > -1 ):
                                    ind = part[:part.find( '}' )]
                                    name = cat_map_dict.get( int( ind ) )
                                    use_var = use_var.replace( ind, name )

                    if ( value is not None ):
                        if ( str( value ).find( "'" ) > 0 ):
                            # Fixing text that has a ' in it
                            value = value.replace( "'", "''" )
                        if ( str( value ).find( '"' ) > 0 ):
                            # Fixing text that has a ' in it
                            value = value.replace( '"', "\'" )
                        if ( str( value ).find( "\n" ) > 0 ):
                            # Fixing text that has a return in it
                            value = str( value ).replace( "\r", "\\r" )
                            value = str( value ).replace( "\n", "\\n" )

                        if ( use_var in questions ):
                            self.resp_dict[ row[0] ][ use_var ] = value

            except (Exception) as error :
                print( 'Error:' + str( error ) )
                print( "ERROR on line " +  str( sys.exc_info()[-1].tb_lineno ) )
                
    def _get_casedata( self, questions_to_use, use_category_names ):
        start = datetime.datetime.now()
        questions = questions_to_use.split( ', ' )

        cat_map_dict = { }
        for key in self.mdm.CategoryMap._items:
            value = self.mdm.CategoryMap._items[ key ]
            cat_map_dict[ value ] = str( key ).lower()

        conn = sqlite3.connect( self.ddf )
        conn.text_factory = lambda b: b.decode(errors = 'ignore')
        c = conn.cursor()
        c.execute('PRAGMA main.page_size = 32768')
        c.execute('PRAGMA main.cache_size=10000')
        c.execute('PRAGMA main.locking_mode=EXCLUSIVE')
        c.execute('PRAGMA main.synchronous=OFF')
        c.execute('PRAGMA main.journal_mode=OFF')

        c.execute( "SELECT [:P0] FROM L1 ORDER BY [:P0];" )
        resps = c.fetchall()
        self.resp_dict = {}
        for resp in resps:
            self.resp_dict[ resp[0] ] = OrderedDict()

            for q in questions:
                self.resp_dict[ resp[0] ][ q ] = None

        # Get a list of non-L1 tables along with it's parent table and it's mdd name
        c.execute( "SELECT TableName, ParentName, DSCTableName FROM Levels WHERE TableName <> 'L1';" )
        tables = c.fetchall()

        # Get all of the columns in the L1 table except for the index (:P0)
        c.execute( "SELECT name FROM PRAGMA_TABLE_INFO('L1') WHERE name NOT Like '%:P%';" )
        cols = c.fetchall()

        # Cycle through the columns
        for col in cols:
            if ( self.mdm.Fields[ str( col[0][:col[0].find(':')] ) ] is not None ):
                skip_q = False
                if ( len( questions ) > 0 ):
                    if ( not str( col[0][:col[0].find(':')] ).lower() in questions ):
                        skip_q = True

                if ( not skip_q ):
                    # Get Respondent.Serial, the column name and the response
                    self.log.logs.info( "SELECT [:P0], '" + col[0][:col[0].find(':')] + "', [" + col[0] + "] from L1;" )
                    c.execute( "SELECT [:P0], '" + col[0][:col[0].find(':')].lower() + "', [" + col[0] + "] from L1;" )
                    rows = c.fetchall()

                    # Export the data to the csv file
                    self.log.logs.info( "Exporting " + str( col[0][:col[0].find(':')] ) )
                    self._export_data( col, rows, cat_map_dict, questions, use_category_names )

        # Cycle through all non-L1 tables
        table_dict = {}
        for table in tables:
            # Dictionary key = table name, value = table row
            table_dict[ table[ 0 ] ] = table

            # Get the response columns, not indexes or level ids
            c.execute( "SELECT name FROM PRAGMA_TABLE_INFO('" + table[0] + "') WHERE name NOT Like '%:P%' AND name NOT Like 'LevelId%';" )
            cols = c.fetchall()

            self.log.logs.info( table[0] )

            # Find the entire tree for this table
            parent = table[1]
            table_tree_list = []
            table_tree_list.insert( 0, table )
            while parent != 'L1':
                parent_data = table_dict.get( parent )
                table_tree_list.insert( 0, parent_data )
                parent = parent_data[1]

            # Cycle through the columns if there are any
            for col in cols:
                dscname_text = ""
                var_generic_name = ""
                join_text = ""
                letter = ""
                where_text = ""
                for i in range( 0, len( table_tree_list ) ):
                    parent_info = table_tree_list[i]

                    # Letter for join table reference
                    letter = chr( 65 + i )

                    # Find the LevelId column, could be C1 or L
                    c.execute( "SELECT name FROM PRAGMA_TABLE_INFO('" + parent_info[0] + "') WHERE name Like 'LevelId%';" )
                    levelID = c.fetchall()

                    # Check iterator type
                    tmp_var = None
                    if ( i == 0 ):
                        tmp_var = self.mdm.Fields[ parent_info[2] ]
                    else:
                        tmp_var = self.mdm.Fields[ var_generic_name + parent_info[2] ]

                    # Build the question
                    if ( tmp_var.IteratorType == '3' ):
                        dscname_text += parent_info[2] + "[' || " + letter + ".[" + levelID[0][0] + "] || ']."
                    else:
                        dscname_text += parent_info[2] + "[{' || " + letter + ".[" + levelID[0][0] + "] || '}]."
                    var_generic_name += parent_info[2] + "[..]."
                    
                    # Join with the parent table(s) to align indexes
                    if ( i > 0 ):
                        join_text += " JOIN "

                    join_text += parent_info[0] + " as " + letter

                    # Match all indexes
                    if ( i < len( table_tree_list ) -1 ):
                        for j in range( i + 1, -1, -1 ):
                            where_text += " AND " + letter + ".[:P" + str( j ) + "] = " + chr( 65 + i + 1 ) + ".[:P" + str( j + 1 ) + "]"

                if ( self.mdm.Fields[ var_generic_name + col[0][:col[0].find(':')] ] is not None ):
                    # Make sure that the variable exists in the metadata
                    
                    try:
                        # Get Respondent.Serial, build the full question name and get the responses
                        sql = "SELECT " + chr( len( table_tree_list ) + 65 ) + ".[:P0], '" + dscname_text.lower() + col[0][:col[0].find(':')].lower() + "', " + letter + ".[" + col[0] + "] FROM " + join_text + " JOIN L1 as " + chr( len( table_tree_list ) + 65 ) + " WHERE " + chr( len( table_tree_list ) + 65 ) + ".[:P0] = A.[:P1]" + where_text + ";"
                        self.log.logs.info( sql )
                        c.execute( sql )
                        rows = c.fetchall()

                        # Export the data to the csv file
                        self._export_data( col, rows, cat_map_dict, questions, use_category_names )
                    except (Exception) as error :
                        print( 'Error:' + str( error ) )
                        print( "ERROR on line " +  str( sys.exc_info()[-1].tb_lineno ) )
                        print( sql )

        # Close the cursor and connection
        c.close()
        conn.close()

        end = datetime.datetime.now()
        elapsed = end - start
        self.log.logs.info( "Elapsed time for to_dataset operation: " + str( elapsed ) )

    