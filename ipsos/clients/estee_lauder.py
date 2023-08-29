import xml.etree.ElementTree as ET
import json, datetime
import ipsos, ipsos.logs, ipsos.dimensions.mtd
import sqlite3
from slugify import slugify
import os


class EL:
    """ 
    This class contains methods specific to the Estee Lauder Client. 

    Usage:
        el = ipsos.clients.estee_lauder.EL( language, verbose_mode )

        example:
            language = 'ENU'
            verbose_mode = True
            el = ipsos.clients.estee_lauder.EL( language, verbose_mode )

            path_to_output_json = "./assets/Multithreadtest/Output Data/python_test_001.json"
            el.create_color_tables( path_to_output_json, remove_checking_db )

    Args: 
        language (str): The language in the mtd file to use for labels (default = ENU)
        verbose (boolean): When True - generates extensive logging of the process (default = False)

    Methods:
        create_color_tables( path_to_output_json, remove_checking_db ): Add the color tables to the JSON file.
    """

    def __init__( self, language = 'ENU', verbose = False ):
        self.language = language
        self.verbose = verbose

        # Set up the logger
        self.log = ipsos.logs.Logs( name = 'estee_lauder', verbose = verbose )

    def _get_side( self, table, tmp_list, db_color_data ):
        """
        This method goes through the side in the json file in order to return all
        of the categories (side elements) in the table.
        
        Args:
            table (dictionary): Dictionary of the current table JSON.
            tmp_list (list): Holds the data for a level of the table.
            db_color_data (dictionary): Holds color table data.

        Return:
            None.
        """
        # Get the items in the axis
        # item = table 'Side'
        for sub_item in table:
            color_data_row = []

            # Get the label for the element
            #   The default label
            self.log.logs.info( "Catgeory: " + str( sub_item[ 'break' ] ) )

            is_base = False
            if ( str( sub_item[ 'break' ] ).find( 'Base:' ) > -1 ):
                is_base = True

            color_data_row.append( sub_item[ 'break' ] )
                
            for row in range( 0, len( table[ 0 ][ 'rowdata' ] ) ):
                if ( is_base ): color_data_row.append( sub_item[ 'rowdata' ][ row ][ 'value' ] )
                if ( is_base == False): color_data_row.append( sub_item[ 'rowdata' ][ row ][ 'percent' ] )

            db_color_data.append( color_data_row )

    def _generate_tables( self, mtd_json_dict, path_to_output_json, db_color_data, new_table_banner_dict, num_color_tables, num_banner_pts, remove_checking_db ):
        """
        This method populates a sqlite database with the imagery data, does various calculations and
        then generates the new color data tables.
        
        Args:
            mtd_json_dict (dictionary): A dictionary holding all of the tables, will be updated with the color tables.
            path_to_output_json (str): The path/name of the json file, used as the name of the sqlite database.
            db_color_data (dictionary): A dictionary holding the imagery data.
            new_table_banner_dict (dictionary): A dictionary holding the main banner for the tables.
            num_color_tables (int): The number of sqlite tables to create.
            num_banner_pts (int): Total number of banner points.
            remove_checking_db (boolean): Whether to keep the sqlite database after the process has finished.

        Returns:
            None.
        """
        db_file = path_to_output_json + '.db'
        
        brand_names = list( db_color_data.keys() )
        # Remove 'None of these brands'
        brand_names.pop()
        
        # Remove the database if it already exists
        if ( os.path.exists( db_file ) ):
            os.remove( db_file )

        conn = sqlite3.connect( db_file )
        c = conn.cursor()
        
        for i in range( 0, num_color_tables ):
            sql = 'CREATE TABLE L' + str( i + 1 ) + ' ([statement] TEXT, '
            
            for item in brand_names:
                sql += '[' + slugify( item ) + '] REAL, '
                
            sql += '[st_avg50] REAL, '
            sql += '[st_avg] REAL'
            sql += ');'
            c.execute( sql )

            sql = 'CREATE TABLE L' + str( i + 1 ) + '_calc ([statement] TEXT, '
            
            for item in brand_names:
                sql += '[' + slugify( item ) + '] TEXT, '
                
            sql = sql[:-2] + ');'
            c.execute( sql )
            
        conn.commit()
                
        statement = 0
        sql = ""
        brands50_to_use = {}
        brands_to_use = ''
        blank_table = ''
        num_brands = len( brand_names )
        for item in brand_names:
            brands_to_use += "[" + slugify( item ) + "]+"
            blank_table += "[" + slugify( item ) + "] = '99',"
        brands_to_use = brands_to_use[:-1]
        blank_table = blank_table[:-1]
        
        for i in range( 0, num_color_tables ):
            brands50_to_use[ 'L' + str( i + 1 ) ] = ''
            
        # remove 'None of these brands' table data
        db_color_data.popitem()
        cnt = 0
        statements = []
        for key, val in db_color_data.items():
            brand_name = key
            color_data = val
            statement += 1

            # Remove None and Sigma
            color_data.pop()
            color_data.pop()
            for row in color_data:
                label = ''
                tab = -1
                for item in row:
                    tab += 1
                    cnt += 1
                    
                    if ( tab == 0 ):
                        label = item

                    if ( statement == 1 and tab == 0 ):
                        statements.append( label )
                        
                        for i in range( 0, num_color_tables ):
                            c.execute( "INSERT INTO L" + str( i + 1 ) + " (statement) VALUES( ? );", [ label ] )
                            c.execute( "INSERT INTO L" + str( i + 1 ) + "_calc (statement) VALUES( ? );", [ label ] )

                    if ( tab > 0 ):
                        if ( label.find( 'Base:' ) > -1 ):
                            if ( int( item.replace('-', '0') ) >= 50 ):
                                brands50_to_use[ 'L' + str( tab ) ] += '[' + slugify( brand_name ) + ']+'

                        c.execute( "UPDATE L" + str( tab ) + " SET [" + slugify( brand_name ) + "] = ? WHERE [statement] = ?;", [ item.replace('%', '').replace('-', '0.0'), label ] )
                    
                    if ( tab == num_color_tables ): 
                        break

        for i in range( 0, num_color_tables ):
            c.execute( "INSERT INTO L" + str( i + 1 ) + " (statement) VALUES( 'brand_sum' );" )

        conn.commit()
        
        # Do the calculations
        # Cycle through tables
        for i in range( 0, num_color_tables ):
            norm_avg = 0.0
            if ( brands50_to_use['L' + str( i + 1 )] == '' ):
                # All brands have a base < 50
                c.execute( "UPDATE L" + str( i + 1 ) + "_calc SET " + blank_table + ";" )
            else:
                for item in statements:
                    c.execute( "UPDATE L" + str( i + 1 ) + " SET [st_avg] = ( select sum( " + brands_to_use + " )/" + str( num_brands ) + " from L" + str( i + 1 ) + " where [statement] = ? ) where [statement] = ?;", [ item, item ] )
                    c.execute( "UPDATE L" + str( i + 1 ) + " SET [st_avg50] = ( select sum( " + brands50_to_use['L' + str( i + 1 )][:-1] + " )/" + str( len( str( brands50_to_use[ 'L' + str( i + 1 ) ][:-1] ).split( '+' ) ) ) + " from L" + str( i + 1 ) + " where [statement] = ? ) where [statement] = ?;", [ item, item ] )

                #Cycle through columns
                for item in brand_names:
                    c.execute( "UPDATE L" + str( i + 1 ) + " SET [" + slugify( item ) + "] = ( select avg( [" + slugify( item ) + "] ) from L" + str( i + 1 ) + " where not( [statement] like 'Base:%' ) ) WHERE [statement] = 'brand_sum';" )

                # Get the normalized average
                c.execute( "SELECT avg( [st_avg] ) FROM L" + str( i + 1 ) + " WHERE not( [statement] like 'Base:%' or [statement] = 'brand_sum' );" )
                rows = c.fetchall()
                norm_avg = str( rows[ 0 ] )[1:-2]

                for item in brand_names:
                    c.execute( "SELECT [statement], CASE WHEN [" + slugify( item ) + "] >= [st_avg50] THEN '1' ELSE '0' END tmp FROM L" + str( i + 1 ) + ";" )
                    rows = c.fetchall()
                    for row in rows:
                        c.execute( "UPDATE L" + str( i + 1 ) + "_calc SET [" + slugify( item ) + "] = ? WHERE [statement] = ?;", [ row[ 1 ], row[ 0 ] ] )
                
                    c.execute( "SELECT [" + slugify( item ) + "] FROM L" + str( i + 1 ) + " WHERE [statement] = 'brand_sum';" )
                    rows = c.fetchall()
                    brand_sum = str( rows[ 0 ] )[1:-2]
                
                    c.execute( "SELECT [statement], CASE WHEN [" + slugify( item ) + "] - ((" + brand_sum + "*[st_avg])/" + norm_avg + ") >= 4 THEN '1' ELSE '0' END tmp FROM L" + str( i + 1 ) + ";" )
                    rows = c.fetchall()
                    for row in rows:
                        c.execute( "UPDATE L" + str( i + 1 ) + "_calc SET [" + slugify( item ) + "] = [" + slugify( item ) + "] || ? WHERE [statement] = ?;", [ row[ 1 ], row[ 0 ] ] )
                
                for item in statements:
                    # Update the statement ratings to 99 for any statement that has a sum of 0
                    c.execute( "UPDATE L" + str( i + 1 ) + "_calc SET " + blank_table + " WHERE [statement] = ? and ( select sum( " + brands_to_use + " ) from L" + str( i + 1 ) + " where [statement] = ? ) = 0.0;", [ item, item ] )

            for item in brand_names:
                # Update the base size in the _calc tables
                c.execute( "UPDATE L" + str( i + 1 ) + "_calc SET [" + slugify( item ) + "] = ( select [" + slugify( item ) + "] from L" + str( i + 1 ) + " where [statement] like 'Base:%' ) WHERE [statement] like 'Base:%';")
                # Update the statement ratings to 99 for any brand that has a base < 50
                c.execute( "UPDATE L" + str( i + 1 ) + "_calc SET [" + slugify( item ) + "] = '99' WHERE (select cast([" + slugify( item ) + "] as float) from L" + str( i + 1 ) + "_calc where [statement] like 'Base:%') < 50.0 and not ([statement] like 'Base:%');")
                                                                                                
        conn.commit()
        
        # Write the data out to the JSON file.
        title_template = "Q19_{}. [{}] Imagery Color <BR/> For each statement, please select the FSC brand(s) that you feel best applies to each statement."
        table_name_template = "ColorTable{}"
        
        cnt = 0
        for item in brand_names:
            cnt += 1
            
            # For the current table.
            tmp_table_dict = {}

            # Set the table name and description.
            tmp_table_dict[ 'tablename' ] = table_name_template.format( str( cnt ) )
            tmp_table_dict[ 'comments' ] = title_template.format( str( cnt ), item )

            # Set the stats description into the table dictionary.
            tmp_table_dict[ 'stattestdescription' ] = ''

            # Set the table filter description into the table dictionary.
            tmp_table_dict[ 'tablefilter' ] = ''

            # Find table level weights
            tmp_table_dict[ 'tableweight' ] = ''

            banner_dict = {}
            banner_dict[ 'Banner' ] = new_table_banner_dict[ 'Banner' ]
            banner_dict[ 'Side' ] = []

            cnt2 = 0
            for statement in statements:
                cnt2 += 1
                side_dict = {}
                side_dict[ 'break' ] = ''
                side_dict[ 'rowdata' ] = []
                side_dict[ 'subbreaks' ] = []
                
                for i in range( 0, num_color_tables ):
                    if ( i == 0 ): side_dict[ 'break' ] = statement
                        
                    c.execute( "SELECT [" + slugify(item) + "] FROM L" + str( i + 1 ) + "_calc WHERE [statement] = ?;", [ statement ] )
                    rows = c.fetchall()
                    
                    sub_dict = {}
                    sub_dict[ 'percent' ] = ''
                    if ( cnt2 == 1 ):
                        sub_dict[ 'value' ] = int( float( str( rows[ 0 ] )[1:-2].replace( "'", "" ) ) )
                    else:
                        sub_dict[ 'value' ] = str( rows[ 0 ] )[1:-2]
                    sub_dict[ 'statresult' ] = ''

                    side_dict[ 'rowdata' ].append( sub_dict )
                    
                for i in range( num_color_tables, num_banner_pts ):
                    # These are ethnicity tables, fill with blanks
                    sub_dict = {}
                    sub_dict[ 'percent' ] = ''
                    sub_dict[ 'value' ] = ''
                    sub_dict[ 'statresult' ] = ''
                    side_dict[ 'rowdata' ].append( sub_dict )
                    
                banner_dict[ 'Side' ].append( side_dict )
                    
            tmp_table_dict[ 'data' ] = banner_dict
            mtd_json_dict[ 'tables' ].append( tmp_table_dict )
        
        conn.close()
        
        if ( remove_checking_db ):
            if ( os.path.exists( db_file ) ):
                os.remove( db_file )

    def _get_banner_info( self, banner, skip = False ):
        """
        This method determines the total number of banner points and the number of banner points to process.
        
        Args:
            banner (list): A list of dictionary items (the banner points).
            skip (boolean): When True - Does not include the banner point in the process count (default = False).
            
        Returns:
            num_color_tables (int): Number of banner points to process.
            num_banner_pts (int): Total number of banner points.
        """
        num_color_tables = 0
        num_banner_pts = 0

        for ban_pt in banner:
            skip_color = skip
            if ( ban_pt[ 'break' ].upper().find( 'ETHNICITY' ) > -1 ): skip_color = True
            if ( len( ban_pt[ 'subbreaks' ] ) == 0 ):
                num_banner_pts += 1
                if ( not skip_color ): num_color_tables += 1
            else:
                t_color, t_tot = self._get_banner_info( ban_pt[ 'subbreaks' ], skip_color )
                num_color_tables += t_color
                num_banner_pts += t_tot

        return num_color_tables, num_banner_pts

    def create_color_tables( self, path_to_output_json, remove_checking_db = True ):
        """
        This method is the entry point for creating the color tables to add to a JSON file.
        
        Usage:
            path_to_output_json = "./assets/Multithreadtest/Output Data/python_test_001.json"
            el.create_color_tables( path_to_output_json )

        Args:
            path_to_output_json (str): The path to the JSON file.
            remove_checking_db (boolean): When True - Remove the checking database (default = True).
            
        Outputs:
            JSON file.

        Returns:
            None
        """
        start = datetime.datetime.now()
        self.log.logs.info( "Starting to create color tables for the JSON file." )
        mtd_json_dict = {}
        mtd_json_dict[ 'tables' ] = []

        # Traverse through the tables
        c = 0
        new_table_banner_dict = {}
        db_color_data = {}
        num_color_tables = 0
        num_banner_pts = 0

        # Get the banner info from the first table in the JSON file
        with open( path_to_output_json ) as j_file:
            mtd_json_dict = json.load( j_file )
        tables = mtd_json_dict.get( 'tables' )
        new_table_banner_dict[ 'Banner' ] = tables[ 0 ][ 'data' ][ 'Banner' ]
        num_color_tables, num_banner_pts = self._get_banner_info( new_table_banner_dict[ 'Banner' ] )

        for item in tables:
            imagery_brand = ''
    
            self.log.logs.info( "Converting table " + str( item[ 'tablename' ] ) )
            # Check description to see if it is an imagery table
            if ( str( item[ 'comments' ] ).find( '] Imagery' ) > 0 ):
                c += 1
                imagery_brand = str( item[ 'comments' ] )[ str( item[ 'comments' ] ).find( '[' ) + 1:str( item[ 'comments' ] ).find( ']' ) ]
            
                # Get the side
                tmp_list = [ None ] * ( len( item[ 'data' ][ 'Side' ] ) + 1 )
                tmp_color_data = []
                self.log.logs.info( "Analyzing " + imagery_brand )
                self._get_side( item[ 'data' ][ 'Side' ], tmp_list, tmp_color_data )

                db_color_data[ imagery_brand ] = tmp_color_data

        # Make sure that there are color tables to add.
        if ( c > 0 ):
            self._generate_tables( mtd_json_dict, path_to_output_json, db_color_data, new_table_banner_dict, num_color_tables, num_banner_pts, remove_checking_db )

            # Remove the json file.
            if ( os.path.exists( path_to_output_json ) ):
                os.remove( path_to_output_json )

            self.log.logs.info( "Exporting the JSON file." )
            with open( path_to_output_json, 'a' ) as j_file:
                json.dump( mtd_json_dict, j_file )

        end = datetime.datetime.now()
        elapsed = end - start
        self.log.logs.info( "Elapsed time for color table conversion: " + str( elapsed ) )
