import ipsos.logs
import sqlite3


class GOOGLE:
    """ 
    This class contains methods specific to the Google Client. 

    Usage:
        google = ipsos.clients.google.GOOGLE( verbose_mode )

        example:
            verbose_mode = True
            google = ipsos.clients.google.GOOGLE( verbose_mode )

            path_to_ddf = "./assets/Multithreadtest/Input Data/python_test_001.ddf"
            path_to_output_csv = "./assets/Multithreadtest/Output Data/python_test_001.csv"
            google.create_stacked_data( path_to_ddf, path_to_output_csv )

    Args: 
        verbose (boolean): When True - generates extensive logging of the process (default = False)

    Methods:
        create_stacked_data( path_to_ddf, path_to_output_csv ): Convert Dimensions data to a stacked format
    """

    def __init__( self, verbose = False ):
        self.verbose = verbose

        # Set up the logger
        self.log = ipsos.logs.Logs( name = 'google', verbose = verbose )

    def _export_csv( self, col, rows, f ):
        """
        This method gets the data and writes it out to the csv file.
        
        Args:
            col (str): The name of the data column being exported.
            rows (SQLite rowset): All rows of data for given column.
            f (str): The csv file object.
            
        Returns:
            None
        """
        # Export the data to a csv file based on the question type
        if ( rows ):
            if ( str( col[0] ).endswith(':S') ):
                # Categorical - Multi-punch
                #   Split out the multi-punch so that each response is in it's own row
                for row in rows:
                    values = row[2].split( ';' )
                    for x in range( 0, len(values) - 1 ):
                        f.write( str( row[0] ) + "\t'" + str( row[1] ) + "'\t" + str( values[ x ] ) + "\n")
            elif ( str( col[0] ).endswith(':X') ):
                # Text responses
                for row in rows:
                    f.write( str( row[0] ) + "\t'" + str( row[1] ) + "'\t'" + str( row[2] ).replace( '\t', '   ' ) + "'\n")
            else:
                # All other responses
                #    Output the result set directly, no need to cycle through it
                for row in rows:
                    f.write( str( row[0] ) + "\t'" + str( row[1] ) + "'\t" + str( row[2] ) + "\n")

    def create_stacked_data( self, path_to_ddf, path_to_output_csv ):
        """
        This method is the entry point for creating the stacked tab-delimited csv file.
        
        Usage:
            path_to_ddf = "./assets/Multithreadtest/Input Data/python_test_001.ddf"
            path_to_output_csv = "./assets/Multithreadtest/Output Data/python_test_001.csv"
            google.create_stacked_data( path_to_ddf, path_to_output_csv )

        Args:
            path_to_ddf (str): The path to the ddf file.
            path_to_output_csv (str): The path to the output csv file.
            
        Outputs:
            tab-delimited csv file.

        Returns:
            None
        """
        # Open the ddf
        conn = sqlite3.connect( path_to_ddf )
        c = conn.cursor()

        # Get a list of non-L1 tables along with it's parent table and it's mdd name
        c.execute( "SELECT TableName, ParentName, DSCTableName FROM Levels WHERE TableName <> 'L1';" )
        tables = c.fetchall()

        # Write out results to a file
        with open( path_to_output_csv, 'w', encoding='utf-8' ) as f:
            f.write( "Serial\tVariableId\tResponse\n" )

            # Get all of the columns in the L1 table except for the index (:P0)
            c.execute( "SELECT name FROM PRAGMA_TABLE_INFO('L1') WHERE name NOT Like '%:P%';" )
            cols = c.fetchall()

            # Cycle through the columns
            for col in cols:
                # Get Respondent.Serial, the column name and the response
                self.log.logs.info( "SELECT [Respondent.Serial:L], '" + col[0][:col[0].find(':')] + "', [" + col[0] + "] from L1 WHERE [" + col[0] + "] IS NOT NULL;" )
                c.execute( "SELECT [Respondent.Serial:L], '" + col[0][:col[0].find(':')] + "', [" + col[0] + "] from L1 WHERE [" + col[0] + "] IS NOT NULL;" )
                rows = c.fetchall()

                # Export the data to the csv file
                self._export_csv( col, rows, f )

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
                        # Build the question
                        dscname_text += parent_info[2] + "[{' || " + letter + ".[" + levelID[0][0] + "] || '}]."

                        # Join with the parent table(s) to align indexes
                        if ( i > 0 ):
                            join_text += " JOIN "

                        join_text += parent_info[0] + " as " + letter

                        # Match all indexes
                        if ( i < len( table_tree_list ) -1 ):
                            for j in range( i + 1, -1, -1 ):
                                where_text += " AND " + letter + ".[:P" + str( j ) + "] = " + chr( 65 + i + 1 ) + ".[:P" + str( j + 1 ) + "]"

                    # Get Respondent.Serial, build the full question name and get the responses
                    sql = "SELECT " + chr( len( table_tree_list ) + 65 ) + ".[Respondent.Serial:L], '" + dscname_text + col[0][:col[0].find(':')] + "', " + letter + ".[" + col[0] + "] FROM " + join_text + " JOIN L1 as " + chr( len( table_tree_list ) + 65 ) + " WHERE " + chr( len( table_tree_list ) + 65 ) + ".[:P0] = A.[:P1]" + where_text + " AND " + letter + ".[" + col[0] + "] IS NOT NULL;"
                    self.log.logs.info( sql )
                    c.execute( sql )
                    rows = c.fetchall()

                    # Export the data to the csv file
                    self._export_csv( col, rows, f )

        # Close the cursor and connection
        c.close()
        conn.close()
        self.log.logs.info( 'Done' )
