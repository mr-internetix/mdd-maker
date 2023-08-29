import xml.etree.ElementTree as ET
import json, datetime
import ipsos, ipsos.logs


class MTD:
    """ 
    This class contains methods for processing Dimensions MTD (tables) files. 

    Usage:
        mtd = ipsos.dimensions.mtd.MTD( path_to_mtd, language, verbose_mode )

        example:
            path_to_mtd = "./assets/Multithreadtest/Input Data/python_test_001.mtd"
            language = 'ENU'
            verbose_mode = True
            mtd = ipsos.dimensions.mtd.MTD( path_to_mtd, language, verbose_mode )

            path_to_output_json = "./assets/Multithreadtest/Output Data/python_test_001.json"
            mtd.convert_mtd_to_json( path_to_output_json )

    Args: 
        path_to_mtd (str): The path to the mtd file. 
        language (str): The language in the mtd file to use for labels (default = ENU)
        verbose (boolean): When True - generates extensive logging of the process (default = False)

    Methods:
        convert_mtd_to_json( path_to_output_json ): Convert the specified MTD file to a JSON file.
    """

    def __init__( self, path_to_mtd, language = 'ENU', verbose = False ):
        self.mtd = path_to_mtd
        self.language = language
        self.verbose = verbose
        self.mtd_json_dict = {}

        # Set up the logger
        self.log = ipsos.logs.Logs( name = 'mtd', verbose = verbose )

    def _get_filter( self, item ):
        """ 
        This method returns the description for a defined filter, if there is no description it will return
        the expression for the filter.
        
        Args:
            item (xml node): A node in the mtd file that holds a filter.
            
        Return:
            filt (str): The filter description or expression.
        """
        filt = ''
        
        self.log.logs.info( "Retrieve the filters." )
        # Check to see if there is a description.
        if ( 'Description' in item.attrib ):
            if ( len( str( item.attrib[ 'Description' ] ).strip() ) > 0 ):
                filt += '(' + str( item.attrib[ 'Description' ] ).strip() + ') AND '
            else:
                # If there is no description, get the expression.
                filt += '(' + str( item.attrib[ 'Expression' ] ).strip() + ') AND '
        else:
            # If there is no description, get the expression.
            filt += '(' + str( item.attrib[ 'Expression' ] ).strip() + ') AND '
        
        return filt

    def get_side_headers( self, item, header_dict, nodes, breadcrumb, cntr ):
        """ 
        This method updates a dictionary that stores the order that each element should be displayed in.
        
        Args:
            item (xml node): A node in the mtd file that holds the side axes.
            header_dict (dictionary): Holds the order that items are shown, may be different than the element order if the table was sorted.
            nodes (str): The nodes to look at.
            breadcrumb (str): Used to help keep track of the hierarchy of nested elements.
            cntr (int): Used to help keep track of order.

        Return:
            None
        """
        self.log.logs.info( "Finding the side headers to determine the category order." )
        ctr = cntr
        ctr2 = 0
        bc = breadcrumb
        for head_item in item.findall( './' + nodes + '/Heading' ):
            if ( nodes == 'ElementHeadings' ): bc = item.attrib[ 'Name' ]

            ctr += 1
            ctr2 += 1
            if ( bc == '' ):
                header_dict[ head_item.attrib[ 'Name' ] ] = str( ctr ) + ':' + str( ctr2 )
            else:
                header_dict[ bc + '_' + head_item.attrib[ 'Name' ] ] = str( ctr ) + ':' + str( ctr2 )

            # Check to see if there is a header nested under this heading
            sub_header = head_item.find( 'SubElementHeadings' )

            if ( sub_header ):
                if ( bc == '' ):
                    bc = head_item.attrib[ 'Name' ]
                else:
                    bc += '_' + head_item.attrib[ 'Name' ]
                ctr = self.get_side_headers( head_item, header_dict, 'SubElementHeadings', bc, ctr )

        return ctr

    def _get_banner( self, item, banner, is_hidden_dict, cnt, c, parent=None ):
        """ 
        This method goes through the banner in the mtd file recursively in order to return all
        of the variables regardless of the level in the banner.
        
        Args:
            item (xml node): An axis item in the mtd file.
            banner (dictionary): Holds the individual banner parts.
            is_hidden_dict (dictionary): Determines if a column in the table should be displayed or not.
            cnt (int): This represents the current column and is used with is_hidden_dict.
            c (int): Count for which table is being processed.

        Return:
            banner (dictionary): The current banner part.
        """
        # Traverse each axis (variable) in the banner.
        for axis in item.findall( './SubAxes/Axis' ):
            # Setup a temporary dictionary for the current variable
            ban_dict = {}
            ban_dict[ 'break' ] = ''
            ban_dict[ 'statletter' ] = ''
            ban_dict[ 'subbreaks' ] = []
        
            self.log.logs.info( "Analyzing banner axis " + str( axis.attrib[ 'Label' ] ) )
            # Get the label for the axis
            #   The default label
            ban_dict[ 'break' ] = axis.attrib[ 'Label' ]

            #   Language specific label if it exists
            for sub_item in axis.findall( './Labels/Label' ):
                if ( sub_item.attrib[ 'Language' ] == self.language ):
                    ban_dict[ 'break' ] = sub_item.attrib[ 'Text' ]

            # Get the elements (categories) in the current axis
            for count, sub_item in enumerate( axis.findall( './Elements/Element' ), start=0 ):
                # Setup a temporary dictionary for the current element.
                sub_dict = {}
                sub_dict[ 'break' ] = ''
                sub_dict[ 'statletter' ] = ''
                sub_dict[ 'subbreaks' ] = []
                
                self.log.logs.info( "Column " + str( sub_item.attrib[ 'Name' ] ) )
                # Get the name of the element, need this to get the stat letter, if it has one.
                name = sub_item.attrib[ 'Name' ]

                # Check to see if the element is being displayed
                is_hidden_dict[ cnt ] = False
                if ( 'ShownOnTable' in sub_item.attrib ):
                    if ( sub_item.attrib[ 'ShownOnTable' ] == 'false' ):
                        is_hidden_dict[ cnt ] = True

                # Update the column counter.
                cnt += 1
                
                if ( not is_hidden_dict.get( cnt - 1 ) ):
                    # Get the label for the element
                    #   The default label
                    sub_dict[ 'break' ] = sub_item.attrib[ 'Label' ]
                    #   Language specific label if it exists
                    for lab_item in sub_item.findall( './Labels/Label' ):
                        if ( lab_item.attrib[ 'Language' ] == self.language ):
                            sub_dict[ 'break' ] = lab_item.attrib[ 'Text' ]

                    # Find the stat letter for this element
                    for head_item in axis.findall( './ElementHeadings/Heading' ):
                        if ( head_item.attrib[ 'Name' ] == name ):
                            if ( 'HeadingId' in head_item.attrib ):
                                sub_dict[ 'statletter' ] = head_item.attrib[ 'HeadingId' ]
                            else:
                                sub_dict[ 'statletter' ] = ''
                            break

                    ban_dict[ 'subbreaks' ].append( sub_dict )

                # Check to see if there is a variable nested under this variable
                self.log.logs.info( "Check to see if there is a variable nested under this variable" )
                sub_axes = axis.find( 'SubAxes' )

                if ( sub_axes ):
                    # If there is a nested variable, recursively call this method to get its information.
                    cnt = self._get_sub_banner( axis, banner, is_hidden_dict, cnt - 1, c, count, sub_dict, is_hidden_dict.get( cnt - 1 ) )
                            
            # Add the variables dictionary to the banner.
            self.log.logs.info( "Update the banner object" )

            if ( not parent ):
                banner[ 'Banner' ].append( ban_dict )

    def _get_sub_banner( self, item, banner, is_hidden_dict, cnt, c, iter, parent, parent_hidden ):
        """ 
        This method goes through the banner in the mtd file recursively in order to return all
        of the variables regardless of the level in the banner.
        
        Args:
            item (xml node): An axis item in the mtd file.
            banner (dictionary): Holds the individual banner parts.
            is_hidden_dict (dictionary): Determines if a column in the table should be displayed or not.
            cnt (int): This represents the current column and is used with is_hidden_dict.
            c (int): Count for which table is being processed.

        Return:
            banner (dictionary): The current banner part.
        """
        # Traverse each axis (variable) in the banner.
        for axis in item.findall( './SubAxes/Axis' ):
            # Setup a temporary dictionary for the current variable
            ban_dict = {}
            ban_dict[ 'break' ] = ''
            ban_dict[ 'statletter' ] = ''
            ban_dict[ 'subbreaks' ] = []
        
            self.log.logs.info( "Analyzing banner axis " + str( axis.attrib[ 'Label' ] ) )
            # Get the label for the axis
            #   The default label
            ban_dict[ 'break' ] = axis.attrib[ 'Label' ]

            #   Language specific label if it exists
            for sub_item in axis.findall( './Labels/Label' ):
                if ( sub_item.attrib[ 'Language' ] == self.language ):
                    ban_dict[ 'break' ] = sub_item.attrib[ 'Text' ]

            # Get the elements (categories) in the current axis
            for count, sub_item in enumerate( axis.findall( './Elements/Element' ), start=0 ):
                # Setup a temporary dictionary for the current element.
                sub_dict = {}
                sub_dict[ 'break' ] = ''
                sub_dict[ 'statletter' ] = ''
                sub_dict[ 'subbreaks' ] = []
                
                self.log.logs.info( "Column " + str( sub_item.attrib[ 'Name' ] ) )
                # Get the name of the element, need this to get the stat letter, if it has one.
                name = sub_item.attrib[ 'Name' ]

                # Check to see if the element is being displayed
                is_hidden_dict[ cnt ] = False
                if ( 'ShownOnTable' in sub_item.attrib ):
                    if ( sub_item.attrib[ 'ShownOnTable' ] == 'false' ):
                        is_hidden_dict[ cnt ] = True
                if ( parent_hidden ): is_hidden_dict[ cnt ] = True

                # Update the column counter.
                cnt += 1
                
                if ( not is_hidden_dict.get( cnt - 1 ) ):
                    # Get the label for the element
                    #   The default label
                    sub_dict[ 'break' ] = sub_item.attrib[ 'Label' ]
                    #   Language specific label if it exists
                    for lab_item in sub_item.findall( './Labels/Label' ):
                        if ( lab_item.attrib[ 'Language' ] == self.language ):
                            sub_dict[ 'break' ] = lab_item.attrib[ 'Text' ]

                    # Find the stat letter for this element
                    elem_cnt = 0
                    for head_item in axis.findall( './ElementHeadings/Heading' ):
                        if ( head_item.attrib[ 'Name' ] == name ):
                            if ( elem_cnt == iter ):
                                if ( 'HeadingId' in head_item.attrib ):
                                    sub_dict[ 'statletter' ] = head_item.attrib[ 'HeadingId' ]
                                else:
                                    sub_dict[ 'statletter' ] = ''
                                break
                            elem_cnt += 1

                    ban_dict[ 'subbreaks' ].append( sub_dict )

                    # Check to see if there is a variable nested under this variable
                    self.log.logs.info( "Check to see if there is a variable nested under this variable" )
                    sub_axes = axis.find( 'SubAxes' )

                    if ( sub_axes ):
                        # If there is a nested variable, recursively call this method to get its information.
                        cnt = self._get_sub_banner( axis, banner, is_hidden_dict, cnt - 1, c, count, ban_dict, is_hidden_dict.get( cnt - 1 ) )
                            
            parent[ 'subbreaks' ].append( ban_dict )

        return cnt

    def _get_side( self, item, side, parent_dict, table, is_hidden_dict, cell_items_dict, header_dict, tmp_list, nodes, breadcrumb ):
        """
        This method goes through the side in the mtd file in order to return all
        of the categories (side elements) in the table.
        
        Args:
            item (xml node): An axis item in the mtd file.
            side (dictionary): Holds the individual side parts.
            table (xml node): The current table.
            is_hidden_dict (dictionary): Determines if a column in the table should be displayed or not.
            cell_items_dict (dictionary): Holds the cell items (value/percaentage/stats).
            header_dict (dictionary): Holds the order that the elements should be output in.
            tmp_list (list): Holds the data for a level of the table.
            nodes (str): The nodes to looked at.
            breadcrumb (str): Used to help keep track of the hierarchy of nested elements.

        Return:
            side (dictionary): The current side part.
        """
        if ( nodes == 'Elements' ):
            t_list = tmp_list
        else:
            t_list = [None] * (len(item.findall('./SubElements/Element')) + 1)

        bc = breadcrumb
        # Get the items in the axis
        for sub_item in item.findall( './' + nodes + '/Element' ):
            if ( nodes == 'Elements' ): bc = item.attrib[ 'Name' ]

            side_dict = {}
            side_dict[ 'break' ] = ''
            side_dict[ 'rowdata' ] = []
            side_dict[ 'subbreaks' ] = []

            # Get the label for the element
            #   The default label
            self.log.logs.info( "Catgeory: " + str( sub_item.attrib[ 'Label' ] ) )
            side_dict[ 'break' ] = sub_item.attrib[ 'Label' ]

            #   Language specific label if it exists
            for lab_item in sub_item.findall( './Labels/Label' ):
                if ( lab_item.attrib[ 'Language' ] == self.language ):
                    side_dict[ 'break' ] = lab_item.attrib[ 'Text' ]

            # Check to see if the item is being displayed
            is_hidden = False
            if ( 'ShownOnTable' in sub_item.attrib ):
                if ( sub_item.attrib[ 'ShownOnTable' ] == 'false' ):
                    is_hidden = True
            
            if ( not is_hidden ):
                tmp_bc = bc
                if ( bc != '' ):
                    tmp_bc += '_'

                for key, value in header_dict.items():
                    if ( tmp_bc + sub_item.attrib[ 'Name' ] == key ):
                        head_info = value.split( ':' )
                        head_cnt = int( head_info[ 0 ] )
                
                item_cnt = 1
                row_size = len( is_hidden_dict ) * len( cell_items_dict )
                cell_cnt = 1
                self.log.logs.info( "Get the cell contents" )
                for cell_item in table.findall( './CellValues/Layer/row' ):
                    if ( item_cnt == head_cnt ):
                        val_cnt = -1

                        sub_dict = {}
                        sub_dict[ 'percent' ] = ''
                        sub_dict[ 'value' ] = ''
                        sub_dict[ 'statresult' ] = ''
                        
                        for cell in range( row_size ):
                            val_cnt += 1
                            if ( not is_hidden_dict.get( cell_cnt ) ):
                                if ( cell_items_dict.get( 'value' ) == str( val_cnt ) ):
                                    sub_dict[ 'value' ] = cell_item.attrib[ 'c' + str( cell + 1 ) ]
                                elif ( cell_items_dict.get( 'percent' ) == str( val_cnt ) ):
                                    sub_dict[ 'percent' ] = cell_item.attrib[ 'c' + str( cell + 1 ) ]
                                else:
                                    sub_dict[ 'statresult' ] = cell_item.attrib[ 'c' + str( cell + 1 ) ]

                            if ( ( cell + 1 ) % len( cell_items_dict ) == 0 ):
                                if ( not is_hidden_dict.get( cell_cnt ) ):
                                    side_dict[ 'rowdata' ].append( sub_dict )
                                
                                cell_cnt += 1
                                val_cnt = -1

                                sub_dict = {}
                                sub_dict[ 'percent' ] = ''
                                sub_dict[ 'value' ] = ''
                                sub_dict[ 'statresult' ] = ''
                                
                        break
                        
                    item_cnt += 1
                    
                self.log.logs.info( "Check for sub-categories (items within a net)" )
                sub_elems = sub_item.find( 'SubElements' )

                if ( sub_elems ):
                    if ( bc == '' ):
                        bc = sub_item.attrib[ 'Name' ]
                    else:
                        bc += '_' + sub_item.attrib[ 'Name' ]

                    self._get_side( sub_item, side, side_dict, table, is_hidden_dict, cell_items_dict, header_dict, t_list, 'SubElements', bc )

                if ( not is_hidden ):
                    t_list[ int( head_info[ 1 ] ) ] = side_dict

        self.log.logs.info( "Update the side object." )
        for d in t_list:
            if ( not ( d is None ) ):
                if ( d[ 'break' ] != '' ):
                    if ( parent_dict is None ):
                        side[ 'Side' ].append( d )
                    else:
                        parent_dict[ 'subbreaks' ].append( d )
                
        return side

    def convert_mtd_to_json( self, path_to_output_json ):
        """
        This method is the entry point for converting an MTD file to a JSON file.
        
        Usage:
            mtd.convert_mtd_to_json( )

        Outputs:
            JSON file.

        Returns:
            None
        """
        start = datetime.datetime.now()
        self.log.logs.info( "Converting " + self.mtd + " into a JSON file." )

        # Read in the mtd file as xml and create the tree object
        tree = ET.parse( self.mtd )

        # Get the root element
        root = tree.getroot()

        # Find any global filters
        global_filters = ''
        for item in root.findall( './Global/Filters/Filter' ):
            global_filters += self._get_filter( item )

        # Trim the global filter if there is one.
        if ( len( global_filters ) > 1 ):
            global_filters = global_filters[ 0:-5 ]

        # Store the global filter in the JSON dictionary and setup the 'tables' node.
        self.mtd_json_dict[ 'globalfilter' ] = global_filters
        self.mtd_json_dict[ 'tables' ] = []

        # Traverse through the tables
        c = 0

        for item in root.findall( './Tables/Table' ):
            c += 1
    
            # Initialize a few dictionaries.
            # For the current table.
            tmp_table_dict = {}
            # Holds the cell items that are in the tables.
            cell_items_dict = {}
            # Holds whether a banner column is displayed or hidden.
            is_hidden_dict = {}
            header_dict = {}
            
            self.log.logs.info( "Converting table " + str( item.attrib[ 'Name' ] ) )
            # Set the table name and description.
            tmp_table_dict[ 'tablename' ] = item.attrib[ 'Name' ]
            tmp_table_dict[ 'comments' ] = item.attrib[ 'Description' ]
            
            # Need to find the order of the cell contents so that results are placed in the correct locations.
            self.log.logs.info( "Finding the cell items..." )
            for cell_item in item.findall( './CellItems/CellItem' ):
                if ( cell_item.attrib[ 'Type' ] == 'Count' ):
                    cell_items_dict[ 'value' ] = cell_item.attrib[ 'Index' ]
                elif ( cell_item.attrib[ 'Type' ] == 'ColPercent' ):
                    cell_items_dict[ 'percent' ] = cell_item.attrib[ 'Index' ]
                elif ( cell_item.attrib[ 'Type' ] == 'ColPropResults' ):
                    cell_items_dict[ 'statresult' ] = cell_item.attrib[ 'Index' ]
                else:
                    cell_items_dict[ 'value' ] = cell_item.attrib[ 'Index' ]
                    
            # Find the statistics that are being used
            stats = ''
            self.log.logs.info( "Finding the statistics..." )
            for stat_item in item.findall( './Statistics/Statistic' ):
                if ( 'Annotation' in stat_item.attrib ):
                    stats += stat_item.attrib[ 'Name' ] + ' - ' + stat_item.attrib[ 'Annotation' ] + '\r\n'
                else:
                    stats += stat_item.attrib[ 'Name' ] + '\r\n'
                
            # Trim the stats description if it exists.
            if ( len( stats ) > 1 ):
                stats = stats[ 0:-4 ]
            
            # Set the stats description into the table dictionary.
            tmp_table_dict[ 'stattestdescription' ] = stats

            # Find table level filters
            table_filters = ''
            for filt_item in item.findall( './Filters/Filter' ):
                table_filters += self._get_filter( filt_item )

            # Trim table level filters if it exists.
            if ( len( table_filters ) > 1 ):
                table_filters = table_filters[ 0:-5 ]

            # Set the table filter description into the table dictionary.
            tmp_table_dict[ 'tablefilter' ] = table_filters

            # Find table level weights
            tmp_table_dict[ 'tableweight' ] = ''
            
            banner_dict = {}
            banner_dict[ 'Banner' ] = []
            banner_dict[ 'Side' ] = []
            cnt = 1
            
            for ban_item in item.findall( './Axes/Axis' ):
                # Find the banner - Name = Top
                if ( ban_item.attrib[ 'Name' ] == 'Top' ):
                    self.log.logs.info( "Analyze the banner" )
                    # Get the banner
                    self._get_banner( ban_item, banner_dict, is_hidden_dict, cnt, c )
            
            for ban_item in item.findall( './Axes/Axis' ):
                # Find the side - Name = Side
                if ( ban_item.attrib[ 'Name' ] == 'Side' ):
                    self.log.logs.info( "Analyze the side" )
                    # Find the headers in order to get the response order, table may be sorted.
                    ctr = 0
                    for head in ban_item.findall( './SubAxes/Axis' ):
                        ctr = self.get_side_headers( head, header_dict, 'ElementHeadings', head.attrib[ 'Name' ], ctr )

                    # Get the side
                    for axis in ban_item.findall('./SubAxes/Axis'):
                        tmp_list = [None] * (len(header_dict) + 1)
                        self.log.logs.info( "Analyzing side axis " + str(axis.attrib['Label']) )
                        self._get_side( axis, banner_dict, None, item, is_hidden_dict, cell_items_dict, header_dict, tmp_list, 'Elements', axis.attrib[ 'Name' ] )

            tmp_table_dict[ 'data' ] = banner_dict
            self.mtd_json_dict[ 'tables' ].append( tmp_table_dict )
            
        self.log.logs.info( "Exporting the JSON file." )
        with open(path_to_output_json, 'w') as j_file:
            json.dump(self.mtd_json_dict, j_file)

        end = datetime.datetime.now()
        elapsed = end - start
        self.log.logs.info( "Elapsed time for conversion: " + str( elapsed ) )
