import logging, datetime
import itertools
from collections import OrderedDict
from ipsos.models.metadata_model.Class import Class
from ipsos.models.metadata_model.Field import Field
from ipsos.models.metadata_model.Variable import Variable
from ipsos.models.metadata_model.Element import Element
from ipsos.models.metadata_model.Elements import Elements
from ipsos.models.metadata_model.ElementsInstance import ElementsInstance
from ipsos.models.metadata_model.Categories import Categories
from ipsos.models.metadata_model.Types import Types


class Fields:
    def __init__( self, mdm_dict, types, category_map, language, context ):
        self._items = OrderedDict()
        self._document = mdm_dict
        self._types = types
        self._category_map = category_map
        self._base_language = language.upper()
        self._base_context = context.upper()
        self._parse()

    def __getitem__( self, key ):
        if ( key.find( '.' ) > 0 ):
            fullname = key.replace( '[..]', '' )
            q_parts = str(fullname).split('.')[ 1: ]

            try:
                field = self._items[ str(fullname).split('.')[ 0 ] ]
                for item in q_parts:
                    if ( type( field ) == Variable ):
                        field = field.HelperFields[ item ]
                    else:
                        field = field._items[ item ]
                return field
            except:
                return None
        else:
            try:
                return self._items[ key ]
            except:
                return None

    def __iter__( self ):
        return iter( [ self._items[ item ] for item in self._items ] )

    def _parse( self ):
        """ Fields is a combination of variables and fields where the variables node
        contains the simple questions and the fields node contains the grids/arrays.

        Store the fields information in a dict along with the simple-question reference id (@ref).
        Go through the variables node adding each to the field class but first check the
        variable id (@id) against the field dict to see if it belongs to a loop, if it does,
        add the loop to the field class and then add the variable to that field object.
        """
        questions_list = [ ]
        field_dict = { }
        class_dict = { }
        system_dict = { }

        # Get the ref for each variable in design in order to remove variables that
        #  are only in the definition node
        if ( type( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'variable' ] ) is list ):
            for variable in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'variable' ]:
                questions_list.append( variable[ '@ref' ] )
        else:
            questions_list.append( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'variable' ][ '@ref' ] )

        # Create the dictionary of variable/grid pairs
        # System variables
        try:
            for field in self._document[ 'xml' ][ 'mdm:metadata' ][ 'system' ][ 'class' ]:
                name = field[ '@name' ]
                self._get_field_variable_pairs( field[ 'fields' ], system_dict, name )
        except:
            pass
        
        # Grid/loop variables
        if ( "loop" in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ] ):
            if ( type( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'loop' ] ) is list ):
                for field in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'loop' ]:
                    name = field[ '@name' ]
                    self._get_field_variable_pairs( field[ 'class' ][ 'fields' ], field_dict, name )
            else:
                name = self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'loop' ][ '@name' ]
                self._get_field_variable_pairs( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'loop' ][ 'class' ][ 'fields' ], field_dict, name )
        if ( "grid" in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ] ):
            if ( type( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'grid' ] ) is list ):
                for field in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'grid' ]:
                    name = field[ '@name' ]
                    self._get_field_variable_pairs( field[ 'class' ][ 'fields' ], field_dict, name )
            else:
                name = self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'grid' ][ '@name' ]
                self._get_field_variable_pairs( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'grid' ][ 'class' ][ 'fields' ], field_dict, name )

        # Class/block variables
        if ( 'class' in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ] ):
            if ( type( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'class' ] ) is list ):
                for field in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'class' ]:
                    name = field[ '@name' ]
                    self._get_field_variable_pairs( field[ 'fields' ], class_dict, name )
            else:
                name = self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'class' ][ '@name' ]
                self._get_field_variable_pairs( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'class' ][ 'fields' ], class_dict, name )

        # Go through the variables node
        #   Find the system variables first
        helper_list = [ ]
        done_list = [ ]
        for variable in self._document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'variable' ]:
            uuid = variable[ '@id' ]
            if ( system_dict.get( uuid ) is not None ):
                # This question is part of a block/class question
                for field in self._document[ 'xml' ][ 'mdm:metadata' ][ 'system' ][ 'class' ]:
                    if ( field[ '@name' ] == system_dict.get( uuid ) ):
                        if ( variable[ '@id' ] in done_list ):
                            break
                        else:
                            self._get_class( field, None, helper_list, done_list, is_system = True )

        for variable in self._document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'variable' ]:
            uuid = variable[ '@id' ]
            if ( system_dict.get( uuid ) is None and not ( uuid in helper_list ) ):
                # Already took care of system variables, skip them here
                if ( field_dict.get( uuid ) is None and class_dict.get( uuid ) is None ):
                    # The variable does not belong to a complex question
                    if ( uuid in questions_list ):
                        self._get_variable( variable, None, helper_list )
                elif ( field_dict.get( uuid ) is None ):
                    # This question is part of a class/block
                    if ( type( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'class' ] ) is list ):
                        for field in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'class' ]:
                            if ( field[ '@name' ] == class_dict.get( uuid ) ):
                                self._get_class( field, None, helper_list, done_list )
                                break
                    else:
                        if ( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'class' ][ '@name' ] == class_dict.get( uuid ) ):
                            self._get_class( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'class' ], None, helper_list, done_list )
                else:
                    # This question is part of a grid/loop
                    if ( "loop" in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ] ):
                        if ( type( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'loop' ] ) is list ):
                            for field in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'loop' ]:
                                if ( field[ '@name' ] == field_dict.get( uuid ) ):
                                    self._get_field( field, None, helper_list, done_list )
                                    break
                        else:
                            if ( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'loop' ][ '@name' ] == field_dict.get( uuid ) ):
                                self._get_field( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'loop' ], None, helper_list, done_list )

                    if ( "grid" in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ] ):
                        if ( type( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'grid' ] ) is list ):
                            for field in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'grid' ]:
                                if ( field[ '@name' ] == field_dict.get( uuid ) ):
                                    self._get_field( field, None, helper_list, done_list, grid_node=True )
                                    break
                        else:
                            if ( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'grid' ][ '@name' ] == field_dict.get( uuid ) ):
                                self._get_field( self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'fields' ][ 'grid' ], None, helper_list, done_list, grid_node=True )

        del ( self._document )
        del ( self._base_language )
        del ( self._base_context )

    def _get_field_variable_pairs( self, o, field_dict, name ):
        """ Drill down to find the simple question and then store that id as the dictionary key,
        the value will be the top level grid name.
        
        As we go through the variables node we will check the id of each variable against this 
        dictionary to see if it belongs to a grid, if it does then we go through the fields
        node to build the complex question.
        """
        for field_key in o.keys():
            if ( field_key == 'variable' ):
                if ( type( o[ 'variable' ] ) == list ):
                    for var in o[ 'variable' ]:
                        for key in var.keys():
                            if ( key == '@ref' ):
                                field_dict[ var[ key ] ] = name
                                break
                else:
                    for key in o[ 'variable' ].keys():
                        if ( key == '@ref' ):
                            field_dict[ o[ 'variable' ][ key ] ] = name
                            break
            elif ( field_key == 'loop' ):
                if ( type( o[ 'loop' ] ) == list ):
                    for loop in o[ 'loop' ]:
                        self._get_field_variable_pairs( loop[ 'class' ][ 'fields' ], field_dict, name )
                else:
                    self._get_field_variable_pairs( o[ 'loop' ][ 'class' ][ 'fields' ], field_dict, name )
            elif ( field_key == 'grid' ):
                if ( type( o[ 'grid' ] ) == list ):
                    for loop in o[ 'grid' ]:
                        self._get_field_variable_pairs( loop[ 'class' ][ 'fields' ], field_dict, name )
                else:
                    self._get_field_variable_pairs( o[ 'grid' ][ 'class' ][ 'fields' ], field_dict, name )

    def _get_field( self, field, f, helper_list, done_list, full_name = None, is_system = False, grid_node=False ):
        isgrid = '0'
        other_vars_list = ''
        name = field[ '@name' ]
        uuid = field[ '@id' ]
        objecttypevalue = int( field[ '@type' ] )
        if ( "@isgrid" in field ): isgrid = field[ '@isgrid' ]
        if ( grid_node ): 
            isgrid = '1'
            objecttypevalue = 2
        iteratortype = field[ '@iteratortype' ]
        if ( iteratortype == '2' and objecttypevalue == 0 ): objecttypevalue = 1
        f_new = Field( name, uuid, objecttypevalue, isgrid, iteratortype, self._base_language, self._base_context )

        if ( full_name is None ):
            f_new.FullName = name
        else:
            f_new.FullName = full_name + '[..].' + name
        f_new.IsSystem = is_system
        if ( "categories" in field ):
            e = ElementsInstance( self._types, self._category_map, self._base_language, self._base_context )
            other_vars_list, _ = e._parse( field[ 'categories' ], f_new, self._document, None )
        if ( iteratortype == '3' ):
            # Numeric iterator - no categories
            if ( "ranges" in field ):
                lowerbound = int( field[ 'ranges' ][ 'range' ].get( '@lowerbound' ) )
                upperbound = int( field[ 'ranges' ][ 'range' ].get( '@upperbound' ) )
                if ( lowerbound > 0 or upperbound > 0 ):
                    if ( upperbound == 2147483647 ):
                        f_new.Type = 2
                        f_new.ObjectTypeValue = 1
                    else:
                        for i in range( lowerbound, upperbound + 1 ):
                            e = Element( str( i ), str( i ), i, self._base_language, self._base_context )
                            e.Labels.Text( self._base_language, self._base_context, str( i ) )
                            e.Label = e.Labels.Label
                            f_new.Elements[ e.Name ] = e
                            f_new.Categories[ e.Name ] = e
                else: f_new.ObjectTypeValue = 1

        if ( "labels" in field ): 
            f_new.Labels._set_from_dict( field[ 'labels' ] )
            f_new.Label = f_new.Labels.Label
        if ( "properties" in field ):
            f_new.Properties._from_dict( field[ "properties" ] )
            for prop in f_new.Properties:
                if ( prop.Name == 'DisplayOrientation' ):
                    f_new.Orientation = prop.Value
                    break

        if ( "helperfields" in field ):
            if ( type( field[ 'helperfields' ] ) == list ):
                for h in field[ 'helperfields' ]:
                    self._get_helperfield( h[ 'helperfields' ][ 'variable' ][ '@ref' ], f_new, helper_list, 1, is_system )
            else:
                for h in field[ 'helperfields' ]:
                    if ( h == 'variable' ):
                        if ( "@ref" in field[ 'helperfields' ][ 'variable' ] ):
                            self._get_helperfield( field[ 'helperfields' ][ 'variable' ][ '@ref' ], f_new, helper_list, 1, is_system )
                        else:
                            for h2 in field[ 'helperfields' ][ 'variable' ]:
                                self._get_helperfield( h2[ '@ref' ], f_new, helper_list, 1, is_system )
                        break

        if ( len( other_vars_list ) > 0 ):
            other_vars = other_vars_list.split( ',' )[1:]
            for ref in other_vars:
                self._get_helperfield( ref, f_new, helper_list, 2, is_system )

        # New code for order
        # Need a fully sorted dict to get the correct order of questions
        new_fields_dict = { }
        for item in field[ 'class' ][ 'fields' ].keys():
            if ( item == 'variable' ):
                if ( type( field[ 'class' ][ 'fields' ][ 'variable' ] ) == list ):
                    for var in field[ 'class' ][ 'fields' ][ 'variable' ]:
                        new_fields_dict[ var[ '@__order__' ] ] = var
                else:
                    new_fields_dict[ field[ 'class' ][ 'fields' ][ 'variable' ][ '@__order__' ] ] = field[ 'class' ][ 'fields' ][ 'variable' ]
            elif ( item == 'loop' ):
                if ( type( field[ 'class' ][ 'fields' ][ 'loop' ] ) == list ):
                    for loop in field[ 'class' ][ 'fields' ][ 'loop' ]:
                        new_fields_dict[ loop[ '@__order__' ] ] = loop
                else:
                    new_fields_dict[ field[ 'class' ][ 'fields' ][ 'loop' ][ '@__order__' ] ] = field[ 'class' ][ 'fields' ][ 'loop' ]
            elif ( item == 'grid' ):
                if ( type( field[ 'class' ][ 'fields' ][ 'grid' ] ) == list ):
                    for loop in field[ 'class' ][ 'fields' ][ 'grid' ]:
                        new_fields_dict[ loop[ '@__order__' ] ] = loop
                else:
                    new_fields_dict[ field[ 'class' ][ 'fields' ][ 'grid' ][ '@__order__' ] ] = field[ 'class' ][ 'fields' ][ 'loop' ]

        for _, value in sorted( new_fields_dict.items() ):
            if ( len( value ) == 4 ):
                self._find_field_variable( value, f_new, helper_list )
                done_list.append( value[ '@ref' ] )
            else:
                self._get_field( value, f_new, helper_list, done_list, full_name = f_new.FullName )
                done_list.append( value[ '@id' ][1:] )
        
        if ( f is None ):
            self._items[ name ] = f_new
        else:
            f._items[ name ] = f_new

    def _get_class( self, field, f, helper_list, done_list, is_system = False ):
        f_new = None
        name = field[ '@name' ]
        uuid = field[ '@id' ]
        f_new = Class( name, uuid, self._base_language, self._base_context )

        f_new.IsSystem = is_system
        if ( "labels" in field ):
            f_new.Labels._set_from_dict( field[ 'labels' ] )
            f_new.Label = f_new.Labels.Label
        if ( "properties" in field ): f_new.Properties._from_dict( field[ "properties" ] )

        if ( "helperfields" in field ):
            if ( type( field[ 'helperfields' ] ) == list ):
                for h in field[ 'helperfields' ]:
                    self._get_helperfield( h[ 'helperfields' ][ 'variable' ][ '@ref' ], f_new, helper_list, 1, is_system )
            else:
                for h in field[ 'helperfields' ]:
                    if ( h == 'variable' ):
                        if ( "@ref" in field[ 'helperfields' ][ 'variable' ] ):
                            self._get_helperfield( field[ 'helperfields' ][ 'variable' ][ '@ref' ], f_new, helper_list, 1, is_system )
                        else:
                            for h2 in field[ 'helperfields' ][ 'variable' ]:
                                self._get_helperfield( h2[ '@ref' ], f_new, helper_list, 1, is_system )
                        break

        for item in field[ 'fields' ].keys():
            if ( item == 'variable' ):
                if ( type( field[ 'fields' ][ 'variable' ] ) == list ):
                    for var in field[ 'fields' ][ 'variable' ]:
                        self._find_field_variable( var, f_new, helper_list, is_system = is_system )
                        done_list.append( var[ '@ref' ] )
                else:
                    self._find_field_variable( field[ 'fields' ][ 'variable' ], f_new, helper_list, is_system = is_system )
        
        if ( f is None ):
            self._items[ name ] = f_new
        else:
            f._items[ name ] = f_new

    def _find_field_variable( self, var, f, helper_list, is_system = False ):
        ref = var[ '@ref' ]
        for variable in self._document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'variable' ]:
            if ( variable[ '@id' ] == ref ):
                self._get_variable( variable, f, helper_list, is_system )
                break

    def _get_variable( self, variable, f, helper_list, is_system = False ):
        other_vars_list = ''

        var, other_vars_list = self._add_variable( variable, other_vars_list, is_system )

        if ( "helperfields" in variable ):
            if ( type( variable[ 'helperfields' ] ) == list ):
                for h in variable[ 'helperfields' ]:
                    self._get_helperfield( h[ 'helperfields' ][ 'variable' ][ '@ref' ], var, helper_list, 1, is_system )
            else:
                for h in variable[ 'helperfields' ]:
                    if ( h == 'variable' ):
                        if ( "@ref" in variable[ 'helperfields' ][ 'variable' ] ):
                            self._get_helperfield( variable[ 'helperfields' ][ 'variable' ][ '@ref' ], var, helper_list, 1, is_system )
                        else:
                            for h2 in variable[ 'helperfields' ][ 'variable' ]:
                                self._get_helperfield( h2[ '@ref' ], var, helper_list, 1, is_system )
                        break
        if ( len( other_vars_list ) > 0 ):
            other_vars = other_vars_list.split( ',' )[1:]
            for ref in other_vars:
                self._get_helperfield( ref, var, helper_list, 2, is_system )

        if ( f is None ):
            self._items[ var.Name ] = var
        else:
            f._items[ var.Name ] = var

    def _get_helperfield( self, ref, var, helper_list, helper_type, is_system = False ):
        helper_list.append( ref )
        help_var = None

        if ( helper_type == 1 ):
            help_var = next( x for x in self._document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'variable' ] if x[ '@id' ] == ref )
        else:
            if ( type( self._document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'othervariable' ] ) is list ):
                help_var = next( x for x in self._document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'othervariable' ] if x[ '@id' ] == ref )
            else:
                if ( self._document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'othervariable' ][ '@id'] == ref ):
                    help_var = self._document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'othervariable' ]

        hvar, _ = self._add_variable( help_var, '', is_system )

        if ( helper_type == 1 ):
            var.HelperFields[ hvar.Name ] = hvar
        else:
            var.OtherCategories[ hvar.Name ] = hvar

    def _add_variable( self, variable, other_vars_list, is_system = False ):
        name = variable[ '@name' ]
        # if ( name == 'LASTCATPUR_SUMMARY__10000013_LASTCATPUR_scale_12' ):
        #     i = 1
        datatype = int( variable[ '@type' ] )
        minvalue = '1'
        maxvalue = '9999'
        if ( "@min" in variable ): minvalue = variable[ '@min' ]
        if ( "@max" in variable ): maxvalue = variable[ '@max' ]
        usagetype = 0
        if ( "@usagetype" in variable ): usagetype = int( variable[ '@usagetype' ] )
        objecttypevalue = 0
        
        var = Variable( name, variable[ '@id' ], datatype, minvalue, maxvalue, objecttypevalue, usagetype, self._base_language, self._base_context )

        var.IsSystem = is_system
        if ( "@defaultanswer" in variable ): var.DefaultAnswer = variable[ '@defaultanswer' ]
        if ( "@expression" in variable ): var.Expression = variable[ '@expression' ]
        if ( "@no-casedata" in variable):
            if ( variable[ '@no-casedata'] == '-1' ): var.HasCaseData = False
        if ( "categories" in variable ):
            e = ElementsInstance( self._types, self._category_map, self._base_language, self._base_context )
            other_vars_list, exclusive_count = e._parse( variable[ 'categories' ], var, self._document, None )
            if ( maxvalue == '9999' ):
                var.MaxValue = str( len( var.Categories ) - exclusive_count )
        if ( "labels" in variable ): 
            var.Labels._set_from_dict( variable[ 'labels' ] )
            var.Label = var.Labels.Label
        if ( "properties" in variable ): var.Properties._from_dict( variable[ "properties" ] )
        if ( "axis" in variable ): var.AxisExpression =  variable[ "axis" ][ '@expression' ]

        return var, other_vars_list
