import uuid, ntpath, sys, datetime, os
from xml.dom import minidom
from ipsos.models.Document import Document
from ipsos.models.metadata_model.Elements import Elements
from ipsos.models.metadata_model.Class import Class
from ipsos.models.metadata_model.Variable import Variable
from csv import reader


class MDD:
    def __init__ ( self, mdd, path_to_mdd, verbose = False ):
        self._document = mdd
        self.mdd = path_to_mdd
        self.verbose = verbose

    def _add_fields( self, root_node, definition_node, fields_node, types_dict ):
        # Done
        for field in self._document.Fields:
            if ( field.ObjectTypeValue == 0 ):
                # Simple question
                self._add_variable( root_node, definition_node, fields_node, field, types_dict )
            else:
                if ( type( field ) == Class ):
                    # Class/Blocks
                    if ( not field.IsSystem ):
                        self._add_class( root_node, definition_node, fields_node, field, types_dict )
                else:
                    # Grid/Loop question
                    self._add_field( root_node, definition_node, fields_node, field, types_dict )

    def _add_class( self, root_node, definition_node, fields_node, field, types_dict ):
        # Done
        guid = str( uuid.uuid4() )

        # Fields node - sub-questions to both fields_node and defintion_node
        class_node = root_node.createElement( 'class' )
        class_node.setAttribute( 'name', field.Name )
        class_node.setAttribute( 'id', '_' + guid )
        class_node.setAttribute( 'global-name-space', '0' )
        fields_node.appendChild( class_node )

        self._add_object_properties( root_node, field, class_node )
        self._add_object_labels( root_node, field, class_node )

        f_node = root_node.createElement( 'fields' )
        f_node.setAttribute( 'name', '@fields' )
        f_node.setAttribute( 'global-name-space', '-1' )
        class_node.appendChild( f_node )

        # Add the variables to the class
        for v in field._items:
            var = field._items[ v ]
            self._add_variable( root_node, definition_node, f_node, var, types_dict )

    def _add_field( self, root_node, definition_node, fields_node, field, types_dict ):
        # Done
        guid = str( uuid.uuid4() )

        # Fields node - sub-questions in fields_node - may or may not go into definition_node depending on type
        loop_node = root_node.createElement( 'loop' )
        loop_node.setAttribute( 'name', field.Name )
        loop_node.setAttribute( 'id', '_' + guid )
        loop_node.setAttribute( 'type', str( field.Type ) )
        loop_node.setAttribute( 'iteratortype', str( field.IteratorType ) )

        if ( field.IsGrid == '1' ):
            loop_node.setAttribute( 'isgrid', '1' )

        fields_node.appendChild( loop_node )

        self._add_object_properties( root_node, field, loop_node )
        self._add_object_labels( root_node, field, loop_node )

        if ( field.IteratorType == 2 ):
            other_dict = {}
            categories_node = root_node.createElement( 'categories' )
            categories_node.setAttribute( 'global-name-space', '-1' )
            loop_node.appendChild( categories_node )
            self._add_object_categories( root_node, field.Elements, categories_node, types_dict, other_dict )
        
        if ( field.IteratorType == 3 ):
            # Numeric iterator - find the range
            lowerbound = 99999
            upperbound = 1
            for e in field.Elements:
                if ( len( e ) > 0 ):
                    elem = field.Elements[ e ]
                    if ( int( elem.Name ) < lowerbound ): lowerbound = int( elem.Name )
                    if ( int( elem.Name ) > upperbound ): upperbound = int( elem.Name )

            ranges_node = root_node.createElement( 'ranges' )
            loop_node.appendChild( ranges_node )

            range_node = root_node.createElement( 'range' )
            range_node.setAttribute( 'upperbound', str( upperbound ) )
            range_node.setAttribute( 'lowerbound', str( lowerbound ) )
            ranges_node.appendChild( range_node )

        class_node = root_node.createElement( 'class' )
        class_node.setAttribute( 'name', '@class' )
        class_node.setAttribute( 'global-name-space', '-1' )
        loop_node.appendChild( class_node )

        f_node = root_node.createElement( 'fields' )
        f_node.setAttribute( 'name', '@fields' )
        f_node.setAttribute( 'global-name-space', '-1' )
        class_node.appendChild( f_node )

        # Add the variables to the class
        for v in field._items:
            var = field._items[ v ]

            if ( var.ObjectTypeValue == 0 ):
                # Simple question
                self._add_variable( root_node, definition_node, f_node, var, types_dict )
            else:
                # Grid/Loop question
                self._add_field( root_node, definition_node, f_node, var, types_dict )

    def _add_variable( self, root_node, definition_node, fields_node, field, types_dict, ref = None, type_of_node = None ):
        guid = str( uuid.uuid4() )
        if ( ref ): guid = ref

        if ( not type_of_node ):
            # Fields node - reference only to defintion node
            #   DO not add to fields node if this is an othervariable
            variable_node = root_node.createElement( 'variable' )
            variable_node.setAttribute( 'name', field.Name )
            variable_node.setAttribute( 'id', '_' + guid )
            variable_node.setAttribute( 'ref', guid )
            fields_node.appendChild( variable_node )

        # Definition node
        def_variable_node = None
        if ( type_of_node ):
            def_variable_node = root_node.createElement( type_of_node )
        else:
            def_variable_node = root_node.createElement( 'variable' )
        def_variable_node.setAttribute( 'id', guid )
        def_variable_node.setAttribute( 'name', field.Name )
        def_variable_node.setAttribute( 'type', str( field.DataType ) )
        if ( not type_of_node ): def_variable_node.setAttribute( 'min', str( field.MinValue ) )

        if ( not type_of_node ):
            if ( field.DataType == 3 ):
                def_variable_node.setAttribute( 'mintype', '3' )

                if ( field.MaxValue == '1' ):
                    def_variable_node.setAttribute( 'max', str( field.MaxValue ) )
                    def_variable_node.setAttribute( 'maxtype', '3' )
            else:
                def_variable_node.setAttribute( 'mintype', '3' )

                def_variable_node.setAttribute( 'max', str( field.MaxValue ) )
                def_variable_node.setAttribute( 'maxtype', '3' )

                if ( field.DataType == 1 ):
                    def_variable_node.setAttribute( 'rangeexp', '[' + str( field.MinValue ) + ' .. ' + str( field.MaxValue ) + ']' )

        if ( field.UsageType != 0 ):
            def_variable_node.setAttribute( 'usagetype', str( field.UsageType ) )

        definition_node.appendChild( def_variable_node )

        if ( field.DataType == 3 ):
            # Add categories
            other_dict = {}
            categories_node = root_node.createElement( 'categories' )
            categories_node.setAttribute( 'global-name-space', '-1' )
            def_variable_node.appendChild( categories_node )
            self._add_object_categories( root_node, field.Elements, categories_node, types_dict, other_dict )

            # Add othervariables if they exist
            for o in field.OtherCategories:
                o_field = field.OtherCategories[ o ]
                o_guid = other_dict[ o ]
                self._add_variable( root_node, definition_node, fields_node, o_field, types_dict, o_guid, 'othervariable' )

        # Add labels
        self._add_object_labels( root_node, field, def_variable_node )

        # Add properties
        self._add_object_properties( root_node, field, def_variable_node )

        # Add axis expression if it exists
        if ( field.Expression ):
            axis_node = root_node.createElement( 'axis' )
            axis_node.setAttribute( 'expression', str( field.Expression ) )
            def_variable_node.appendChild( axis_node )

        # Add helperfields if they exist
        if ( len( field.HelperFields ) > 0 ):
            helper_node = root_node.createElement( 'helperfields' )
            helper_node.setAttribute( 'id', str( uuid.uuid4() ) )
            helper_node.setAttribute( 'name', '@helperfields' )
            helper_node.setAttribute( 'global-name-space', '-1' )
            def_variable_node.appendChild( helper_node )

            for h in field.HelperFields:
                h_guid = str( uuid.uuid4() )
                h_field = field.HelperFields[ h ]

                var_node = root_node.createElement( 'variable' )
                var_node.setAttribute( 'id', '_' + h_guid )
                var_node.setAttribute( 'name', h_field.Name )
                var_node.setAttribute( 'ref', h_guid )
                helper_node.appendChild( var_node )

                self._add_variable( root_node, definition_node, fields_node, h_field, types_dict, h_guid, 'variable' )

    def _add_system_variables( self, root_node, system_node, file ):

        with open( './ipsos/models/metadata_model/' + file, mode='r', encoding='utf-8' ) as f:
            lines = f.readlines( )

            for line in lines:
                if ( len( line ) > 0 ):
                    node = minidom.parseString( line ).documentElement

                    system_node.appendChild( node )

    def _add_system_routing( self, root_node, systemrouting_node ):
        # Done
        routing_node = root_node.createElement( 'routing' )
        routing_node.setAttribute( 'context', '__METADATASERVICES_CHANGETRACKER' )
        routing_node.setAttribute( 'interviewmodes', '0' )
        routing_node.setAttribute( 'usekeycodes', '0' )
        systemrouting_node.appendChild( routing_node )

        routing_node = root_node.createElement( 'routing' )
        routing_node.setAttribute( 'context', 'PAPER' )
        routing_node.setAttribute( 'interviewmodes', '0' )
        routing_node.setAttribute( 'usekeycodes', '0' )
        systemrouting_node.appendChild( routing_node )
        
        ritem_node = root_node.createElement( 'ritem' )
        ritem_node.setAttribute( 'name', 'Respondent' )
        ritem_node.setAttribute( 'item', 'Respondent' )
        routing_node.appendChild( ritem_node )
        
    def _add_datasources( self, root_node, datasources_node, mdm_node ):
        # Done
        original_filename = ntpath.basename( self.mdd )
        original_filename_without_extension = os.path.splitext( original_filename )[0]

        datasources_node.setAttribute( 'default', 'mrDataFileDsc' )
        mdm_node.appendChild( datasources_node )

        has_ddf = False
        for key in self._document.DataSources._items:
            ddf = self._document.DataSources[ key ]

            connection_node = root_node.createElement( 'connection' )
            connection_node.setAttribute( 'name', ddf.Name )

            if ( ddf.CDSCName.lower() == 'mrdatafiledsc' ):
                has_ddf = True
                connection_node.setAttribute( 'dblocation', original_filename_without_extension + '.ddf' )
            else:
                connection_node.setAttribute( 'dblocation', ddf.DBLocation )
            connection_node.setAttribute( 'cdscname', ddf.CDSCName )
            connection_node.setAttribute( 'project', ddf.Project )
            connection_node.setAttribute( 'id', str( uuid.uuid4( ) ) )

            datasources_node.appendChild( connection_node )
        
        if ( not has_ddf ):
            # There was no ddf associated with this mdd, create the connection for the ddf that will be generated
            tmp_name = ntpath.basename( self.mdd )
            ddf_name = os.path.splitext( tmp_name )[0]

            connection_node = root_node.createElement( 'connection' )
            connection_node.setAttribute( 'name', 'mrDataFileDsc' )
            connection_node.setAttribute( 'dblocation', ddf_name + '.ddf' )
            connection_node.setAttribute( 'cdscname', 'mrDataFileDsc' )
            connection_node.setAttribute( 'project', '' )
            connection_node.setAttribute( 'id', str( uuid.uuid4( ) ) )

            datasources_node.appendChild( connection_node )

    def _add_shared_list( self, root_node, definition_node, types_node, types_dict ):
        # Done
        for key in self._document.Types._items:
            typ = self._document.Types[ key ]

            # Add to the types node
            guid = str( uuid.uuid4() )
            types_dict[ key ] = guid
            categories_node = root_node.createElement( 'categories' )
            categories_node.setAttribute( 'id', '_' + guid )
            categories_node.setAttribute( 'name', key )
            categories_node.setAttribute( 'ref', guid )
            types_node.appendChild( categories_node )
        
            # Add to the deffinition node
            cats_node = root_node.createElement( 'categories' )
            cats_node.setAttribute( 'id', guid )
            cats_node.setAttribute( 'name', key )
            cats_node.setAttribute( 'global_name-space', '0' )
            definition_node.appendChild( cats_node )

            # Check for categories
            if ( len( typ._items ) > 0 ):
                other_dict = {}
                self._add_object_categories( root_node, typ._items, cats_node, types_dict, other_dict )

    def _add_object_categories( self, root_node, obj, node, types_dict, other_dict ):
        # Done
        for c in obj:
            cat = obj[ c ]

            category_node = None
            reference = False
            if ( type( cat ) == Elements ):
                if ( cat.IsReference ):
                    # This element is a shared list
                    ref = types_dict.get( c )
                    category_node = root_node.createElement( 'categories' )
                    category_node.setAttribute( 'id', '_' + str( uuid.uuid4() ) )
                    category_node.setAttribute( 'name', c )
                    category_node.setAttribute( 'categoriesref', ref )
                    category_node.setAttribute( 'global-name-space', '-1' )
                    category_node.setAttribute( 'inline', '-1' )
                    category_node.setAttribute( 'ref_name', c )
                    node.appendChild( category_node )
                    reference = True
                else:
                    category_node = root_node.createElement( 'categories' )
                    category_node.setAttribute( 'name', c )
                    category_node.setAttribute( 'global-name-space', '-1' )
                    node.appendChild( category_node )
            else:
                category_node = root_node.createElement( 'category' )

                category_node.setAttribute( 'id', '_' + str( uuid.uuid4() ) )
                category_node.setAttribute( 'name', c )
                if ( cat.Fixed ): category_node.setAttribute( 'fixed', str(cat.Fixed ) )
                if ( cat.Exclusive ): category_node.setAttribute( 'exclusive', str( cat.Exclusive ) )
                node.appendChild( category_node )

                # if ( len( cat.Other ) ):
                #     category_node.setAttribute( 'other-local', '-1' )

                #     o_guid = str( uuid.uuid4() )
                #     other_node = root_node.createElement( 'othervariable' )
                #     other_node.setAttribute( 'id', '_' + o_guid )
                #     other_node.setAttribute( 'name', c )
                #     other_node.setAttribute( 'ref', o_guid )
                #     category_node.appendChild( other_node )
                #     other_dict[ cat.Other ] = o_guid

            if ( not reference ):
                # Check for/add labels
                self._add_object_labels( root_node, cat, category_node )
                
                # Check for properties
                if ( len( cat.Properties._items ) > 0 ):
                    self._add_object_properties( root_node, cat, category_node )

                if ( type( cat ) == Elements ):
                    self._add_object_categories( root_node, cat._items, category_node, types_dict, other_dict )

    def _add_object_properties( self, root_node, obj, node ):
        # Done
        properties_node = root_node.createElement( 'properties' )
        node.appendChild( properties_node )

        for prop in obj.Properties._items:
            property_node = root_node.createElement( 'property' )
            property_node.setAttribute( 'name', prop.Name )
            property_node.setAttribute( 'value', str( prop.Value ) )
            property_node.setAttribute( 'type', str( prop.Type ) )
            property_node.setAttribute( 'context', prop.Context )
            properties_node.appendChild( property_node )

    def _add_object_labels( self, root_node, obj, node ):
        # Done
        labels_node = root_node.createElement( 'labels' )
        labels_node.setAttribute( 'context', 'LABEL' )
        node.appendChild( labels_node )

        if ( len( obj.Labels._items ) > 0 ):
            for l in obj.Labels._items:
                for lab in obj.Labels._items[ l ]:
                    label = obj.Labels._items[ l ][ lab ]

                    text_node = root_node.createElement( 'text' )
                    text_node.setAttribute( 'context', l )
                    text_node.setAttribute( 'xml:lang', lab.lower() )
                    txt = root_node.createTextNode( label )
                    text_node.appendChild( txt )
                    labels_node.appendChild( text_node )
        else:
            text_node = root_node.createElement( 'text' )
            text_node.setAttribute( 'context', 'QUESTION' )
            text_node.setAttribute( 'xml:lang', 'en-us' )
            txt = root_node.createTextNode( obj.Label )
            text_node.appendChild( txt )
            labels_node.appendChild( text_node )

    def _add_languages( self, root_node, languages_node ):
        # Done
        with open( './ipsos/models/metadata_model/language_ids.tsv', mode='r', encoding='utf-8' ) as f:
            lines = f.readlines( )

        for key in self._document.Languages._items:
            lang = self._document.Languages[ key ]

            language_node = root_node.createElement( 'language' )
            language_node.setAttribute( 'name', key )

            for line in lines:
                items = line.replace( '\n', '' ).split( '\t' )

                if ( items[1].upper() == lang.Name.upper() ):
                    language_node.setAttribute( 'id', items[0] )
                    break

            # Check for alternate languages
            if ( len( lang.Alternatives ) > 0 ):
                alternatives_node = root_node.createElement( 'alternatives' )
                language_node.appendChild( alternatives_node )

                for alt in lang.Alternatives:
                    alternative_node = root_node.createElement( 'alternative' )
                    alternative_node.setAttribute( 'name', alt )
                    alternatives_node.appendChild( alternative_node )

            languages_node.appendChild( language_node )

    def _add_contexts( self, root_node, contexts_node ):
        # Done
        for key in self._document.Contexts._items:
            ctext = self._document.Contexts[ key ]

            context_node = root_node.createElement( 'context' )
            context_node.setAttribute( 'name', key )
            contexts_node.appendChild( context_node )

            # Check for alternatives
            if ( ctext.Alternatives ):
                alternatives_node = root_node.createElement( 'alternatives' )
                context_node.appendChild( alternatives_node )

                if ( type( ctext.Alternatives ) is list ):
                    for alt in ctext.Alternatives:
                        alternative_node = root_node.createElement( 'alternative' )
                        alternative_node.setAttribute( 'name', alt )
                        alternatives_node.appendChild( alternative_node )
                else:
                    alternative_node = root_node.createElement( 'alternative' )
                    alternative_node.setAttribute( 'name', ctext.Alternatives )
                    alternatives_node.appendChild( alternative_node )
        
    def _add_categorymap( self, root_node, categorymap_node ):
        # Done
        for key in self._document.CategoryMap._items:
            value = self._document.CategoryMap._items.get( key )

            categoryid_node = root_node.createElement( 'categoryid' )
            categoryid_node.setAttribute( 'name', key )
            categoryid_node.setAttribute( 'value', str( value ) )
            categorymap_node.appendChild( categoryid_node )
                
    def _add_document_level_properties( self, root_node, properties_node ):
        # Done
        pass

    def download_metadata_to_mdd( self ):
        # Convert in-memory metadata model to mdd
        mdd_id = uuid.uuid4( )
        
        # Create the root
        root_node = minidom.Document( )
        pi = root_node.createProcessingInstruction( 'xml-stylesheet', 'type="text/xsl" href="mdd.xslt"' )
        
        # Create the root node called 'xml'
        xml = root_node.createElement( 'xml' )
        root_node.appendChild( xml )
        root_node.insertBefore( pi, xml )

        # Create the metadata node to be nested in xml - done
        mdm_node = root_node.createElement( 'mdm:metadata' )
        mdm_node.setAttribute( 'mdm_createversion', '6.0.1.1.61' )
        mdm_node.setAttribute( 'mdm_createlastversion', '6.0.1.1.61' )
        mdm_node.setAttribute( 'id', str( mdd_id ) )
        mdm_node.setAttribute( 'data_version', '9' )
        mdm_node.setAttribute( 'data_sub_version', '1' )
        mdm_node.setAttribute( 'systemvariable', '1' )
        mdm_node.setAttribute( 'dbfiltervalidation', '-1' )
        mdm_node.setAttribute( 'xmlns:mdm', 'http://www.spss.com/mr/dm/metadatamodel/Arc 3/2000-02-04' )
        xml.appendChild( mdm_node )

        # Done
        # Add in the data sources
        datasources_node = root_node.createElement( 'datasources' )
        self._add_datasources( root_node, datasources_node, mdm_node )

        # Done
        # Add in document level properties
        properties_node = root_node.createElement( 'properties' )
        mdm_node.appendChild( properties_node )
        self._add_document_level_properties( root_node, properties_node )

        # Add in the defintion node
        definition_node = root_node.createElement( 'definition' )
        mdm_node.appendChild( definition_node )
        self._add_system_variables( root_node, definition_node, 'system_definition.xml' )

        # Add in the system class variables - done
        system_node = root_node.createElement( 'system' )
        system_node.setAttribute( 'name', '@fields' )
        system_node.setAttribute( 'global-name-space', '-1' )
        mdm_node.appendChild( system_node )
        self._add_system_variables( root_node, system_node, 'system.xml' )

        # Done
        # Add in the system routing - done
        systemrouting_node = root_node.createElement( 'systemrounting' )
        mdm_node.appendChild( systemrouting_node )
        self._add_system_routing( root_node, systemrouting_node )

        # Done
        # Add in the mappings node - no children to add as we are not adding variables that
        #  do not have case data - done
        mappings_node = root_node.createElement( 'mappings' )
        mdm_node.appendChild( mappings_node )

        # Done
        # Add in the design node (grids/loops/classes [non-system])
        design_node = root_node.createElement( 'design' )
        mdm_node.appendChild( design_node )

        # Done
        # Add the types node and append to design
        types_dict = { }
        types_node = root_node.createElement( 'types' )
        types_node.setAttribute( 'name', '@types' )
        types_node.setAttribute( 'global-name-space', '-1' )
        design_node.appendChild( types_node )
        self._add_shared_list( root_node, definition_node, types_node, types_dict )

        # Add the fields node and append to design
        fields_node = root_node.createElement( 'fields' )
        fields_node.setAttribute( 'name', '@fields' )
        fields_node.setAttribute( 'global-name-space', '-1' )
        design_node.appendChild( fields_node )
        self._add_fields( root_node, definition_node, fields_node, types_dict )

        # Done
        # Add in the languages
        languages_node = root_node.createElement( 'languages' )
        languages_node.setAttribute( 'base', 'EN-US' )
        mdm_node.appendChild( languages_node )
        self._add_languages( root_node, languages_node )

        # Done
        # Add in the default contexts (analysis/question)
        contexts_node = root_node.createElement( 'contexts' )
        contexts_node.setAttribute( 'base', 'Question' )
        mdm_node.appendChild( contexts_node )
        self._add_contexts( root_node, contexts_node )

        # Done
        # Add in the labeltypes node
        labeltypes_node = root_node.createElement( 'labeltypes' )
        labeltypes_node.setAttribute( 'base', 'Label' )
        mdm_node.appendChild( labeltypes_node )

        context_node = root_node.createElement( 'context' )
        context_node.setAttribute( 'name', 'LABEL' )
        labeltypes_node.appendChild( context_node )

        # Done
        # Add in the routingcontexts node
        routingcontexts_node = root_node.createElement( 'routingcontexts' )
        routingcontexts_node.setAttribute( 'base', 'Web' )
        mdm_node.appendChild( routingcontexts_node )

        context_node = root_node.createElement( 'context' )
        context_node.setAttribute( 'name', 'WEB' )
        routingcontexts_node.appendChild( context_node )

        context_node = root_node.createElement( 'context' )
        context_node.setAttribute( 'name', 'PAPER' )
        routingcontexts_node.appendChild( context_node )

        # Done
        # Add in the scripttypes node
        scripttypes_node = root_node.createElement( 'scripttypes' )
        scripttypes_node.setAttribute( 'base', 'mrScriptBasic' )
        mdm_node.appendChild( scripttypes_node )

        context_node = root_node.createElement( 'context' )
        context_node.setAttribute( 'name', 'MRSCRIPTBASIC' )
        scripttypes_node.appendChild( context_node )

        # Done
        # Add in the savelogs node
        savelogs_node = root_node.createElement( 'savelogs' )
        mdm_node.appendChild( savelogs_node )

        savelog_node = root_node.createElement( 'savelog' )
        savelog_node.setAttribute( 'fileversion', '6.0.1.1.61' )
        savelog_node.setAttribute( 'versionset', '' )
        savelog_node.setAttribute( 'username', 'xtrack cdm' )
        savelog_node.setAttribute( 'date', str( datetime.datetime.now() ) )
        savelogs_node.appendChild( savelog_node )

        user_node = root_node.createElement( 'user' )
        user_node.setAttribute( 'name', 'IBM SPSS Data Collection' )
        user_node.setAttribute( 'fileversion', '6.1.25865' )
        user_node.setAttribute( 'comment', 'Created by XTrack for Dimensions use.' )
        savelog_node.appendChild( user_node )

        # Done
        # Add in the atoms node
        atoms_node = root_node.createElement( 'atoms' )
        mdm_node.appendChild( atoms_node )

        # Done
        # Add in the labels node
        labels_node = root_node.createElement( 'labels' )
        labels_node.setAttribute( 'context', 'LABEL' )
        mdm_node.appendChild( labels_node )

        text_node = root_node.createElement( 'text' )
        text_node.setAttribute( 'context', 'QUESTION' )
        text_node.setAttribute( 'xml:lang', 'en-us' )
        labels_node.appendChild( text_node )

        # Done
        # Add in the versionlist node - no children being added
        versionlist_node = root_node.createElement( 'versionlist' )
        mdm_node.appendChild( versionlist_node )

        # Done
        # Add in the categorymap
        categorymap_node = root_node.createElement( 'categorymap' )
        mdm_node.appendChild( categorymap_node )
        self._add_categorymap( root_node, categorymap_node )

        # Convert xml to a string
        xml_str = root_node.toprettyxml( encoding='UTF-8' )

        # Output the xml to the mdd
        with open( self.mdd, 'bw' ) as f:
            f.write( xml_str )        