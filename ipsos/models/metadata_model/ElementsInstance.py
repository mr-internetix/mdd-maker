from collections import OrderedDict
from ipsos.models.metadata_model.Element import Element
from ipsos.models.metadata_model.Elements import Elements
from ipsos.models.metadata_model.Properties import Properties
from ipsos.models.metadata_model.Label import Label

class ElementsInstance:
    def __init__( self, types, category_map, language, context ):
        self._types = types
        self._base_language = language.upper()
        self._base_context = context.upper()
        self._category_map = category_map

    def _parse( self, d, var, xml_document, el ):
        other_vars_list = ''
        exclusive_count = 0
        if ( d is not None ):
            for key in d.keys():
                if ( key == "category" ): 
                    o_list, e_count = self._parse_category( d[ key ], var, other_vars_list, exclusive_count, el )
                    other_vars_list += o_list
                    exclusive_count += e_count
                if ( key == "categories" ): 
                    o_list, e_count = self._parse_categories( d[ key ], var, other_vars_list, exclusive_count, xml_document, el )
                    other_vars_list += o_list
                    exclusive_count += e_count
                if ( key == "@categoriesref" ): 
                    o_list, e_count = self._parse_categories( d[ key ], var, other_vars_list, exclusive_count, xml_document, el )
                    other_vars_list += o_list
                    exclusive_count += e_count

        return other_vars_list, exclusive_count

    def _parse_categories( self, o, var, other_vars_list, exclusive_count, xml_document, el ):
        if ( type( o ) == list ):
            for categories in o:
                e = Elements( categories[ '@id' ], categories[ '@name' ], self._category_map, self._base_language, self._base_context )
                if ( '@categoriesref' in categories ):
                    # Types list
                    e.IsReference = True
                    item = next( x for x in xml_document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'categories' ] if x[ '@id' ] == categories[ '@categoriesref' ] )
                    items = self._types[ item[ '@name' ] ]
                    e._items = items._items
                    if ( var is not None ):
                        self._add_types_categories( var, e )
                else:
                    o_list, e_count = self._parse( categories, var, xml_document, e )
                    other_vars_list += o_list
                    exclusive_count += e_count

                if ( "properties" in categories ): e.Properties._from_dict( categories[ "properties" ] )
                if ( "labels" in categories ): 
                    e.Labels._set_from_dict( categories[ 'labels' ] )
                else:
                    e.Labels.Text( self._base_language, self._base_context, categories[ '@name' ] )
                e.Label = e.Labels.Label

                if ( el is not None ):
                    el._items[ e.Name ] = e
                elif ( var is not None ):
                    var.Elements[ e.Name ] = e
        elif ( type( o ) == OrderedDict ):
            e = Elements( o[ '@id' ], o[ '@name' ], self._category_map, self._base_language, self._base_context )
            if ( '@categoriesref' in o ):
                # Types list
                e.IsReference = True
                item = next( x for x in xml_document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'categories' ] if x[ '@id' ] == o[ '@categoriesref' ] )
                items = self._types[ item[ '@name' ] ]
                e._items = items._items
                if ( var is not None ):
                    self._add_types_categories( var, e )
            else:
                o_list, e_count = self._parse( o, var, xml_document, e )
                other_vars_list += o_list
                exclusive_count += e_count

            if ( "properties" in o ): e.Properties._from_dict( o[ "properties" ] )
            if ( "labels" in o ): 
                e.Labels._set_from_dict( o[ 'labels' ] )
            else:
                e.Labels.Text( self._base_language, self._base_context, o[ '@name' ] )
            e.Label = e.Labels.Label

            if ( el is not None ):
                el._items[ e.Name ] = e
            elif ( var is not None ):
                var.Elements[ e.Name ] = e
        else:
            item = next( x for x in xml_document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'categories' ] if x[ '@id' ] == o )
            if ( item ):
                e = Elements( item[ '@id' ], item[ '@name' ], self._category_map, self._base_language, self._base_context )
                # Types list
                e.IsReference = True
                items = self._types[ item[ '@name' ] ]
                e._items = items._items
                if ( var is not None ):
                    self._add_types_categories( var, e )

                if ( el is not None ):
                    el._items[ e.Name ] = e
                elif ( var is not None ):
                    var.Elements[ e.Name ] = e
            else:
                raise NotImplementedError( "Error parsing categories in Elements._parse_categories().  Unexpected type." )
        
        return other_vars_list, exclusive_count

    def _add_types_categories( self, var, el ):
        for name, e in el._items.items():
            if ( type( e ) == Elements ):
                self._add_types_categories( var, e )
            else:
                var.Categories[ name ] = e

    def _parse_category( self, o, var, other_vars_list, exclusive_count, el ):
        if ( type( o ) == list ):
            for category in o:
                e = Element( category[ '@id' ], category[ '@name' ], self._category_map[ category[ '@name' ] ], self._base_language, self._base_context )
                if ( "@factor-value" in category ): e.Factor = category[ '@factor-value' ]
                if ( "@fixed" in category ):
                    if ( category[ '@fixed' ] == '-1' ):
                        e.Fixed = True
                if ( "@exclusive" in category ): 
                    if ( category[ '@exclusive' ] == '-1' ):
                        e.Exclusive = True
                        exclusive_count += 1
                if ( "labels" in category ): 
                    e.Labels._set_from_dict( category[ 'labels' ] )
                else:
                    e.Labels.Text( self._base_language, self._base_context, category[ '@name' ] )
                e.Label = e.Labels.Label
                if ( "properties" in category ): e.Properties._from_dict( category[ "properties" ] )

                if ( el is not None ):
                    el._items[ e.Name ] = e
                elif ( var is not None ):
                    var.Elements[ e.Name ] = e

                if ( var is not None ):
                    var.Categories[ e.Name ] = e

                if ( 'othervariable' in category ):
                    for h in category[ 'othervariable' ]:
                        if ( h == '@ref' ):
                            other_vars_list += ',' + category[ 'othervariable' ][ '@ref' ]
                            break
        else:
            e = Element( o[ '@id' ], o[ '@name' ], self._category_map[ o[ '@name' ] ], self._base_language, self._base_context )
            if ( "@factor-value" in o ): e.Factor = o[ '@factor-value' ]
            if ( "@fixed" in o ):
                if ( o[ '@fixed' ] == '-1' ):
                    e.Fixed = True
            if ( "@exclusive" in o ): 
                if ( o[ '@exclusive' ] == '-1' ):
                    e.Exclusive = True
                    exclusive_count += 1
            if ( 'labels' in o ):
                e.Labels._set_from_dict( o[ 'labels' ] )
            else:
                e.Labels.Text( self._base_language, self._base_context, o[ '@name' ] )
            e.Label = e.Labels.Label
            if ( "properties" in o ): e.Properties._from_dict( o[ "properties" ] )

            if ( el is not None ):
                el._items[ e.Name ] = e
            elif ( var is not None ):
                var.Elements[ e.Name ] = e

            if ( var is not None ):
                var.Categories[ e.Name ] = e

            if ( "othervariable" in o ):
                for h in o[ 'othervariable' ]:
                    if ( h == '@ref' ):
                        other_vars_list += ',' + o[ 'othervariable' ][ '@ref' ]
                        break
        
        return other_vars_list, exclusive_count
