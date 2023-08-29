import json, logging
from collections import OrderedDict
from ipsos.models.metadata_model.Type import Type
from ipsos.models.metadata_model.Element import Element
from ipsos.models.metadata_model.Elements import Elements
from ipsos.models.metadata_model.ElementsInstance import ElementsInstance
from ipsos.models.metadata_model.Label import Label


class Types:

    def __init__( self, mdm_dict, language, context, category_map ):
        logging.debug( "Instantiating Types" )
        self._items = OrderedDict()
        self._document = mdm_dict
        self._base_language = language.upper()
        self._base_context = context.upper()
        self._category_map = category_map
        self._parse( )

    def __getitem__( self, key ):
        return self._items[ key ]

    def __iter__( self ):
        return iter( [ self._items[ item ] for item in self._items ] )

    def _parse( self ):
        if ( "types" in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ] ):
            if ( "categories" in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'types' ] ):
                for item in self._document[ 'xml' ][ 'mdm:metadata' ][ 'design' ][ 'types' ][ 'categories' ]:
                    self._items[ item[ '@name' ] ] = Type( item[ '@id' ], item[ '@name' ], self._category_map, self._base_language, self._base_context, item[ '@ref' ] )
                    d = next( x for x in self._document[ 'xml' ][ 'mdm:metadata' ][ 'definition' ][ 'categories' ] if x[ '@name' ] == item[ '@name' ] )
                    if ( "properties" in d ): self._items[ item[ '@name' ] ].Properties._from_dict( d[ "properties" ] )
                    e = ElementsInstance( self._items, self._category_map, self._base_language, self._base_context )
                    e._parse( d, None, self._document, self._items[ item[ '@name' ] ] )

        del( self._document )
        del( self._base_language )
        del( self._base_context )
        del( self._category_map )
