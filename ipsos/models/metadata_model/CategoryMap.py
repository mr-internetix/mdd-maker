import itertools, logging
from collections import OrderedDict

class CategoryMap:
    def __init__( self, mdm_dict ):
        logging.debug( "Instantiating Category Map" )
        self._items = OrderedDict()
        self._document = mdm_dict
        self._parse()

    def __getitem__( self, key ):
        if ( self._items.get( key.lower() ) ):
            return self._items[ key.lower() ]
        else:
            return -1

    def __iter__( self ):
        return iter( [ self._items[ item ] for item in self._items ] )

    def _parse( self ):
        for item in self._document[ 'xml' ][ 'mdm:metadata' ][ 'categorymap' ][ 'categoryid' ]:
            self._items[ item[ '@name' ].lower() ] = int( item[ '@value' ] )
            
        del( self._document )

    def NameToValue( self, name ):
        return self._items[ name.lower() ]

    def ValueToName( self, value ):
        for k, v in self._items.items():
            if ( v == value ): return k
