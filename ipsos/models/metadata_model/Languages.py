import logging
from collections import OrderedDict
from ipsos.models.metadata_model.Language import Language

class Languages:

    def __init__( self, mdm_dict ):
        logging.debug( "Instantiating Languages" )
        self._items = OrderedDict()
        self._document = mdm_dict
        self._parse()

    def __getitem__( self, key ):
        return self._items[ key ]

    def __iter__( self ):
        return iter( [ self._items[ item ] for item in self._items ] )

    def _parse( self ):
        self.Base = self._document[ 'xml' ][ 'mdm:metadata' ][ 'languages' ][ '@base' ]
        for item in self._document[ 'xml' ][ 'mdm:metadata' ][ 'languages' ][ 'language' ]:
            l = None
            if ( type( item ) is str ):
                l = Language( self._document[ 'xml' ][ 'mdm:metadata' ][ 'languages' ][ 'language' ][ '@name' ] )
            else:
                l = Language( item[ '@name' ] )

            if ( 'alternatives' in item ):
                if ( type( item[ 'alternatives' ][ 'alternative' ] ) == list ):
                    alternative = []
                    for alt in item[ 'alternatives' ][ 'alternative' ]:
                        alternative.append( alt[ '@name' ] )
                    l.Alternatives = alternative
                else:
                    l.Alternatives = [ item[ 'alternatives' ][ 'alternative' ][ '@name' ] ]
            
            if ( type( item ) is str ):
                self._items[ self._document[ 'xml' ][ 'mdm:metadata' ][ 'languages' ][ 'language' ][ '@name' ] ] = l
            else:
                self._items[ item[ '@name' ] ] = l
        del self._document