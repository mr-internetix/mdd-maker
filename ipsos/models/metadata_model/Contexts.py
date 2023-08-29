import logging
from collections import OrderedDict
from ipsos.models.metadata_model.Context import Context

class Contexts:

    def __init__( self, mdm_dict ):
        logging.debug( "Instantiating Contexts" )
        self._items = OrderedDict()
        self._document = mdm_dict
        self._parse()

    def __getitem__( self, key ):
        return self._items[ key ]

    def __iter__( self ):
        return iter( [ self._items[ item ] for item in self._items ] )

    def _parse( self ):
        self.Base = self._document[ 'xml' ][ 'mdm:metadata' ][ 'contexts' ][ '@base' ]
        for item in self._document[ 'xml' ][ 'mdm:metadata' ][ 'contexts' ][ 'context' ]:
            if ( 'alternatives' in item ):
                if ( type( item[ 'alternatives' ][ 'alternative' ] ) == list ):
                    alternatives = ''
                    for alt in item[ 'alternatives' ][ 'alternative' ]:
                        alternatives += alt[ '@name' ] + ', '
                    alternatives = alternatives[:-2]
                else:
                    alternatives = item[ 'alternatives' ][ 'alternative' ][ '@name' ]
            else:
                alternatives = None
            self._items[ item[ '@name' ] ] = Context( item[ '@name' ], alternatives )
        del self._document