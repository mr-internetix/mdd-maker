import logging
from collections import OrderedDict
from ipsos.models.metadata_model.DataSource import DataSource

class DataSources:

    def __init__( self, mdm_dict ):
        logging.debug( "Instantiating DataSources" )
        self._items = OrderedDict()
        self._document = mdm_dict
        self._parse()

    def __getitem__( self, key ):
        return self._items[ key ]

    def __iter__( self ):
        return iter( [ self._items[ item ] for item in self._items ] )

    def _parse( self ):
        self.Default = self._document[ 'xml' ][ 'mdm:metadata' ][ 'datasources' ][ '@default' ]

        if ( type( self._document[ 'xml' ][ 'mdm:metadata' ][ 'datasources' ][ 'connection' ] ) is list ):
            for connection in self._document[ 'xml' ][ 'mdm:metadata' ][ 'datasources' ][ 'connection' ]:
                self._items[ connection[ '@name' ] ] = DataSource( connection[ '@cdscname' ], connection[ '@dblocation' ], connection[ '@name' ], connection[ '@project' ] )
        else:
            connection = self._document[ 'xml' ][ 'mdm:metadata' ][ 'datasources' ][ 'connection' ]
            self._items[ connection[ '@name' ] ] = DataSource( connection[ '@cdscname' ], connection[ '@dblocation' ], connection[ '@name' ], connection[ '@project' ] )

        del self._document