import logging
from collections import OrderedDict

class Properties:
    def __init__( self ):
        self._items = []

    def __getitem__( self, key ):
        return self._items[ key ]

    def __iter__( self ):
        return iter( [ item for item in self._items ] )

    def _from_dict( self, d ):
        if ( "property" in d ):
            if ( type( d[ "property" ] ) == list ):
                for o in d[ "property" ]:
                    if ( '@type' in o and '@value' in o ):
                        p = Property( o[ '@context' ], o[ '@name' ], o[ '@type' ], o[ '@value' ] )
                        if ( "properties" in o ): p.Properties._from_dict( o[ "properties" ] )
                        self._items.append( p )   
            else:
                o = d[ 'property' ]
                if ( '@type' in o and '@value' in o ):
                    p = Property( o[ '@context' ], o[ '@name' ], o[ '@type' ], o[ '@value' ] )
                    if ( "properties" in o ): p.Properties._from_dict( o[ "properties" ] )
                    self._items.append( p )

class Property:
    def __init__( self, context, name, t, value ):
        self.Context = context
        self.Name = name
        self.Type = t
        self.Value = value
        self.Properties = Properties()