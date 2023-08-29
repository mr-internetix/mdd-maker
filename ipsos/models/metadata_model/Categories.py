from collections import OrderedDict

class Categories:
    def __init__( self ):
        self._items = OrderedDict()

    def __getitem__( self, key ):
        return self._items[ key ]

    def __iter__( self ):
        return iter( [ self._items[ item ] for item in self._items ] )
