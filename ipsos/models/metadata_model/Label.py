class Label:
    def __init__( self, language, context ):
        self._items = {}
        self.Label = ''
        self._base_language = language.upper()
        self._base_context = context.upper()

    def Text( self, language, context, value ):
        if ( context.upper() not in self._items ): self._items[ context.upper() ] = {}
        self._items[ context.upper() ][ language.upper() ] = value
        
        if ( context.upper() == self._base_context and language.upper() == self._base_language ):
            self.Label = value

    def TextAt( self, language, context ):
        try:
            return self._items[ context.upper() ][ language.upper() ]
        except:
            # No label for Language/Context combination
            return ''

    def _set_from_dict( self, o ):
        if ( type( o[ 'text' ] ) == list ):
            for item in o[ 'text' ]:
                if ( "#text" in item ): self.Text( item[ '@xml:lang' ], item[ '@context' ], item[ '#text' ] )
        else:
            if ( "#text" in o[ 'text' ] ): self.Text( o[ 'text' ][ '@xml:lang' ], o[ 'text' ][ '@context' ], o[ 'text' ][ '#text' ] )
    
        del ( self._base_language )
        del ( self._base_context )