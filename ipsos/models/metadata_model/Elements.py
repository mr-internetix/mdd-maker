from collections import OrderedDict
from ipsos.models.metadata_model.Properties import Properties
from ipsos.models.metadata_model.Label import Label

class Elements:
    def __init__( self, id, name, category_map, language, context, ref = None ):
        self._items = OrderedDict()
        self.Id = id
        self.Name = name
        self.Label = ''
        self.IsReference = False
        self._base_language = language.upper()
        self._base_context = context.upper()
        self._category_map = category_map
        self.Ref = ref
        self.Labels = Label( language, context )
        self.Properties = Properties()

    def __getitem__( self, key ):
        return self._items[ key ]

    def __iter__( self ):
        return iter( [ self._items[ item ] for item in self._items ] )
