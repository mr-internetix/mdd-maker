from ipsos.models.metadata_model.Label import Label
from ipsos.models.metadata_model.Properties import Properties
from collections import OrderedDict


class Class:
    def __init__( self, name, uuid, language, context ):
        self._items = OrderedDict()
        self.Name = name
        self.Label = ''
        self.UUID = uuid
        self.ObjectTypeValue = 3 
        self.IsSystem = False 
        self.VariableInstances = OrderedDict()
        self.Labels = Label( language, context )
        self.Properties = Properties()
        self.HelperFields = OrderedDict()

    def __getitem__( self, key ):
        return self._items[ key ]

    def __iter__( self ):
        return iter( [ self._items[ item ] for item in self._items ] )

