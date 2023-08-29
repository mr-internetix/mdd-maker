from ipsos.models.metadata_model.Label import Label
from ipsos.models.metadata_model.Properties import Properties
from ipsos.models.metadata_model.Variable import Variable
from collections import OrderedDict


class Field:
    def __init__( self, name, uuid, objecttypevalue, isgrid, iteratortype, language, context ):
        self._items = OrderedDict()
        self.Name = name
        self.FullName = name
        self.Label = ''
        self.UUID = uuid
        self.IsGrid = isgrid
        self.ObjectTypeValue = objecttypevalue
        self.Type = 1
        self.Orientation = 'Horizontal'
        self.IteratorType = iteratortype
        self.IsSystem = False
        self.Elements = OrderedDict()
        self.Categories = OrderedDict()
        self.VariableInstances = OrderedDict()
        self.Labels = Label( language, context )
        self.Properties = Properties()
        self.OtherCategories = OrderedDict()
        self.HelperFields = OrderedDict()
                
    def __getitem__( self, key ):
        if ( key.find( '.' ) > 0 ):
            fullname = key.replace( '[..]', '' )
            q_parts = str(fullname).split('.')[ 1: ]

            field = self._items[ str(fullname).split('.')[ 0 ] ]
            for item in q_parts:
                if ( type( field ) == Variable ):
                    field = field.HelperFields[ item ]
                else:
                    field = field._items[ item ]
            return field
        else:
            return self._items[ key ]

    def __iter__( self ):
        return iter( [ self._items[ item ] for item in self._items ] )
