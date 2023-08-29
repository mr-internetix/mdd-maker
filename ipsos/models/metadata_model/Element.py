from ipsos.models.metadata_model.Label import Label
from ipsos.models.metadata_model.Properties import Properties


class Element:
    def __init__( self, id, name, value, language, context ):
        self.Id = id
        self.Name = name
        self.Value = int( value )
        self.Factor = None
        self.Label = ''
        self.Labels = Label( language, context )
        self.Fixed = False
        self.Exclusive = False
        self.Properties = Properties()
        