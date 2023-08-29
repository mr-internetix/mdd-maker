from ipsos.models.metadata_model.Label import Label
from collections import OrderedDict


class VariableInstance:
    def __init__( self, name, uuid, datatype, minvalue, maxvalue, objecttypevalue, usagetype ):
        self.Name = name
        self.FullName = name
        self.Label = ''
        self.Indexes = None
        self.UUID = uuid
        self.DataType = datatype
        self.ObjectTypeValue = objecttypevalue 
        self.MinValue = minvalue
        self.MaxValue = maxvalue
        self.HasCaseData = True
        self.UsageType = usagetype
        self.Expression = None
        self.IsSystem = False
        self.Elements = OrderedDict()
        self.Categories = OrderedDict()
        self.Labels = OrderedDict()
        self.Indices = list()
