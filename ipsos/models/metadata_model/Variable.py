from ipsos.models.metadata_model.Label import Label
from ipsos.models.metadata_model.Properties import Properties
from collections import OrderedDict


class Variable:
    def __init__( self, name, uuid, datatype, minvalue, maxvalue, objecttypevalue, usagetype, language, context ):
        self.Name = name
        self.FullName = name
        self.Label = ''
        self.UUID = uuid
        self.DataType = datatype
        self.ObjectTypeValue = objecttypevalue 
        self.MinValue = minvalue
        self.MaxValue = maxvalue
        self.UsageType = usagetype
        self.HasCaseData = True
        self.DefaultAnswer =None
        self.Expression = None
        self.AxisExpression = None
        self.Namespace = None
        self.IsSystem = False
        self.Elements = OrderedDict()
        self.Categories = OrderedDict()
        self.VariableInstances = OrderedDict()
        self.Labels = Label( language, context )
        self.Properties = Properties()
        self.OtherCategories = OrderedDict()
        self.HelperFields = OrderedDict()