import ipsos.xmltodict as xmltodict
import functools, json, jsonpickle, logging, pprint

from ipsos.models.metadata_model.CategoryMap import CategoryMap
from ipsos.models.metadata_model.Contexts import Contexts
from ipsos.models.metadata_model.Languages import Languages
from ipsos.models.metadata_model.DataSources import DataSources
from ipsos.models.metadata_model.Fields import Fields
from ipsos.models.metadata_model.VariableInstances import VariableInstances
from ipsos.models.metadata_model.Types import Types
from ipsos.models.metadata_model.RoutingData import RoutingData
from ipsos.models.metadata_model.ScreeningTableList import ScreeningTableList
from ipsos.models.metadata_model.CustomProperty import CustomProperty

class Document():
    def __init__( self ):
        self._dict = None

    @property
    @functools.lru_cache()
    def CategoryMap( self ):
        return CategoryMap( self._dict )

    @property
    @functools.lru_cache()
    def Contexts( self ):
        return Contexts( self._dict )

    @property
    @functools.lru_cache()
    def Languages( self ):
        return Languages( self._dict )

    @property
    @functools.lru_cache()
    def CreatedByVersion( self ):
        return self._dict[ 'xml' ][ 'mdm:metadata' ][ '@mdm_createversion' ]

    @property
    @functools.lru_cache()
    def DataSources( self ):
        return DataSources( self._dict )

    @property
    @functools.lru_cache()
    def Fields( self ):
        return Fields( self._dict, self._types, self._category_map, self._languages.Base, self._contexts.Base )

    @property
    @functools.lru_cache()
    def VariableInstances( self ):
        return VariableInstances( self._fields )

    @property
    @functools.lru_cache()
    def Types( self ):
        return Types( self._dict, self._languages.Base, self._contexts.Base, self._category_map )
    
    @property
    @functools.lru_cache()
    def RoutingData(self):
        return RoutingData( self._dict)
    
    @property
    @functools.lru_cache()
    def ScreeningTableList(self):
        return ScreeningTableList( self._dict)
    
    @property
    @functools.lru_cache()
    def CustomProperty(self):
        return CustomProperty( self._dict)

    def Close( self ):
        self = None

    def Open( self, path ):
        logging.debug( "Opening " + path )
        self._path = path
        self._dict = self.toDict()
        
        self.serialize( "serialized.json" )

    def serialize( self, path ):
        logging.debug( "Serializing to " + path )
        # Instantiate all cacheable properties
        self._category_map = self.CategoryMap
        self._contexts = self.Contexts
        self._languages = self.Languages
        self._createdbyversion = self.CreatedByVersion
        self._datasources = self.DataSources
        self._types = self.Types
        self._fields = self.Fields
        self._variableinstances = self.VariableInstances


        # to do:  check versioning

        # Clean up
        # del( self._dict )


    def toDict( self ):
        logging.debug( "Converting MDD XML to dictionary" )
        return xmltodict.parse( open( self._path, "r", encoding = "utf-8" ).read(), ordered_mixed_children=True )

    def toJson( self, path, pretty = False ):
        logging.debug( "Saving MDD XML as JSON" )
        with open( path, "w", encoding = "utf-8" ) as f:
            if ( pretty ):
                print(json.dumps( self._dict,indent = 4, sort_keys = True))
                f.write( json.dumps( self._dict, indent = 4, sort_keys = True ) )
            else:
                f.write( json.dumps( self._dict ) )