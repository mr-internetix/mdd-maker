import logging
import itertools
from collections import OrderedDict
from ipsos.models.metadata_model.Class import Class
from ipsos.models.metadata_model.Field import Field
from ipsos.models.metadata_model.Variable import Variable
from ipsos.models.metadata_model.VariableInstance import VariableInstance
from ipsos.models.metadata_model.Element import Element
from ipsos.models.metadata_model.Elements import Elements
from ipsos.models.metadata_model.Categories import Categories
from ipsos.models.metadata_model.Types import Types


class VariableInstances:
    def __init__( self, fields ):
        self._items = OrderedDict()
        self._Fields = fields
        self._create_variableinstances()

    def __getitem__( self, key ):
        return self._items[ key ]

    def __iter__( self ):
        return iter( [ self._items[ item ] for item in self._items ] )

    def _create_variableinstances( self ):
        for item in self._Fields._items:
            o = self._Fields._items[ item ]
            if ( type( o ) is Variable ):
                self._get_variableinstance( o, o.Name, None, None )
            elif ( type( o ) is Class ):
                # For classes need to build the fully qualified name
                self._build_class_variableinstances( o )
            elif ( type( o ) is Field ):
                # For grids need to build the fully qualified name
                name_dict = {}
                elem_dict = {}
                quest_list = []
                iter = 1
                self._build_grid_variableinstances( name_dict, elem_dict, quest_list, o, iter )

        del self._Fields
        
    def _get_variableinstance( self, f, fullname, el, quest_list ):
        if ( f.DataType != 0 ):
            # No variable instances for info items
            var_inst = self._add_variableinstance( f, fullname )
            var_inst.IsSystem = f.IsSystem
            
            if ( el is not None ):
                elems = el.split( '|' )
                indexes = ''
                for i in range( 0, len( elems ) - 1 ):
                    o = quest_list[ i ]
                    if ( o.IteratorType == '3' ):
                        indexes += elems[ i ] + ','
                    else:
                        indexes += '{' + elems[ i ] + '},'
                    index = {}
                    index[ elems[ i ] ]  = o.Categories[ elems[ i ] ]
                    var_inst.Indices.append( index )
                var_inst.Indexes = indexes[:-1]
                o = quest_list[ 0 ]
                o.VariableInstances[ var_inst.FullName ] = var_inst
                self._items[ var_inst.FullName ] = var_inst

                if ( len( f.HelperFields ) > 0 ):
                    self._get_helperfield_variableinstances( o, f, fullname, var_inst )
            else:
                f.VariableInstances[ var_inst.FullName ] = var_inst
                self._items[ var_inst.FullName ] = var_inst
                if ( len( f.HelperFields ) > 0 ):
                    self._get_helperfield_variableinstances( f, f, fullname, None )

    def _get_helperfield_variableinstances( self, o, f, fullname, var_inst ):
        for h in f.HelperFields:
            help_var = f.HelperFields.get( h )

            hvar_inst = self._add_variableinstance( help_var, fullname )
            hvar_inst.FullName = fullname + '.' + hvar_inst.Name
            if ( var_inst ):
                hvar_inst.Indexes = var_inst.Indexes
                hvar_inst.Indices = var_inst.Indices
            else:
                hvar_inst.Indexes = None
                hvar_inst.Indices = None
            
            help_var.FullName = fullname + '.' + hvar_inst.Name
            f.VariableInstances[ hvar_inst.FullName ] = hvar_inst

            if ( o is not f ):
                o.VariableInstances[ hvar_inst.FullName ] = hvar_inst

            self._items[ hvar_inst.FullName ] = hvar_inst

    def _add_variableinstance( self, f, fullname ):

        # print(f)
        name = f.Name
        uuid = f.UUID
        datatype = f.DataType
        minvalue = f.MinValue
        maxvalue = f.MaxValue
        objecttypevalue = f.ObjectTypeValue
        usagetype = f.UsageType

        var_inst = VariableInstance( name, uuid, datatype, minvalue, maxvalue, objecttypevalue, usagetype )
        
        var_inst.FullName = fullname
        var_inst.Elements = f.Elements
        var_inst.Categories = f.Categories
        var_inst.Label = f.Label
        var_inst.HasCaseData = f.HasCaseData
        var_inst.Expression = f.Expression

        return var_inst

    def _build_grid_variableinstances( self, name_dict, elem_dict, quest_list, f, iter ):
        name = f.Name
        varinst_names = [ ]
        elem_names = [ ]
        for e in f.Categories:
            if ( f.IteratorType == '3' ):
                varinst_names.append( name + '[' + e + ']' )
            else:
                varinst_names.append( name + '[{' + e + '}]' )
            elem_names.append( e )

        name_dict[ iter ] = varinst_names
        elem_dict[ iter ] = elem_names
        quest_list.append( f )

        for field in f._items:
            fld = f._items[ field ]
            if ( type( fld ) is Variable ):
                name_keys = name_dict.keys()
                name_values = (name_dict[key] for key in name_keys)
                name_combos = [dict(zip(name_keys, combination)) for combination in itertools.product(*name_values)]

                elem_keys = elem_dict.keys()
                elem_values = (elem_dict[key] for key in elem_keys)
                elem_combos = [dict(zip(elem_keys, combination)) for combination in itertools.product(*elem_values)]

                cntr = -1
                for part in name_combos:
                    cntr += 1
                    fullname = ''
                    elements = ''
                    for i in range( 1, len( part ) + 1 ):
                        key = list( part )[ i - 1 ]
                        fullname += part.get( key ) + '.'
                        elements += elem_combos[ cntr ].get( key ) + '|'
                    fullname += field
                    self._get_variableinstance( fld, fullname, elements, quest_list )

                var_fullname = ''
                for i in range( 0, len( quest_list ) ):
                    o = quest_list[ i ]
                    var_fullname += o.Name + '[..].'
                fld.FullName = var_fullname + fld.Name

                # if ( len( quest_list ) > 1 ):
                #     quest_list.pop()
                #     name_dict.popitem()
                #     elem_dict.popitem()
            if ( type( fld ) is Field ):
                self._build_grid_variableinstances( name_dict, elem_dict, quest_list, fld, iter + 1 )

                if ( len( quest_list ) > 1 ):
                    quest_list.pop()
                    name_dict.popitem()
                    elem_dict.popitem()

    def _build_class_variableinstances( self, f ):
        for field in f._items:
            fld = f._items[ field ]
            if ( type( fld ) is Variable ):
                fullname = f.Name + '.' + field
                fld.FullName = fullname
                self._get_variableinstance( fld, fullname, None, None )
