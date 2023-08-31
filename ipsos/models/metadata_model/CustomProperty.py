
class CustomProperty:

    # constructor
    def __init__(self,mdm_dict) -> None:
        self._document = mdm_dict

    
    def slice_string(self,text):
        try:
            slice_point = text.find('on') + 2 # The plus 2 moves past the 'on'
            sliced_string = text[slice_point:]
            return sliced_string.strip() # strip()
        except Exception as e:
            print(e)
            return text
    


    # method get custom property_iDatagenerator
    def _get_custom_property_idatagenerator(self):
        """
        Returns the custom property of idatagenerator on the variables

        """

        try:
            json_obj  = self._document ['xml']['mdm:metadata']['definition']['variable']

            '''variables for idatagenerator'''
            # var_names = []
            # var_property_value = []
            name_of_variables = []
            custom_property_ = []
            custom_property_value = []

            
            for var in json_obj:
                try:
                    current_var_property = var['properties']['property']

                    for property in current_var_property:
                        if property['@name'].lower()  == "idatagenerator":
                            name_of_variables.append(var['@name'])
                            custom_property_.append("idatagenerator")
                            custom_property_value.append(property['@value'])
                except Exception as e:
                    pass
            


            """
            Code for getting screening Table list properties 
            
            """
            screening_table_object = self._document ['xml']['mdm:metadata']['definition']['categories'] 
            all_the_values = None
            
            for values in screening_table_object:
                if values['@name'] == "ScreeningTableList":
                    try:
                        first_category = values['category'][0]
                        if first_category['properties']: 
                            all_the_values = values['category']
                            break
                    except Exception as e:
                        pass
            if all_the_values is not None:
                for category in all_the_values:
                    try:
                        name = self.slice_string(category.get('labels', {}).get('text', [{}])[0].get('#text',{}))
                    except Exception as e:
                        name = self.slice_string(category.get('labels', {}).get('text', [{}]).get('#text',{}))

                    property = None
                    value = None
                    try:
                        property = (category['properties']['property']['@name']).lower()
                        value = (category['properties']['property']['@value']).lower()    
                    except Exception as e:
                        pass

                    name_of_variables.append(name)
                    custom_property_.append(property)
                    custom_property_value.append(value)

                return {
                    'name_of_variables':name_of_variables,
                            'custom_property':custom_property_,
                            'custom_property_value':custom_property_value}

            else:
                return None 
                      


        except Exception as e:
            print(e)