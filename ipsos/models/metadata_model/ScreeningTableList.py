

class ScreeningTableList:


    # constructor
    def __init__(self,mdm_dict):
        self._document = mdm_dict


    
    def slice_string(self,text):
        try:
            slice_point = text.find('on') + 2 # The plus 2 moves past the 'on'
            sliced_string = text[slice_point:]
            return sliced_string.strip() # strip()
        except Exception as e:
            return text

    
    # methods

    def _get_screening_table_List(self):
        """
        Returns Screening Table List present in metadata
        with precode , question and custom properties set to it
        
        """
        try:

            json_obj = self._document ['xml']['mdm:metadata']['definition']['categories'] 
            
            precodes= []
            name_of_variables = []
            custom_property_ = []
            custom_property_value = []

            for values in json_obj:
                if values['@name'] == "ScreeningTableList":
                    try:
                        first_category = values['category'][0]
                        if first_category['properties']:
                            all_the_values = values['category']
                            break
                    except Exception as e:
                        pass


            for category in all_the_values:
                precode = category['@name']
                name = self.slice_string(category.get('labels', {}).get('text', [{}])[0].get('#text',{}))
                property = None
                value = None
                try:
                    property = category['properties']['property']['@name']
                    value = category['properties']['property']['@value']    
                except Exception as e:
                    pass

                precodes.append(precode)
                name_of_variables.append(name)
                custom_property_.append(property)
                custom_property_value.append(value)


            return {'precodes': precodes,'name_of_variables':name_of_variables,
                        'custom_property':custom_property_,
                        'custom_property_value':custom_property_value}
        



        except Exception as e:
            pass


