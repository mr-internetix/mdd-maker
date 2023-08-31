
# imports 
# imports
from ipsos.dimensions.mdd import MDD
from ipsos.dimensions.ddf import DDF
import pandas as pd
import datetime
import re



path_of_mdd = r"./mdd_files/S23032595.mdd"

# path_of_mdd = r"./mdd_files/S23032063.mdd"

def get_custom_inputs_mdd(mdd_path,export = False):    
    try:
        my_mdd = DDF(path_of_mdd)
        data = my_mdd.mdm.CustomProperty._get_custom_property_idatagenerator()
        global all_variables_df 
        all_variables_df= None

        global custom_property_df
        custom_property_df = None


        if data is not  None:
            custom_property_df  = pd.DataFrame(data)
            custom_property_df["custom_property"] = custom_property_df["custom_property"].astype(str)
            custom_property_df = custom_property_df.loc[custom_property_df['custom_property'] != "None" , :]
            if export == True:
                custom_property_df.to_excel("./data/new_output.xlsx", index=False)

        # data_type_recoding        
        data_type_recode ={
            1: "numeric",
            2:"text",
            3:"categorical",
            6:"double",
            5: "date",
            7:"boolean"
        }

        variables_names = []
        variables_datatype =[]
        variables_min_values = []
        varibales_max_values = []
        variables_labels = []
        categorical_values = []
        only_categories = []


        for var_name in my_mdd._list_of_all_var_names():
            variables_names.append(var_name)
            variables_datatype.append(my_mdd._get_variable_datatype(var_name))
            variables_min_values.append(my_mdd._get_variable_min_value(var_name))
            varibales_max_values.append(my_mdd._get_variable_max_value(var_name))
            variables_labels.append(my_mdd._get_variable_label(var_name))
            categorical_values.append(my_mdd.get_category_dict(var_name))
            only_categories.append(my_mdd.get_only_category(var_name))


        data = {
            "variables_names": variables_names,
            "variables_datatype": variables_datatype,
            "variables_min_values": variables_min_values,
            "variables_max_values": varibales_max_values,
            "variables_labels": variables_labels,
            "categorical_values": categorical_values,
            "only_categories":only_categories,                     
            }
        

        try:
            all_variables_df = pd.DataFrame(data)


            all_variables_df['variable_datatype_names'] = all_variables_df["variables_datatype"].map(data_type_recode)


            def split_varibales(string):
                try:
                    value = str(string).split("[")[0].lower()

                    return value
                except Exception as e:
                    return string



            all_variables_df['new_variable_names']= all_variables_df['variables_names'].apply(split_varibales).str.lower().astype(str)

            if export != True:
                all_variables_df.to_excel("./data/final_data.xlsx",index=False)
        except Exception as e:
            pass

    except Exception as e:
        print(e)
    
    finally:
        return custom_property_df , all_variables_df  


custom_df , all_df  = get_custom_inputs_mdd(path_of_mdd)


print(all_df)


# print(custom , all)

