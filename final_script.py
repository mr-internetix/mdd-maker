
# imports 
# imports
from ipsos.dimensions.mdd import MDD
from ipsos.dimensions.ddf import DDF
import pandas as pd
import datetime
import re



# recording time 
start = datetime.datetime.now()
path_of_mdd = r"./mdd_files/S23032063.mdd"
my_mdd = DDF(path_of_mdd)
routing_data = my_mdd.mdm.RoutingData._get_routing_data()
get_custom_properties_screener_list = my_mdd.mdm.ScreeningTableList._get_screening_table_List()



data = my_mdd.mdm.CustomProperty._get_custom_property_idatagenerator()
new_df  = pd.DataFrame(data)
new_df["custom_property"] = new_df["custom_property"].astype(str)
new_df = new_df.loc[new_df['custom_property'] != "None" , :]
new_df.to_excel("./data/new_output.xlsx", index=False)


print(new_df)

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
# variable_present_in_routing = []
# precodes_for_property  = get_custom_properties_screener_list['precodes']
# name_of_variables = get_custom_properties_screener_list['name_of_variables']
# custom_property = get_custom_properties_screener_list['custom_property']
# custom_property_value = get_custom_properties_screener_list['custom_property_value']  




for var_name in my_mdd._list_of_all_var_names():
    variables_names.append(var_name)
    variables_datatype.append(my_mdd._get_variable_datatype(var_name))
    variables_min_values.append(my_mdd._get_variable_min_value(var_name))
    varibales_max_values.append(my_mdd._get_variable_max_value(var_name))
    variables_labels.append(my_mdd._get_variable_label(var_name))
    categorical_values.append(my_mdd.get_category_dict(var_name))
    only_categories.append(my_mdd.get_only_category(var_name))

    # if var_name in routing_data:
    #     variable_present_in_routing.append("1")
    # else:
    #     variable_present_in_routing.append("2")


data = {
    "variables_names": variables_names,
    "variables_datatype": variables_datatype,
    "variables_min_values": variables_min_values,
    "variables_max_values": varibales_max_values,
    "variables_labels": variables_labels,
    "categorical_values": categorical_values,
    "only_categories":only_categories,
    # "variable_present_in_routing":variable_present_in_routing,
                                                           

    }

# new_df_data = {
    
#      "precodes_for_property" : get_custom_properties_screener_list['precodes'],
#     "name_of_variables" :get_custom_properties_screener_list['name_of_variables'],
#     "custom_property":get_custom_properties_screener_list['custom_property'],
#     "custom_property_value":get_custom_properties_screener_list['custom_property_value']

# }


# custom_df = pd.DataFrame(new_df_data)
# custom_df.to_excel("./data/new_df_data.xlsx")



# custom_df


def filter_dataframe(df, col_name, value_list):
    
    # Escape special characters in values
    escaped_values = [re.escape(value) for value in value_list]

    print(escaped_values)

    # Create pattern, adding word bounds
    pattern = '|'.join(r"\b{}\b".format(x) for x in escaped_values)


    print(pattern)

    # filter the dataframe
    df_filtered = df[df[col_name].str.contains(pattern, case=False,na=False, regex=True)]
   
    return df_filtered



df = pd.DataFrame(data)
df['variable_datatype_names'] = df["variables_datatype"].map(data_type_recode)
# df_filtered = filter_dataframe(df,"variables_names",name_of_variables)




# new_df = pd.DataFrame(new_df_data)


def split_varibales(string):
    try:
        value = str(string).split("[")[0].lower()

        return value
    except Exception as e:
        return string






def split_basis_on(string):
    try:
        value = str(string).split("on")[1].lower()
        return value

    except Exception as e:
        return string


df['new_variable_names']= df['variables_names'].apply(split_varibales).str.lower().astype(str)

# new_df['new_variable_names'] = new_df['new_variable_names'].str.lower()
# df['new_variable_names']= df['variables_names'].apply(lambda x : x[ : x.find("[")].lower())
# print(df['new_variable_names'])


# new_df['name_of_variables2'] = new_df['name_of_variables'].apply(split_basis_on).str.lower()




# df


# new_df


# df


# new_df = new_df.loc[new_df["custom_property"]=="IDATAGENERATOR", :]


# new_df


# # final_df = pd.merge(df,new_df , how="left", left_on="new_variable_names", right_on="name_of_variables2")
# final_df = new_df.merge(df, how="left", right_on="new_variable_names", left_on="name_of_variables2")
# re_arranging_columns = ['variables_names','new_variable_names','variables_min_values','variables_max_values',
#                         'categorical_values','only_categories','variable_present_in_routing','variable_datatype_names','custom_property','custom_property_value']

                        


# final_df.loc[final_df["new_variable_names"] == "resp_age"]


# columns_to_drop = ['precodes_for_property','name_of_variables','variables_datatype','name_of_variables2']
# columns_to_drop = []


# final_df.drop(columns=columns_to_drop , inplace=True)

#  [markdown]
# df.to_xlsx("./data/main_data.xlsx")
# 
# new_df = pd.DataFrame(new_df_data)
# 
# new_df.to_xlsx("./data/new_data.xlsx)
# 


# df.to_excel("./data/main_data.xlsx")

# new_df = pd.DataFrame(new_df_data)

# new_df.to_excel("./data/new_data.xlsx")

df.to_excel("./data/final_data.xlsx",index=False)



