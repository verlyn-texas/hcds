from app.api.templates import Template_Definition_Full, get_calculation
from app.db.couchdb_client import CouchDBClient
import re

# Initialize CouchDB client
couchdb_client = CouchDBClient(url="http://localhost:5984", username="admin", password="admin", db_name="data_items")

#########################
# Functions
#########################

def _function_sum(items_list: list[str]) -> float:
    return sum(float(item) for item in items_list)

def _function_product(items_list: list[float]) -> float:
    product = 1
    for item in items_list:
        product = product * float(item)
    return product

def _function_difference(items_list: list[float]) -> float:
    return items_list[0] - items_list[1]

def _function_quotient(items_list: list[float]) -> float:
    return items_list[0] / items_list[1]

def _function_and(items_list: list[bool]) -> bool:
    return all(items_list)

def _function_or(items_list: list[bool]) -> bool:
    return any(items_list)

def _function_not(items_list: list[bool]) -> bool:
    return not items_list[0]

def _function_count(items_list: list[float]) -> float:
    return len(items_list)

def _function_concatenate(items_list: list[str]) -> str:
    return ''.join(items_list)

def _function_contains(items_list: list[str]) -> bool:
    return items_list[0] in items_list[1]

def _function_not_contains(items_list: list[str]) -> bool:
    return items_list[0] not in items_list[1]

def _function_greater_than(items_list: list[float]) -> bool:
    return items_list[0] > items_list[1]

def _function_less_than(items_list: list[float]) -> bool:
    return items_list[0] < items_list[1]

def _function_greater_than_or_equal_to(items_list: list[float]) -> bool:
    return items_list[0] >= items_list[1]

def _function_less_than_or_equal_to(items_list: list[float]) -> bool:
    return items_list[0] <= items_list[1]

def _function_equal_to(items_list: list[float]) -> bool:
    return items_list[0] == items_list[1]

def _function_not_equal_to(items_list: list[float]) -> bool:
    return items_list[0] != items_list[1]

def _function_if(items_list: list):
    return items_list[2] if items_list[0] else items_list[1]

def _function_lookup(items_list: list[float]) -> float:
    # TODO: Implement the lookup function.

    # Based on the data type of the source attribute, the “lookup” function will operate as such given a list of entities, a sought value, a source attribute, and a target attribute
    # Short Text, Long Text
    #    - Calculates the Damerau-Levenshtein (DL) distance between the source attribute value and the sought value.
    #    - Sorts the list of entities based on the DL distance from lowest to highest and, within that, by the source attribute values from lowest to highest.
    #    - Returns the target attribute of the first entity in the sorted list.
    # Rich Text, Role, Group, User
    #    - Not supported
    # Whole Number, Integer, Decimal, Percentage, DateTime, Time
    #    - Calculates absolute value between the source attribute value and the sought value.
    #    - Returns the target attribute of the entity with the lowest absolute value.
    #    - Randomly selects one of the entities if they share the same lowest absolute value.
    # Boolean, Categorical
    #    - Seeks an exact match between the source attribute value and the sought value.
    #    - If there is a single exact match, the system returns the target attribute of the matching entity.
    #    - If there is no exact match, the system returns “null”
    #    - If there is more than one match, the system selects one of the matching entities randomly and returns its target attribute.

    
    return items_list[0]

def _function_min(items_list: list[float]) -> float:
    return min(items_list)

def _function_max(items_list: list[float]) -> float:
    return max(items_list)

def _function_average(items_list: list[float]) -> float:
    return sum(items_list) / len(items_list)

def _function_identity(items_list):
    if type(items_list) == list:
        return items_list[0]
    else:
        return str(items_list)

#########################
# Calculation Engine
#########################

def calc(template: Template_Definition_Full,
         entity, computation_definition_id: str):
    
    # TODO: Cast all values to the correct data type.


    # Get the parent entity based on the parent_entity_id in the entity
    # Get formula_code from template based on computation_definition_id
    # Walk formula_code tree from top to bottom
    # For each function get the values of each function argument and pass to a function
    #    - For sibling
    #        - attributes: get the values from the entity attributes list
    #        - calculations: pass the calculation_definition_id to the calc function.  Use the same entity and template.
    #    - For uncle
    #        - attributes, get the value from the parent entity attributes list
    #        - calculations: pass the calculation_definition_id, the parent entity, and template to the calc function
    
    def get_values(function_list, entity, parent_entity):

        value_list = []
        
        def match_uuid(item, value_list, entity, parent_entity):
            uuid_with_prefix_regex = r"(.*?)([0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})"
            match = re.search(uuid_with_prefix_regex, item)
            print(f"pre value list: {value_list}")
            if match:
                prefix = match.group(1) # Group 1 is the prefix
                uuid = match.group(2)   # Group 2 is the UUID
                print(f"prefix: {prefix}")
                print(f"uuid: {uuid}")
                print(f"entity_id: {entity['_id']}")
                print(f"parent_entity_id: {parent_entity['_id']}")
                if prefix == '':
                    value = None
                    for attribute in parent_entity["attributes"]:
                        if attribute["attribute_definition_id"] == uuid:
                            value = str(attribute["value"])
                            value_list.append(value)
                            break
                    for attribute in entity["attributes"]:
                        if attribute["attribute_definition_id"] == uuid:
                            value = str(attribute["value"])
                            value_list.append(value)
                            break
                elif prefix == '_':
                    query = {
                        "selector": {
                            "parent_entity_id": entity["_id"]
                        }
                    }
                    children = couchdb_client.db.find(query)
                    for child_entity in children:
                        for attribute in child_entity["attributes"]:
                            if attribute["attribute_definition_id"] == uuid:
                                value = str(attribute["value"])
                                value_list.append(value)
                    print(f"post value list: {value_list}")
                elif prefix == 'c_':
                    value = calc(template, entity, uuid)
                    value_list.append(value)
                elif prefix == '_c_':
                    query = {
                        "selector": {
                            "parent_entity_id": entity["_id"]
                        }
                    }
                    children = couchdb_client.db.find(query)
                    for child_entity in children:
                        value = calc(template, child_entity, uuid)
                        value_list.append(value)
                else:
                    raise ValueError(f"Unknown prefix: {prefix}")
            else:
                value_list.append(item)
        
        # This is the entry point for the get_values function.
        # This function expects a list
        # When it finds a string, it is passed to match_uuid.
        # When it finds a dictionary, it is passed to process_function.
        # There should never be a list inside a list, so it should raise an error.
        
        for item in function_list:
            if type(item) == str:
                match_uuid(item, value_list, entity, parent_entity)
            elif type(item) == list:
                raise ValueError(f"Lists in a list are not allowed.")
            elif type(item) == dict:
                value = process_function(item, entity, parent_entity)
                value_list.append(value)
            else:
                raise ValueError(f"Unknown type: {type(item)}")
            
        return value_list

    def process_function(formula_code_portion, entity, parent_entity):
        # Find the function handle and function list values based on the formula_code_portion.
        # This function may be called recursively.

        if type(formula_code_portion) == str:
            # The string is placed into a list and passed to get_values
            function_handle = None
            function_list_values = get_values([formula_code_portion], entity, parent_entity)
        elif type(formula_code_portion) == list:
            # The formula_code is a list.  When that arrives, the first item is passed to process_function.
            # This path should not occur after the first pass.
            function_handle = None
            function_list_values = process_function(formula_code_portion[0], entity, parent_entity)
        elif type(formula_code_portion) == dict:
            # When a function is encountered, its function handle and function values are extracted.
            # A function dictionary should only have a single key and value.
            function_handle = list(formula_code_portion.keys())[0]
            function_list_values = get_values(list(formula_code_portion.values())[0], entity, parent_entity)
        else:
            raise ValueError(f"Unknown formula_code_portion type: {type(formula_code_portion)}")
        
        print(f"function_handle: {function_handle}")
        print(f"function_list: {function_list_values}")
        # print(f"function_list[0]: {function_list_values[0]}")

        # Perform the function based on the function handle and function list values.

        if function_handle == "SUM" or function_handle == "+":
            result = _function_sum(function_list_values)
        elif function_handle == "DIFFERENCE" or function_handle == "-":
            result = _function_difference(function_list_values)
        elif function_handle == "PRODUCT" or function_handle == "*":
            result = _function_product(function_list_values)
        elif function_handle == "MIN":
            result = _function_min(function_list_values)
        elif function_handle == "MAX":
            result = _function_max(function_list_values)
        elif function_handle == "MEAN":
            result = _function_average(function_list_values)
        elif function_handle == "IF":
            result = _function_if(function_list_values)
        elif function_handle == "LOOKUP":
            result = _function_lookup(function_list_values)
        elif function_handle == "QUOTIENT" or function_handle == "/":
            result = _function_quotient(function_list_values)
        elif function_handle == "AND":
            result = _function_and(function_list_values)
        elif function_handle == "OR":
            result = _function_or(function_list_values)
        elif function_handle == "NOT":
            result = _function_not(function_list_values)
        elif function_handle == "COUNT":
            result = _function_count(function_list_values)
        elif function_handle == "CONCATENATE":
            result = _function_concatenate(function_list_values)
        elif function_handle == "CONTAINS":
            result = _function_contains(function_list_values)
        elif function_handle == "NOT_CONTAINS":
            result = _function_not_contains(function_list_values)
        elif function_handle == ">":
            result = _function_greater_than(function_list_values)
        elif function_handle == "<":
            result = _function_less_than(function_list_values)
        elif function_handle == ">=":
            result = _function_greater_than_or_equal_to(function_list_values)
        elif function_handle == "<=":
            result = _function_less_than_or_equal_to(function_list_values) 
        elif function_handle == "=":
            result = _function_equal_to(function_list_values)
        elif function_handle == "!=":
            result = _function_not_equal_to(function_list_values)
        elif function_handle == "&&":
            result = _function_and(function_list_values)
        elif function_handle == "||":
            result = _function_or(function_list_values)
        elif function_handle == None:
            # This usually occurs when the formula_code_portion is a string or
            # when it is the first step through the formula_code.
            result = _function_identity(function_list_values)
        else:
            raise ValueError(f"Unknown function [A]: {function_handle}")
        
        print(f"result: {result}")
        return result

    # This is the entry point for the calculation.
    # Get the parent entity based on the parent_entity_id in the entity
    # Get formula_code from template based on computation_definition_id
    # Perform the calculation and return the result
    
    parent_entity_id = entity["parent_entity_id"]
    print(f"parent_entity_id: {parent_entity_id}")
    parent_entity = couchdb_client.get_document(parent_entity_id)
    calculation = get_calculation(template_id=template['_id'], calculation_id=computation_definition_id)
    formula_code = calculation['formula_code']

    final_result = process_function(formula_code, entity=entity, parent_entity=parent_entity)

    return final_result

example_formula_code = [
    {
    "SUM": [
        "_9461d5db-72ba-4b72-bbb5-02113deaa637"
    ]
    }
]