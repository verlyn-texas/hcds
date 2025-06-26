import json
import re
from . import formula_parser

# TODO: Raise errors when problems are found

def find_entity_by_id(entity, entity_id: str):
    """Find an entity by ID"""
   
    if entity['id'] == entity_id:
        return entity, None  # Return the entity and None as parent when found directly
    if 'entities' in entity:
        for child_entity in entity['entities']:
            result = find_entity_by_id(child_entity, entity_id)
            if result:
                return result[0], entity  # Return the found entity and its parent
    return None

def parse_name_string(s):
    """
    Parses a name string and returns a tuple based on the dot-prefix and name structure.
    """
    print(f"s: {s}")
    # Try "uncle" pattern first (most specific in terms of leading dots)
    match_uncle = re.match(r"^\.\.(\w+)$", s)
    if match_uncle:
        return ("uncle", match_uncle.group(1))

    # Try "nephew" pattern (two names)
    match_nephew = re.match(r"^\.(\w+)\.(\w+)$", s)
    if match_nephew:
        return ("nephew", match_nephew.group(1), match_nephew.group(2))

    # Try "sibling" pattern (single name)
    match_sibling = re.match(r"^\.(\w+)$", s)
    if match_sibling:
        return ("sibling", match_sibling.group(1))

    # If no pattern matches
    return None

def get_element_id(entity: dict, name1: str, name2: str, type: str, template: dict) -> str:
    """
    Returns an ID prepended with a
    '' if it is a single attribute
    '_' if it is an array of attributes
    'c_' if it is a single calculation
    '_c_' if it is an array of calculations
    """
    
    attribute_id = None
    
    if type == "uncle":
        parent_entity, grandparent_entity = find_entity_by_id(template['trunk'], entity["parent_id"])
        for attribute in parent_entity["attributes"]:
            if attribute["name"].lower().replace(" ", "_") == name1:
                attribute_id = attribute["id"]
        for calculation in parent_entity["calculations"]:
            if calculation["name"].lower().replace(" ", "_") == name1:
                attribute_id = "c_" + calculation["id"]
    elif type == "sibling":
        for attribute in entity["attributes"]:
            if attribute["name"].lower().replace(" ", "_") == name1:
                attribute_id = attribute["id"]
        for calculation in entity["calculations"]:
            if calculation["name"].lower().replace(" ", "_") == name1:
                attribute_id = "c_" + calculation["id"]
    elif type == "nephew":
        for entity in entity["entities"]:
            if entity["name"].lower().replace(" ", "_") == name1:
                for attribute in entity["attributes"]:
                    if attribute["name"].lower().replace(" ", "_") == name2:
                        attribute_id = "_" + attribute["id"]
                for calculation in entity["calculations"]:
                    if calculation["name"].lower().replace(" ", "_") == name2:
                        attribute_id = "_c_" + calculation["id"]
        
    return attribute_id

def process_list(parsed_formula: list, entity: dict, template: dict) -> list:
    # TODO: Add error notice when an element is not found.  Currently a null is returned.
    
    def decode_name(potential_id: str, entity: dict, template: dict) -> str:
        name_tuple = parse_name_string(potential_id)
        if name_tuple:
            if name_tuple[0] == "uncle":
                return get_element_id(entity, name_tuple[1], None, "uncle", template)
            elif name_tuple[0] == "sibling":
                return get_element_id(entity, name_tuple[1], None, "sibling", template)
            elif name_tuple[0] == "nephew":
                return get_element_id(entity, name_tuple[1], name_tuple[2], "nephew", template)
        return potential_id
    
    new_formula = []
    for part in parsed_formula:
        if type(part) == str:
            new_formula.append(decode_name(part, entity, template))
        elif type(part) == dict:
            new_dict = {list(part.keys())[0]: process_list(list(part.values())[0], entity, template)}
            new_formula.append(new_dict)
        elif type(part) == list:
            new_list = process_list(part, entity, template)
            new_formula.append(new_list)
    
    return new_formula

def process_entity_formulas(trunk: dict, template: dict) -> dict:
    for calculation in trunk["calculations"]:
        parsed_formula = formula_parser.parse_formula(calculation["formula"])
        new_formula = process_list(parsed_formula, trunk, template)
        calculation["formula_code"] = new_formula
    for entity in trunk["entities"]:
        new_entity = process_entity_formulas(entity, template)
        entity = new_entity

    return trunk

def process_templateformulas(template: dict) -> dict:
    trunk = template["trunk"]
    new_trunk = process_entity_formulas(trunk, template)
    template["trunk"] = new_trunk
    return template

