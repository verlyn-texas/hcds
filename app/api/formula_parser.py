import re

part_idx = 0

def _add_to_level(parent, part_list_length, part_list):
    global part_idx
    while part_idx < part_list_length:
        part = part_list[part_idx]
        if part == "(":
            part_idx += 1
            new_level = []
            parent.append(_add_to_level(new_level, part_list_length, part_list))
        elif part == ")":
            part_idx += 1
            return parent
        elif part == ",":
            part_idx += 1
        else:
            parent.append(part)
            part_idx += 1
    return parent

def _create_function_tuple(part_list):
    list_length = len(part_list)
    part_idx = 0
    while part_idx < list_length:
        if part_list[part_idx] in ["SUM", "DIFFERENCE", "PRODUCT", "MIN", "MAX", "MEAN", "IF", "LOOKUP", "QUOTIENT", "AND", "OR", "NOT", "COUNT", "CONCATENATE", "CONTAINS", "NOT_CONTAINS"]:
            function_name = part_list[part_idx]
            tuplized_part_list = _create_function_tuple(part_list[part_idx + 1])
            new_list = function_name, tuplized_part_list
            part_list[part_idx] = new_list
            part_list.pop(part_idx + 1)
            list_length -= 1
            part_idx += 1
        else:
            part_idx += 1
    return part_list

def _funtionalize_part(part_list):
    # Clean up the sub lists
    for part_idx in range(len(part_list)):
        if type(part_list[part_idx]) == tuple:
            part_list[part_idx] = part_list[part_idx][0], _funtionalize_part(part_list[part_idx][1])
    
    # Process Multiplications and Divisions
    completed = False
    part_idx = 1
    while not completed:
        list_length = len(part_list)
        if part_idx > 0 and part_idx < list_length:
            if part_list[part_idx] in ["*", "/"]:
                function_name = part_list[part_idx]
                new_list = function_name, [part_list[part_idx - 1], part_list[part_idx + 1]]
                part_list[part_idx - 1] = new_list
                part_list.pop(part_idx)
                part_list.pop(part_idx)
            else:
                part_idx += 1
        else:
            completed = True
    
    # Process Additions and Subtractions
    completed = False
    part_idx = 1
    while not completed:
        list_length = len(part_list)
        if part_idx > 0 and part_idx < list_length:
            if part_list[part_idx] in ["-", "+"]:
                function_name = part_list[part_idx]
                new_list = function_name, [part_list[part_idx - 1], part_list[part_idx + 1]]
                part_list[part_idx - 1] = new_list
                part_list.pop(part_idx)
                part_list.pop(part_idx)
            else:
                part_idx += 1
        else:
            completed = True
    
    # Process Comparisons
    completed = False
    part_idx = 1
    while not completed:
        list_length = len(part_list)
        if part_idx > 0 and part_idx < list_length:
            if part_list[part_idx] in [">", "<", ">=", "<=", "=", "!=", "&&", "||"]:
                function_name = part_list[part_idx]
                new_list = function_name, [part_list[part_idx - 1], part_list[part_idx + 1]]
                part_list[part_idx - 1] = new_list
                part_list.pop(part_idx)
                part_list.pop(part_idx)
            else:
                part_idx += 1
        else:
            completed = True
    
    return part_list

def _convert_tuples_to_dict(formula_list):
    # Traverses the elements in a list and coverts any tuples found into dictionaries
    # where the first element of the tuple is the key and the second element is the value.
    # It handles nested tuples.
    # It returns a list.
    # The following is an example of the input and output:
    # Input: [('IF', [('>=', ['A', '0.5']), 'B', ('*', ['C', ('SUM', ['D', 'E'])])])]
    # Output = [{'IF': [{'>=', ['A', '0.5']}, 'B', {'*', ['C', {'SUM', ['D', 'E']}]}]}]
    
    result = []
    for item in formula_list:
        if type(item) == tuple:
            # Convert tuple to dictionary: first element as key, second element as value
            converted_value = _convert_tuples_to_dict(item[1])
            result.append({item[0]: converted_value})
        else:
            # If it's not a tuple, add it as-is to the result list
            result.append(item)
    return result

def parse_formula(formula):
    
    # Define the delimiters, including two-character ones
    delimiters = r'(>=|<=|!=|==|&&|\|\||[,()*\/+-]|>|<|=)'

    # Use the delimiters pattern to split the formula
    parts = re.split(delimiters, formula)

    # Trim leading and trailing whitespace from each part
    parts = [part.strip() for part in parts]
    parts_without_empty = [part for part in parts if part]
    
    # Create a top level list
    # Add to list until a parenthesis is found
    # When a parenthesis is found, create a new list
    # Add to the new list until the closing parenthesis is found
    # Add the new list to the top level list
    # Return the top level list
    # Ignore commas
    parent = []
    part_list_length = len(parts_without_empty)
    global part_idx
    part_idx = 0
    top_level = _add_to_level(parent, part_list_length, parts_without_empty)

    # When a function is found, create a tuple of the function name and its argument list
    top_level = _create_function_tuple(top_level)

    # Combine functional sequences into a single function with arguments
    functional_top_level = _funtionalize_part(top_level)

    return _convert_tuples_to_dict(functional_top_level)

# formula_one = "IF(A >= 0.5, B, C * SUM(D, E))"
# formula_two = "IF(A || B, C, D)"
# formula_three = "2 * 3 * SUM(4 * 5, 6)"
# formula_four = "A * B * C * D"
# formula_five = "A * B + C * D"
# formula_six = "A"
# formula_seven = "1 * A"

# print(parse_formula(formula_one))
# print(parse_formula(formula_two))
# print(parse_formula(formula_three))
# print(parse_formula(formula_four))
# print(parse_formula(formula_five))
# print(parse_formula(formula_six))
# print(parse_formula(formula_seven))