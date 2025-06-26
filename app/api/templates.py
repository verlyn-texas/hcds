from fastapi import APIRouter, HTTPException, status, Response, Query, Path
from pydantic import BaseModel, Field
from typing import List, Optional
from app.db.couchdb_client import CouchDBClient
import uuid
import copy
import re
from datetime import datetime, UTC
from . import formula_resolution

router = APIRouter()

#########################################################
# Pydantic models
#########################################################

# RESPONSES

example_attribute_and_calculation_data_type_constraints = {
    "short_text": {
        "text_validation": "^[A-Z][a-zA-Z0-9 ]*$"
        },
    "long_text": {
        "text_validation": "^(\S+\s*){0,49}$"
        },
    "whole_number": {
        "min_value": 0,
        "max_value": 100,
        "display_thousands_separator": True,
        },
    "integer": {
        "min_value": -50,
        "max_value": 60,
        "display_thousands_separator": False,
        },
    "decimal": {
        "min_value": 0,
        "max_value": 200000,
        "precision": 3,
        "display_thousands_separator": True,
        "currency": "USD",
        },
    "percentage": {
        "min_value": 0,
        "max_value": 100,
        "precision": 2
        },
    "boolean": {
        "true_value": "Yes",
        "false_value": "No"
        },
    "categorical": {
        "values": ["Bronze", "Silver", "Gold"]
        },
    "datetime": {
        "display_date": True,
        "display_time": True,
        "display_date_format": "MM/DD/YYYY",
        "display_time_format": "HH:MM 24-hour",
        "display_timezone": "America/New_York"
        },
    "time": {
        "display_time_format": "HH:MM 24-hour",
        "display_timezone": "America/New_York"
        },
    "nullable": {
        "nullable": True
        }
}

example_formula = "IF(..policy_factor > 0.5, ..total_applicant_scores, .cost_factor * SUM(.building.replacement_cost, .building.twice_cost)"

class Attribute_Definition(BaseModel):
    """
    An attribute definition describes one type of information that is associated with an entity.
    If an entity definition is like a database table, then an attribute definition is like a
    column in the table.

    An attribute definition name must be unique within its set of siblings and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.

    An attribute definition has a data_type. For each data_type, there are constraints that
    define the possible values.
   
    Allowable choices for data_type are:
    - short_text
    - whole_number
    - integer
    - decimal
    - percentage
    - boolean
    - categorical
    - datetime
    - time
    - time_span
    """
    id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174000")
    parent_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    name: str = Field(..., example="Cost")
    description: str = Field(..., example="The cost of the item.")
    data_type: str = Field(..., example="decimal")
    data_type_constraints: dict = Field(..., example=example_attribute_and_calculation_data_type_constraints)
    defaultvalue: str = Field(..., example="4001.003")

class Calculation_Definition(BaseModel):
    """
    A calculation definition describes one type of information that is associated with an entity.
    If an entity definition is like a database table, then an calculation definition is like a
    column in the table.

    Instead of describing a value to be stored, a calculation describes a mathematical formula
    that is used to calculate a value.

    A calculation definition name must be unique within its set of siblings and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.

    Allowable choices for data_type are:
    - short_text
    - whole_number
    - integer
    - decimal
    - percentage
    - boolean
    - categorical
    - datetime
    - time
    - time_span

    The formula is a set of nested dictionaries that describes the calculation.
    Each dictionary has a single key that is the name of a function and a value that is a list of arguments.
    The arguments can be values (like numbers or strings) or references to other attributes or calculations.

    Formula arguments references are limited to the uncle, sibling, and nephew of the current calculation.
    Formulas are validated not to contain circular references.
    """
    id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174000")
    parent_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    name: str = Field(..., example="Total Cost")
    description: str = Field(..., example="The cost of all items in the order.")
    data_type: str = Field(..., example="decimal")
    formula: str = Field(..., example=example_formula)

class Entity_Definition(BaseModel):
    """
    An entity definition is a collection of attributes definitions, calculations definitions, and other entity definitions.
    An entity is like a table in a database.

    An entity definition name must be unique within its set of siblings and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.
    """
    id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174000")
    parent_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    name: str = Field(..., example="Employees")
    description: str = Field(..., example="Employees of the company.")
    attributes: List['Attribute_Definition'] = []
    entities: List['Entity_Definition'] = []
    calculations: List['Calculation_Definition'] = []

class Entity_Definition_Summary(BaseModel):
    """
    A summary of an entity definition.
    """
    name: str = Field(..., example="Employees")
    entities: List['Entity_Definition_Summary'] = []

class Entity_Definition_Tree(BaseModel):
    """
    A tree of entity definitions.
    """
    tempate_definition_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174000")
    entities: List[Entity_Definition_Summary] = []

class Template_Definition_Summary(BaseModel):
    """
    A template definition is like a database schema.  It contains a singular trunk entity definition.
    The trunk entity is the root of the entity hierarchy.  It is the entity that is used to
    create new entities.  It is also the entity that is used to create new attributes and calculations.

    An template definition name must be unique within the system and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.
    
    While there can be multiple templates definition, only one of them will be in a 'Published' status at any one time.
    Published templates are immutable.  They serve as the pattern for creating data entries into an associated
    dataset.  Draft templates can be modified but cannot be used to create new data entries.  Deprecated templates
    were used for creating data entries but are no longer in use.  They are also immutable.

    Allowable choices for status are:
    - Draft
    - Published
    - Deprecated

    The id field is the unique identifier of the template definition and is assigned by the system upon template
    definition creation.
    
    The source_id is the ID of the template from which the new template is copied.  If the template is
    not copied from another template, the source_id is None.

    For brevity, the trunk entity definition is not included in the template definition summary.
    """
    id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    name: str = Field(..., example="My Template")
    status: str = Field(..., example="Draft")
    published_date: Optional[datetime] = Field(None, example="2025-05-21T12:00:00")
    source_id: Optional[str] = Field(None, example="18fdad59d4703844e9bbcaffa902b12a")

class Template_Definition_Full(BaseModel):
    """
    A template definition is like a database schema.  It contains a singular trunk entity definition.
    The trunk entity is the root of the entity hierarchy.  It is the entity that is used to
    create new entities.  It is also the entity that is used to create new attributes and calculations.

    An template definition name must be unique within the system and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.
    
    While there can be multiple templates definition, only one of them will be in a 'Published' status at any one time.
    Published templates are immutable.  They serve as the pattern for creating data entries into an associated
    dataset.  Draft templates can be modified but cannot be used to create new data entries.  Deprecated templates
    were used for creating data entries but are no longer in use.  They are also immutable.

    Allowable choices for status are:
    - Draft
    - Published
    - Deprecated

    The id field is the unique identifier of the template definition and is assigned by the system upon template
    definition creation.

    The source_id is the ID of the template from which the new template is copied.  If the template will
    not be copied from another template, the source_id should be set to 'None'.
    """
    id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    name: str = Field(..., example="My Template")
    status: str = Field(..., example="Draft")
    published_date: Optional[datetime] = Field(None, example="2025-05-21T12:00:00")
    source_id: Optional[str] = Field(None, example="18fdad59d4703844e9bbcaffa902b12a")
    trunk: Entity_Definition

class Element_Definition(BaseModel):
    """
    An element definition is a means to reference an entity definition, attribute definition, or
    calculation definition.  Element definitions are not bonefide entity definitions, attribute definitions, or
    calculation definitions and are not stored in the template definition.

    The properties of an element definition are those that are common to entity definitions,
    attribute definitions, and calculation definitions.
    """
    id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174000")
    parent_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    name: str = Field(..., example="My Element")

class ErrorResponse(BaseModel):
    """
    An error response is a response to an API request that contains an error message.
    """
    detail: str = Field(..., description="Error message", example="This is an error message")
    status_code: int = Field(..., description="HTTP status code", example=400)

# REQUESTS

# - Read

class Entity_Definition_ID(BaseModel):
    """
    Use this pattern to read an entity definition by its ID.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    entity_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")

class Attribute_Definition_ID(BaseModel):
    """
    Use this pattern to read an attribute definition by its ID.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    attribute_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")

class Calculation_Definition_ID(BaseModel):
    """
    Use this pattern to read a calculation definition by its ID.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    calculation_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")

class Template_Definition_ID(BaseModel):
    """
    Use this pattern to read a template definition by its ID.
    """
    id: str = Field(..., description="The unique identifier of the template", example="18fdad59d4703844e9bbcaffa9020370")

class Entity_Definition_Tree_ID(BaseModel):
    """
    Use this pattern to read an entity definition tree by its ID.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")


# - Create

class Attribute_Definition_Create(BaseModel):
    """
    Use this pattern to create an attribute definition.

    An attribute definition describes one type of information that is associated with an entity.
    If an entity definition is like a database table, then an attribute definition is like a
    column in the table.

    An attribute definition name must be unique within its set of siblings and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.

    An attribute definition has a data_type. For each data_type, there are constraints that
    define the possible values.
   
    Allowable choices for data_type are:
    - short_text
    - whole_number
    - integer
    - decimal
    - percentage
    - boolean
    - categorical
    - datetime
    - time
    - time_span
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    parent_entity_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    name: str = Field(..., example="Employee Last Name")
    description: str = Field(..., example="The surname of the employee.")
    data_type: str = Field(..., example="short_text")
    data_type_constraints: dict = Field(..., example=example_attribute_and_calculation_data_type_constraints)
    defaultvalue: str = Field(..., example="Doe")

class Calculation_Definition_Create(BaseModel):
    """
    Use this pattern to create a calculation definition.

    A calculation definition describes one type of information that is associated with an entity.
    If an entity definition is like a database table, then an calculation definition is like a
    column in the table.

    Instead of describing a value to be stored, a calculation describes a mathematical formula
    that is used to calculate a value.

    A calculation definition name must be unique within its set of siblings and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.

    Allowable choices for data_type are:
    - short_text
    - whole_number
    - integer
    - decimal
    - percentage
    - boolean
    - categorical
    - datetime
    - time
    - time_span

    The formula is a set of nested dictionaries that describes the calculation.
    Each dictionary has a single key that is the name of a function and a value that is a list of arguments.
    The arguments can be values (like numbers or strings) or references to other attributes or calculations.

    Formula arguments references are limited to the uncle, sibling, and nephew of the current calculation.
    Formulas are validated not to contain circular references.    
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    parent_entity_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    name: str = Field(..., example="My Calculation")
    description: str = Field(..., example="This is my calculation")
    data_type: str = Field(..., example="short_text")
    formula: str = Field(..., example=example_formula)
    # formula_code: Optional[list] = Field(None)

class Entity_Definition_Create(BaseModel):
    """
    Use this pattern to create an entity definition.

    An entity definition is a collection of attributes definitions, calculations definitions, and other entity definitions.
    An entity is like a table in a database.

    An entity definition name must be unique within its set of siblings and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    parent_entity_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    name: str = Field(..., example="My Entity")
    description: str = Field(..., example="This is my entity")

class Template_Definition_Create(BaseModel):
    """
    Use this pattern to create a template definition.

    A template definition is like a database schema.  It contains a singular trunk entity definition.
    The trunk entity is the root of the entity hierarchy.  It is the entity that is used to
    create new entities.  It is also the entity that is used to create new attributes and calculations.

    An template definition name must be unique within the system and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.
    
    While there can be multiple templates definition, only one of them will be in a 'Published' status at any one time.
    Published templates are immutable.  They serve as the pattern for creating data entries into an associated
    dataset.  Draft templates can be modified but cannot be used to create new data entries.  Deprecated templates
    were used for creating data entries but are no longer in use.  They are also immutable.

    Allowable choices for status are:
    - Draft
    - Published
    - Deprecated

    The id field is the unique identifier of the template definition and is assigned by the system upon template
    definition creation.

    The source_id is the ID of the template from which the new template is copied.  If the template is
    not copied from another template, the source_id is None.    
    """
    name: str = Field(..., example="My New Template", description="The name of the new template")
    source_id: Optional[str] = Field(default="None", example="18fdad59d4703844e9bbcaffa9020370", description="The source template ID from which the new template is copied.  If there is no source, the field should be 'None'")

# - Update

class Template_Definition_Update(BaseModel):
    """
    Use this pattern to update a template definition.

    A template definition is like a database schema.  It contains a singular trunk entity definition.
    The trunk entity is the root of the entity hierarchy.  It is the entity that is used to
    create new entities.  It is also the entity that is used to create new attributes and calculations.

    An template definition name must be unique within the system and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.

    The templateid field is the unique identifier of the template definition and is assigned by the system upon template
    definition creation.    
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    name: Optional[str] = Field(None, example="My Updated Template")

class Attribute_Definition_Update(BaseModel):
    """
    Use this pattern to update an attribute definition.

    An attribute definition describes one type of information that is associated with an entity.
    If an entity definition is like a database table, then an attribute definition is like a
    column in the table.

    An attribute definition name must be unique within its set of siblings and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.

    An attribute definition has a data_type. For each data_type, there are constraints that
    define the possible values.
   
    Allowable choices for data_type are:
    - short_text
    - whole_number
    - integer
    - decimal
    - percentage
    - boolean
    - categorical
    - datetime
    - time
    - time_span
    """

    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    attribute_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    name: Optional[str] = Field(None, example="My Updated Attribute")
    description: Optional[str] = Field(None, example="This is my updated attribute")
    data_type: Optional[str] = Field(None, example="short_text")
    data_type_constraints: Optional[dict] = Field(None, example=example_attribute_and_calculation_data_type_constraints)
    defaultvalue: Optional[str] = Field(None, example="My Updated Default Value")

class Calculation_Definition_Update(BaseModel):
    """
    Use this pattern to update a calculation definition.

    A calculation definition describes one type of information that is associated with an entity.
    If an entity definition is like a database table, then an calculation definition is like a
    column in the table.

    Instead of describing a value to be stored, a calculation describes a mathematical formula
    that is used to calculate a value.

    A calculation definition name must be unique within its set of siblings and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.

    Allowable choices for data_type are:
    - short_text
    - whole_number
    - integer
    - decimal
    - percentage
    - boolean
    - categorical
    - datetime
    - time
    - time_span

    The formula is a set of nested dictionaries that describes the calculation.
    Each dictionary has a single key that is the name of a function and a value that is a list of arguments.
    The arguments can be values (like numbers or strings) or references to other attributes or calculations.

    Formula arguments references are limited to the uncle, sibling, and nephew of the current calculation.
    Formulas are validated not to contain circular references.  
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    calculation_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    parent_entity_id: Optional[str] = Field(None, example="123e4567-e89b-12d3-a456-426614174002")
    name: Optional[str] = Field(None, example="My Updated Calculation")
    description: Optional[str] = Field(None, example="This is my updated calculation")
    data_type: Optional[str] = Field(None, example="short_text")
    formula: Optional[str] = Field(None, example=example_formula)

class Entity_Definition_Update(BaseModel):
    """
    Use this pattern to update an entity definition.

    An entity definition is a collection of attributes definitions, calculations definitions, and other entity definitions.
    An entity is like a table in a database.

    An entity definition name must be unique within its set of siblings and the name must contain only numbers,
    letters, and spaces.  Consecutive spaces are not allowed.  The name must start with a letter and must
    be between 4 and 25 characters inclusive.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    entity_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    name: Optional[str] = Field(None, example="My Updated Entity Name")
    description: Optional[str] = Field(None, example="This is my updated entity description")

# - Delete

class Element_Definition_Delete(BaseModel):
    """
    Use this pattern to delete an attribute definition, calculation definition, or entity definition by ID.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    element_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")

# - Publish

class Template_Definition_Publish(BaseModel):
    """
    Use this pattern to publish a template definition.

    Publishing a template definition makes it immutable and sets any published template definition to deprecated
    status.  Templates definitions in draft status are not affected.  When a template is published, it can be used
    as a pattern for storing data in a data set.  It will no longer be possible to store data using a deprecated
    template definition.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")

# - Copy

class Attribute_Definition_Copy(BaseModel):
    """
    Use this pattern to copy an attribute definition from one template to another.
    """
    source_template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    source_attribute_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    target_template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa902b12a")
    target_entity_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174003")

class Entity_Definition_Copy(BaseModel):
    """
    Use this pattern to copy an entity definition from one template to another.
    """
    source_template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    source_entity_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174001")
    target_template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa902b12a")
    target_entity_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174003")

# Initialize CouchDB client
couchdb_client = CouchDBClient(url="http://localhost:5984", username="admin", password="admin", db_name="structure")
couchdb_dataset_client = CouchDBClient(url="http://localhost:5984", username="admin", password="admin", db_name="data_items")

#########################################################
# Helper Functions
#########################################################

def find_entity_by_id(entity: Entity_Definition, entity_id: str) -> Optional['Entity_Definition']:
    """Find an entity by ID"""
   
    if entity['id'] == entity_id:
        return entity, None  # Return the entity and None as parent when found directly
    if 'entities' in entity:
        for child_entity in entity['entities']:
            result = find_entity_by_id(child_entity, entity_id)
            if result:
                if result[1] is not None:
                    return result
                else:
                    return result[0], entity  # Return the found entity and its parent
    return None

def set_new_id(trunk: Entity_Definition) -> None:
    """Set the id field for all entities in the trunk"""
    trunk['id'] = str(uuid.uuid4())
    if 'attributes' in trunk:
        for attribute in trunk['attributes']:
            attribute['id'] = str(uuid.uuid4())
    if 'calculations' in trunk:
        for calculation in trunk['calculations']:
            calculation['id'] = str(uuid.uuid4())
    if 'entities' in trunk:
        for entity in trunk['entities']:
            set_new_id(entity)

def update_parent_id(trunk: Entity_Definition) -> None:
    """Update the parent_id of all entities, attributes, and calculations"""
    if 'entities' in trunk:
        for entity in trunk['entities']:
            entity['parent_id'] = trunk['id']
            update_parent_id(entity)
    if 'attributes' in trunk:
        for attribute in trunk['attributes']:   
            attribute['parent_id'] = trunk['id']
    if 'calculations' in trunk:
        for calculation in trunk['calculations']:
            calculation['parent_id'] = trunk['id'] 

def find_attribute_by_id(trunk: Entity_Definition, attribute_id: str) -> Optional['Attribute_Definition']:
    """Find an attribute by ID"""
    if 'attributes' in trunk:
        for attribute in trunk['attributes']:
            if attribute['id'] == attribute_id:
                return attribute, trunk
    if 'entities' in trunk:
        for entity in trunk['entities']:
            result = find_attribute_by_id(entity, attribute_id)
            if result:
                attribute, _ = result
                return attribute, entity  
    return None
    
def find_calculation_by_id(trunk: Entity_Definition, calculation_id: str) -> Optional['Calculation_Definition']:
    """Find a calculation by ID"""
    if 'calculations' in trunk:
        for calculation in trunk['calculations']:
            if calculation['id'] == calculation_id:
                return calculation, trunk
    if 'entities' in trunk:
        for entity in trunk['entities']:
            result = find_calculation_by_id(entity, calculation_id)
            if result:
                calculation, _ = result
                return calculation, entity
    return None 

def delete_element_by_id(trunk: Entity_Definition, element_id: str) -> Optional[dict]:
    """Delete an element by ID and return the deleted element"""
    element = None
    if 'attributes' in trunk:
        for attribute in trunk['attributes']:
            if attribute['id'] == element_id:
                element = attribute
                trunk['attributes'].remove(attribute)
                return element
    if 'calculations' in trunk:
        for calculation in trunk['calculations']:
            if calculation['id'] == element_id:
                element = calculation
                trunk['calculations'].remove(calculation)
                return element
    if 'entities' in trunk:
        for entity in trunk['entities']:
            if entity['id'] == element_id:
                element = entity
                if element['name'] == 'Trunk':
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete trunk")
                else:
                    trunk['entities'].remove(entity)
                    return element
            else:
                result = delete_element_by_id(entity, element_id)
                if result:
                    return result
    return None

def check_unique_name(parent_entity: Entity_Definition, name: str) -> bool:
    """Check if a name is unique within siblings of a parent entity"""
    if 'attributes' in parent_entity:
        for attribute in parent_entity['attributes']:
            if attribute['name'] == name:
                return False
    if 'entities' in parent_entity:
        for entity in parent_entity['entities']:
            if entity['name'] == name:
                return False    
    if 'calculations' in parent_entity:
        for calculation in parent_entity['calculations']:
            if calculation['name'] == name:
                return False
    return True

def check_unique_template_name(name: str) -> bool:
    """Check if a name is unique within the database"""
    query = {
        "selector": {
            "name": name
        }
    }
    results = list(couchdb_client.db.find(query))  # Convert map to list
    return len(results) == 0

def check_name_format(name: str) -> bool:
    """Check if a name is in the correct format.
    - Must start with a letter
    - Must be between 4 and 25 characters
    - Must contain only letters, numbers, and spaces
    - Must not contain consecutive spaces
    """
    return re.match(r"^[a-zA-Z](?:[a-zA-Z0-9]+(?: [a-zA-Z0-9]+)*){0,24}$", name) is not None

def find_elements_by_name(trunk: Entity_Definition, name: str) -> List[Element_Definition]:
    """
    Find elements by name in a template.
    """
    elements = []
    if trunk['name'] == name:
        elements.append(trunk)
    for attribute in trunk['attributes']:
        if attribute['name'] == name:
            elements.append(attribute)
    for calculation in trunk['calculations']:
        if calculation['name'] == name:
            elements.append(calculation)
    for entity in trunk['entities']:
        elements.extend(find_elements_by_name(entity, name))
    return elements

def check_formula_code(trunk: Entity_Definition, calculation_id: str) -> bool:
    """
    Performs the following checks of a calculation's formula code
    in context of a proposed template:
    - All attribute and calculation IDs are either:
        - Uncles
        - Siblings
        - Nephews
    - Functions are supported
    - No circular references
    """

    reason = ""
    result = find_calculation_by_id(trunk, calculation_id)
    if not result:
        reason = "Calculation not found."
        return False, reason
    
    calculation, _ = result
    
    # Check that functions are supported
    def check_functions(formula_code):
        global reason
        if not isinstance(formula_code, list):
            reason = "Formula code is not a list."
            return False
        else:
            for item in formula_code:
                if type(item) == tuple:
                    if item[0] not in ["SUM", "DIFFERENCE", "PRODUCT", "MIN", "MAX", "MEAN", "IF", "LOOKUP", "QUOTIENT", "AND", "OR", "NOT", "COUNT", "CONCATENATE", "*", "+", "-", "/", ">", "<", "=", "!=", ">=", "<=", "&&", "||"]:
                        reason = "Unsupported function."
                        return False
                    if not check_functions(item[1]):
                        reason = "Unsupported function."
                        return False
        return True
    
    if not check_functions(calculation['formula_code']):
        return False
    
    # Checks all attribute and calculation IDs are either uncles, siblings, nephews
    # 1. Gets the scope of the calculation
    def get_scope(calculation, trunk):
        scope = []
        calcs_only = []
        result = find_entity_by_id(trunk, calculation['parent_id'])
 
        parent, _ = result
        grandparent_id = parent['parent_id']
        if grandparent_id != 'None':
            result = find_entity_by_id(trunk, grandparent_id)
            grandparent, _ = result
            # Add uncle attributes and calculations
            for uncle_attribute in grandparent['attributes']:
                scope.append(uncle_attribute['id'])
            for uncle_calculation in grandparent['calculations']:
                scope.append(uncle_calculation['id'])
                calcs_only.append(uncle_calculation['id'])
        # Add sibling attributes and calculations
        for sibling_attribute in parent['attributes']:
            scope.append(sibling_attribute['id'])
        for sibling_calculation in parent['calculations']:
            if sibling_calculation['id'] != calculation_id:
                scope.append(sibling_calculation['id'])
                calcs_only.append(sibling_calculation['id'])
        # Add nephew attributes and calculations
        for sibling in parent['entities']:
            for nephew_attribute in sibling['attributes']:
                scope.append(nephew_attribute['id'])
            for nephew_calculation in sibling['calculations']:
                scope.append(nephew_calculation['id'])
                calcs_only.append(nephew_calculation['id'])

        return scope, calcs_only      
    
    scope, calcs_only = get_scope(calculation, trunk)

    # 2. Gets the attribute and calculation IDs in the calculation
    regex = r"^[a-z_]{0,2}([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$"

    def get_ids(formula_code):
        ids = []
        if not isinstance(formula_code, list):
            return []
        else:
            for item in formula_code:
                if type(item) == tuple:
                    ids.extend(get_ids(item[1]))
                elif isinstance(item, str):  # Only process string items
                    match = re.match(regex, item)
                    if match:
                        ids.append(match.group(1))
        return ids

    ids = get_ids(calculation['formula_code'])

    # 3. Checks that the IDs are in the scope
    for id in ids:
        if id not in scope:
            reason = "ID not in scope."
            return False
    
    # 4. Check for circular references
    # 1. Build the (function, input) tuples from the template formula codes
    def process_formula_code_by_entity(trunk):
        function_inputs = []
        for calculation in trunk['calculations']:
            ids = get_ids(calculation['formula_code'])
            for id in ids:
                function_inputs.append((calculation['id'], id))
        for entity in trunk['entities']:
            function_inputs.extend(process_formula_code_by_entity(entity))
        return function_inputs
    
    function_inputs = process_formula_code_by_entity(trunk)

    # 2. Check for circular references using a graph traversal algorithm
    def check_circularities(function_inputs):
        graph = {}
        for func, inp in function_inputs:
            graph.setdefault(func, []).append(inp)

        visited = set()
        recursion_stack = set()

        def is_cyclic(node):
            visited.add(node)
            recursion_stack.add(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if is_cyclic(neighbor):
                        return True
                elif neighbor in recursion_stack:
                    return True

            recursion_stack.remove(node)
            return False

        for node in graph:
            if node not in visited:
                if is_cyclic(node):
                    return True

        return False
    
    if check_circularities(function_inputs):
        reason = "Circular reference."
        return False, reason
    
    return True, reason


#########################################################
# Routes
#########################################################

# - Read

@router.get("/templates_def/summary", 
    tags=["Templates"],
    response_model=List[Template_Definition_Summary],
    summary="List template summaries",
    description="Get a list of all templates with their summary information",
    responses={
        200: {
            "description": "List of template summaries",
            "content": {
                "application/json": {
                    "example": [{
                        "id": "18fdad59d4703844e9bbcaffa9020370",
                        "name": "My Template Name",
                        "status": "Draft",
                        "source_id": "18fdad59d4703844e9bbcaffa902b12a"
                    }]
                }
            }
        }
    }
)
def list_templates_summary(
    status: Optional[str] = Query(None, description="Filter templates by status"),
    name: Optional[str] = Query(None, description="Filter templates by name")
) -> List[Template_Definition_Summary]:
    """
    Get a list of all templates with their summary information.
    
    Args:
        status: Optional filter for template status (Draft, Published, Deprecated)
        name: Optional filter for template name (case-insensitive partial match)
        
    Returns:
        List[Template_Definition_Summary]: List of template summaries matching the filters
    """
    # Build the Mango query
    query = {
        "selector": {
            "_id": {"$gt": None}  # Match all documents
        }
    }
    
    # Add filters to selector
    if status:
        query["selector"]["status"] = status
    if name:
        query["selector"]["name"] = {"$regex": f"(?i){name}"}  # Case-insensitive regex match
    
    # Execute the query
    results = couchdb_client.db.find(query)
    
    # Process results
    templates = []
    for doc in results:
        doc['id'] = doc['_id']  # Map CouchDB's _id to our id field
        templates.append(doc)
    
    return templates

@router.get("/templates_def/full",
    tags=["Templates"],
    response_model=List[Template_Definition_Full],
    summary="List template full definitions",
    description="Get a list of all templates with their full definitions",
    responses={
        200: {
            "description": "List of template full definitions",
            "content": {
                "application/json": {
                    "example": [{
                        "id": "18fdad59d4703844e9bbcaffa9020370",
                        "name": "My Template",
                        "status": "Draft",
                        "source_id": None,
                        "trunk": {
                            "id": "18fdad59d4703844e9bbcaffa9020370",
                            "name": "Trunk",
                            "description": "Root entity of the template",
                            "attributes": [],
                            "entities": [],
                            "calculations": []
                        }
                    }]
                }
            }
        }
    }
)
def list_templates_full(
    status: Optional[str] = Query(None, description="Filter templates by status"),
    name: Optional[str] = Query(None, description="Filter templates by name")
) -> List[Template_Definition_Full]:
    """
    Get a list of all templates with their full definitions.
    
    Args:
        status: Optional filter for template status (Draft, Published, Deprecated)
        name: Optional filter for template name (case-insensitive partial match)
        
    Returns:
        List[Template_Definition_Full]: List of templates matching the filters
    """
    # Build the Mango query
    query = {
        "selector": {
            "_id": {"$gt": None}  # Match all documents
        }
    }
    
    # Add filters to selector
    if status:
        query["selector"]["status"] = status
    if name:
        query["selector"]["name"] = {"$regex": f"(?i){name}"}  # Case-insensitive regex match
    
    # Execute the query
    results = couchdb_client.db.find(query)
    
    # Process results
    templates = []
    for doc in results:
        doc['id'] = doc['_id']  # Map CouchDB's _id to our id field
        templates.append(doc)
    
    return templates

@router.get("/templates_def/{template_id}",
    tags=["Templates"],
    response_model=Template_Definition_Full,
    summary="Get template by ID",
    description="Get a template by its ID",
    responses={
        200: {
            "description": "The full template definition",
            "content": {
                "application/json": {
                    "example": {
                        "id": "18fdad59d4703844e9bbcaffa9020370",
                        "name": "My Template",
                        "status": "Draft",
                        "source_id": "18fdad59d4703844e9bbcaffa902b12a",
                        "trunk": {
                            "id": "123e4567-e89b-12d3-a456-426614174001",
                            "name": "Trunk",
                            "description": "Root entity of the template",
                            "attributes": [],
                            "entities": [],
                            "calculations": []
                        }
                    }
                }
            }
        }
    }
)
def get_template(
    template_id: str,
    response: Response) -> Template_Definition_Full:
    """
    Get a template by ID.
    
    Args:
        template_id: The unique identifier of the template
        
    Returns:
        Template_Definition_Full: The full template definition
        
    Raises:
        HTTPException: If template is not found
    """
    template = couchdb_client.get_document(template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    template['id'] = template_id
    return template

@router.get("/entities_def/{template_id}/{entity_id}", 
    tags=["Elements"],
    response_model=Entity_Definition,
    summary="Get entity by ID",
    description="Get an entity by its template ID and entity ID",
    responses={
        200: {
            "description": "The entity definition",
            "content": {
                "application/json": {
                    "example": {
                        "id": "123e4567-e89b-12d3-a456-426614174000",
                        "name": "My Entity",
                        "description": "This is my entity",
                        "attributes": [],
                        "entities": [],
                        "calculations": []
                    }
                }
            }
        }
    }
)
def get_entity(
    template_id: str = Path(..., description="The template ID"),
    entity_id: str = Path(..., description="The entity ID")
):
    template = couchdb_client.get_document(template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    
    result = find_entity_by_id(template['trunk'], entity_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity not found")
    
    entity, _ = result
    return entity

@router.get("/attributes_def/{template_id}/{attribute_id}",
    tags=["Elements"],
    response_model=Attribute_Definition,
    summary="Get attribute by ID",
    description="Get an attribute by its template ID and attribute ID",
    responses={
        200: {
            "description": "The attribute definition",
            "content": {
                "application/json": {
                    "example": {
                        "id": "123e4567-e89b-12d3-a456-426614174000",
                        "name": "My Attribute",
                        "description": "This is my attribute",
                        "data_type": "short_text",
                        "data_type_constraints": "max_length: 100",
                        "defaultvalue": "My Default Value"
                    }
                }
            }
        }
    }
)
def get_attribute(template_id: str, attribute_id: str):
    template = couchdb_client.get_document(template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    result = find_attribute_by_id(template['trunk'], attribute_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attribute not found")
    
    attribute, _ = result
    return attribute

@router.get("/calculations_def/{template_id}/{calculation_id}",
    tags=["Elements"],
    response_model=Calculation_Definition,
    summary="Get calculation by ID",
    description="Get a calculation by its template ID and calculation ID",
    responses={
        200: {
            "description": "The calculation definition",
            "content": {
                "application/json": {
                    "example": {
                        "id": "123e4567-e89b-12d3-a456-426614174000",
                        "name": "My Calculation",
                        "description": "This is my calculation",
                        "data_type": "short_text",
                        "formula": "1 + 2"
                    }
                }
            }
        }
    }
)
def get_calculation(template_id: str, calculation_id: str):
    template = couchdb_client.get_document(template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    result = find_calculation_by_id(template['trunk'], calculation_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Calculation not found")
    
    calculation, _ = result
    return calculation

@router.get("/calculations_def/scope/{template_id}/{calculation_id}",
    tags=["Elements"],
    response_model=List[str],
    summary="Get the scope of a calculation",
    description="Get the scope of a calculation by its template ID and calculation ID",
    responses={
        200: {
            "description": "The scope of the calculation",
            "content": {
                "application/json": {
                    "example": ["123e4567-e89b-12d3-a456-426614174000", "123e4567-e89b-12d3-a456-426614174001"]
                }
            }
        }   
    }
)
def get_calculation_scope(template_id: str, calculation_id: str):
    scope, _ = get_attributes_and_calculations_in_scope(template_id, calculation_id)
    return scope

@router.get("/elements/{template_id}/{name}",
    tags=["Elements"],
    response_model=List[Element_Definition],
    summary="Get elements by name",
    description="Get elements by name",
)
def get_elements_by_name(template_id: str, name: str):
    template = couchdb_client.get_document(template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    elements = find_elements_by_name(template['trunk'], name)
    return elements

@router.get("/entity_definition_tree/{template_id}",
    tags=["Templates"],
    response_model=Entity_Definition_Tree,
    summary="Get the entity definition tree",
    description="Get the entity definition tree",
)
def get_entity_definition_tree(template_id: str):
    template = couchdb_client.get_document(template_id) 
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")

    def build_tree(entity_definition: dict) -> Entity_Definition_Summary:
        return Entity_Definition_Summary(
            name=entity_definition["name"],
            entities=[build_tree(entity) for entity in entity_definition.get("entities", [])]
        )
    
    entity_definition_tree = Entity_Definition_Tree(
        tempate_definition_id=template_id,
        entities=[build_tree(template['trunk'])]
    )

    return entity_definition_tree

# - Create

@router.post("/templates_def", 
    tags=["Templates"],
    response_model=Template_Definition_Full, 
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Template created successfully"},
        404: {"description": "Source template not found"},
    }
)
def create_template(template_request: Template_Definition_Create):
    if template_request.source_id == 'None' or template_request.source_id == None or template_request.source_id == '':
        trunk = {
            "id": str(uuid.uuid4()),
            "parent_id": 'None',
            "name": 'Trunk',
            "description": 'Root entity of the template',
            "attributes": [],
            "entities": [],
            "calculations": []
        }
        new_template = {
            "name": template_request.name,
            "status": "Draft",
            "source_id": 'None',
            "trunk": trunk
        }
    else:
        original_template = couchdb_client.get_document(template_request.source_id)
        if not original_template:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source template not found")
        
        # Create a new template object with copied content
        new_template = {
            "name": template_request.name,
            "status": "Draft",
            "source_id": template_request.source_id,
            "trunk": copy.deepcopy(original_template['trunk'])
        }
        # Generate new IDs for all entities, attributes, and calculations
        set_new_id(new_template['trunk'])
        update_parent_id(new_template['trunk'])
    
    # Check if the name is in the correct format
    if not check_name_format(new_template['name']):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name must be in the correct format")
    
    # Check if the name is unique
    if not check_unique_template_name(new_template['name']):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name must be unique")

    # Create the new template in the database
    template_id = couchdb_client.create_document(new_template)
    new_template["id"] = template_id
    return new_template

@router.post("/entities_def",
    tags=["Elements"],
    response_model=Entity_Definition,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Entity created successfully"},
        404: {"description": "Template or Entity not found"},
        400: {"description": "Cannot modify non-draft template or reserved entity name used or name in wrong format"}
    }
)
def add_entity(entity_request: Entity_Definition_Create):

    if entity_request.name == 'Trunk' or entity_request.name == 'trunk':
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="'Trunk' and 'trunk' are reserved entity names that cannot be used for a new entity")
    
    template = couchdb_client.get_document(entity_request.template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    if template["status"] != "Draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot modify non-draft template")
    
    # Find the parent entity within the template
    result = find_entity_by_id(template['trunk'], entity_request.parent_entity_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Entity not found')
    
    target_parent_entity, _ = result
    
    # Check if the name is unique within the parent entity
    if not check_unique_name(target_parent_entity, entity_request.name):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name must be unique within the parent entity")
    
    # Create new entity with a UUID
    new_entity = {
        "id": str(uuid.uuid4()),
        "parent_id": entity_request.parent_entity_id,
        "name": entity_request.name,
        "description": entity_request.description,
        "attributes": [],
        "entities": [],
        "calculations": []
    }
    
    # Add the new entity to the source entity
    target_parent_entity['entities'].append(new_entity)
    
    # Update the template in the database
    success = couchdb_client.update_document(entity_request.template_id, template)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update template")
    
    # Return the new entity
    return new_entity

@router.post("/attributes_def",
    tags=["Elements"],
    response_model=Attribute_Definition,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Attribute created successfully"},
        404: {"description": "Template or entity not found"},
        400: {"description": "Cannot modify non-draft template or reserved attribute name used or name in wrong format"}
    }
)
def add_attribute_to_entity(attribute_request: Attribute_Definition_Create):
    template = couchdb_client.get_document(attribute_request.template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    if template["status"] != "Draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot modify non-draft template")
    
    # Find the parent entity within the template
    result = find_entity_by_id(template['trunk'], attribute_request.parent_entity_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Entity not found')
    
    target_parent_entity, _ = result
    
    # Check if the name is unique within the parent entity
    if not check_unique_name(target_parent_entity, attribute_request.name):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name must be unique within the parent entity")
    
    # Create new attribute with a UUID
    new_attribute = {
        "id": str(uuid.uuid4()),
        "parent_id": attribute_request.parent_entity_id,
        "name": attribute_request.name,
        "description": attribute_request.description,
        "data_type": attribute_request.data_type,
        "data_type_constraints": attribute_request.data_type_constraints,
        "defaultvalue": attribute_request.defaultvalue
    }
    
    # Add the attribute to the entity's attributes list
    if 'attributes' not in target_parent_entity:
        target_parent_entity['attributes'] = []
    target_parent_entity['attributes'].append(new_attribute)
    
    # Update the template in the database
    success = couchdb_client.update_document(attribute_request.template_id, template)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update template")
        
    # Return the new attribute
    return new_attribute

@router.post("/calculations_def",
    tags=["Elements"],
    response_model=Calculation_Definition,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Calculation created successfully"},
        404: {"description": "Template or entity not found"},
        400: {"description": "Cannot modify non-draft template or reserved calculation name used or name in wrong format or formula incorrect"}
    }
)
def add_calculation_to_entity(calculation_request: Calculation_Definition_Create):
    template = couchdb_client.get_document(calculation_request.template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    if template["status"] != "Draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot modify non-draft template")

    # Find the source entity within the template
    trunk = template['trunk']
    result = find_entity_by_id(template['trunk'], calculation_request.parent_entity_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Entity not found')
    
    target_parent_entity, _ = result
    
    # Check if the name is unique within the parent entity
    if not check_unique_name(target_parent_entity, calculation_request.name):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name must be unique within the parent entity")
    
    # Create new calculation with a UUID
    new_calculation_id = str(uuid.uuid4())
    new_calculation = {
        "id": str(new_calculation_id),
        "parent_id": calculation_request.parent_entity_id,
        "name": calculation_request.name,
        "description": calculation_request.description,
        "data_type": calculation_request.data_type,
        "formula": calculation_request.formula,
        "formula_code": []
    }
    
    # Add the calculation to the entity's calculations list if it doesn't exist
    target_parent_entity['calculations'].append(new_calculation)

    # Process the template formulas
    formula_resolution.process_templateformulas(template)

    # Check if the formula is valid
    is_valid, reason = check_formula_code(trunk, new_calculation_id)
    if not is_valid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=reason)

    # Update the template in the database
    success = couchdb_client.update_document(calculation_request.template_id, template)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update template")
    
    # Return the new calculation
    return new_calculation

# - Update

@router.put("/attributes_def",
    tags=["Elements"],
    response_model=Attribute_Definition,
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Attribute updated successfully"},
        404: {"description": "Template or attribute not found"},
        400: {"description": "Cannot modify non-draft template or reserved attribute name used or name in wrong format"}
    }
)
def update_attribute(attribute_request: Attribute_Definition_Update):
    
    template = couchdb_client.get_document(attribute_request.template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    if template["status"] != "Draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot modify non-draft template")
    
    trunk = template['trunk']
    result = find_attribute_by_id(trunk, attribute_request.attribute_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Attribute not found')
    
    attribute_picked, parent_entity = result
    
    # Check if the name is unique within the parent entity
    if not check_unique_name(parent_entity, attribute_request.name):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name must be unique within the parent entity")

    # Create a copy of the attribute to modify
    new_attribute = attribute_picked.copy()
    if attribute_request.name:
        new_attribute['name'] = attribute_request.name
    if attribute_request.description:
        new_attribute['description'] = attribute_request.description
    if attribute_request.data_type:
        new_attribute['data_type'] = attribute_request.data_type
    if attribute_request.data_type_constraints: 
        new_attribute['data_type_constraints'] = attribute_request.data_type_constraints
    if attribute_request.defaultvalue:
        new_attribute["defaultvalue"] = attribute_request.defaultvalue
    
    # Replace the old attribute with the new one
    parent_entity['attributes'].remove(attribute_picked)
    parent_entity['attributes'].append(new_attribute)
    
    # Update the template in the database
    success = couchdb_client.update_document(attribute_request.template_id, template)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update template")
    
    return new_attribute

@router.put("/calculations_def",
    tags=["Elements"],
    response_model=Calculation_Definition,
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Calculation updated successfully"},
        404: {"description": "Template or calculation not found"},
        400: {"description": "Cannot modify non-draft template or reserved calculation name used or name in wrong format"}
    }
)
def update_calculation(calculation_request: Calculation_Definition_Update): 

    template = couchdb_client.get_document(calculation_request.template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found") 
    if template["status"] != "Draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot modify non-draft template")
    
    trunk = template['trunk']
    result = find_calculation_by_id(trunk, calculation_request.calculation_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Calculation not found')
    
    calculation_picked, parent_entity = result
    
    # Check if the name is unique within the parent entity
    if not check_unique_name(parent_entity, calculation_request.name):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name must be unique within the parent entity")

    # Create a copy of the calculation to modify
    new_calculation = calculation_picked.copy()
    if calculation_request.name:
        new_calculation['name'] = calculation_request.name
    if calculation_request.description:
        new_calculation['description'] = calculation_request.description        
    if calculation_request.data_type:
        new_calculation['data_type'] = calculation_request.data_type
    if calculation_request.formula:
        new_calculation['formula'] = calculation_request.formula

    # Replace the old calculation with the new one
    parent_entity['calculations'].remove(calculation_picked)
    parent_entity['calculations'].append(new_calculation)

    # Process the template formulas
    formula_resolution.process_templateformulas(template)

    # Check if the formula is valid
    if check_formula_code(trunk, calculation_request.calculation_id):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formula is not valid")
    
    # Update the template in the database
    success = couchdb_client.update_document(calculation_request.template_id, template)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update template")
    
    return new_calculation

@router.put("/templates_def",
    tags=["Templates"],
    response_model=Template_Definition_Full,
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Template updated successfully"},
        404: {"description": "Template not found"},
        400: {"description": "Cannot modify non-draft template"}
    }
)
def update_template(template_request: Template_Definition_Update):
    # Logic to update a template
    template = couchdb_client.get_document(template_request.template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    if template["status"] != "Draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot modify non-draft template")
    
    if template_request.name:
        template["name"] = template_request.name

    # Check if the name is in the correct format
    if not check_name_format(template['name']):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name must be in the correct format")
    
    # Check if the name is unique
    if not check_unique_template_name(template['name']):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name must be unique")

    template['id'] = template_request.template_id
    return template

@router.put("/entities_def",
    tags=["Elements"],
    response_model=Entity_Definition,
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Entity updated successfully"},
        404: {"description": "Template or entity not found"},
        400: {"description": "Cannot modify non-draft template or reserved entity name used or name in wrong format or name is not unique within the parent entity"}
    }
)
def update_entity(entity_request: Entity_Definition_Update):
    template = couchdb_client.get_document(entity_request.template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    if template["status"] != "Draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot modify non-draft template") 
    
    result = find_entity_by_id(template['trunk'], entity_request.entity_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Entity not found')
    
    source_entity, parent_entity = result
    if source_entity['name'] == 'Trunk':
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot modify trunk")
    
    # Check if the name is unique within the parent entity
    if not check_unique_name(parent_entity, entity_request.name):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name must be unique within the parent entity")
    
    # Create a copy of the entity to modify
    new_entity = source_entity.copy()
    if entity_request.name:
        new_entity['name'] = entity_request.name
    if entity_request.description:
        new_entity['description'] = entity_request.description
    
    # Update the template in the database   
    success = couchdb_client.update_document(entity_request.template_id, template)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update template")
    
    return new_entity

# - Delete

@router.delete("/elements",
    tags=["Elements"],
    response_model=Element_Definition,
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Element deleted successfully"},
        404: {"description": "Template or element not found"},
        400: {"description": "Cannot modify non-draft template"}
    }
)
def delete_element(element_request: Element_Definition_Delete):
    # TODO: Perform formula reference checks after deletion
    template = couchdb_client.get_document(element_request.template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    if template["status"] != "Draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot modify non-draft template")
    
    # Delete the element and get the deleted element
    deleted_element = delete_element_by_id(template['trunk'], element_request.element_id)
    if not deleted_element:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Element not found")
    
    # Update the template in the database
    success = couchdb_client.update_document(element_request.template_id, template)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update template")
    
    # Return the deleted element
    return deleted_element

# - Publish

@router.post("/templates_def/publish",
    tags=["Templates"],
    response_model=Template_Definition_Summary,
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Template published successfully"},
        404: {"description": "Template not found"},
        400: {"description": "Only draft templates can be published"}
    }
)
def publish_template(template_request: Template_Definition_Publish):
    
    # Logic to publish a template
    template = couchdb_client.get_document(template_request.template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    if template["status"] != "Draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only draft templates can be published")
    # Set current published template to deprecated
    for doc_id in couchdb_client.db:
        t = couchdb_client.get_document(doc_id)
        if t["status"] == "Published":
            t["status"] = "Deprecated"
            couchdb_client.update_document(doc_id, t)
    template["status"] = "Published"
    template["published_date"] = datetime.now().isoformat()
    couchdb_client.update_document(template_request.template_id, template)
    template['id'] = template_request.template_id

    # Create a trunk entity data item

    trunk_entity = {
        "template_id": template_request.template_id,
        "entity_definition_id": template['trunk']['id'],
        "entity_id": None,
        "parent_entity_id": None,
        "entity_definition_name": "Trunk",
        "creation_date": datetime.now(UTC).isoformat(),
        "is_deleted": False,
        "attributes": [],
        "calculations": []
    }

    trunk_entity_id = couchdb_dataset_client.create_document(trunk_entity)
    trunk_entity["entity_id"] = trunk_entity_id

    return template

# - Copy

@router.post("/attributes_def/copy",
    tags=["Elements"],
    response_model=Attribute_Definition,
    status_code=status.HTTP_200_OK,
    responses={
    200: {"description": "Attribute copied successfully"},
    404: {"description": "Source template,target template, or attribute not found"},
    400: {"description": "Cannot modify non-draft template"}
    })
def copy_attribute(attribute_request: Attribute_Definition_Copy):
    
    # Get Sources  
    source_template = couchdb_client.get_document(attribute_request.source_template_id)
    if not source_template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source template not found")
        
    result = find_attribute_by_id(source_template['trunk'], attribute_request.source_attribute_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source attribute not found")
    
    source_attribute, _ = result
    
    # Get Targets
    target_template = couchdb_client.get_document(attribute_request.target_template_id)
    if not target_template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target template not found")
    if target_template["status"] != "Draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot modify non-draft template")

    # Perform Copy
    new_name = source_attribute['name'] + " (copy)"
    new_attribute = {
        "id": str(uuid.uuid4()),
        "name": new_name,
        "description": source_attribute["description"],
        "data_type": source_attribute["data_type"],
        "data_type_constraints": source_attribute["data_type_constraints"],
        "defaultvalue": source_attribute["defaultvalue"]
    }

    # Add the new attribute to the target entity
    result = find_entity_by_id(target_template['trunk'], attribute_request.target_entity_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target entity not found")
    
    target_entity, _ = result
    if 'attributes' not in target_entity:
        target_entity['attributes'] = []
    target_entity['attributes'].append(new_attribute)

    # Update the target template in the database
    success = couchdb_client.update_document(attribute_request.target_template_id, target_template)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update template")
    
    return new_attribute

@router.post("/entities_def/copy",
    tags=["Elements"],
    response_model=Entity_Definition,
    status_code=status.HTTP_200_OK,
    responses={
    200: {"description": "Entity copied successfully"},
    404: {"description": "Source template, target template, target entity, or source entity not found"},
    400: {"description": "Cannot modify non-draft template or cannot copy trunk."}
    })
def copy_entity(entity_request: Entity_Definition_Copy):
    
    # Get Sources  
    source_template = couchdb_client.get_document(entity_request.source_template_id)
    if not source_template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source template not found")
        
    result = find_entity_by_id(source_template['trunk'], entity_request.source_entity_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source entity not found")
    
    source_entity, _ = result
    if source_entity['name'] == 'Trunk':
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot copy trunk. Use the template copy function instead.")
    
    # Get Targets
    target_template = couchdb_client.get_document(entity_request.target_template_id)
    if not target_template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target template not found")
    if target_template["status"] != "Draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot modify non-draft template")
    
    # Perform Copy
    new_name = source_entity['name'] + " (copy)"
    new_entity = {
        "id": str(uuid.uuid4()),
        "name": new_name,
        "description": source_entity["description"],
    }

    # Add the new entity to the target template
    result = find_entity_by_id(target_template['trunk'], entity_request.target_entity_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target entity not found")
    
    target_entity, _ = result
    if 'entities' not in target_entity:
        target_entity['entities'] = []
    target_entity['entities'].append(new_entity)

    # Update the target template in the database
    success = couchdb_client.update_document(entity_request.target_template_id, target_template)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update template")
    
    return new_entity

# - Test Code
# The following code is used to test the formula resolution process.
# It is not used in the production environment.
# For this reason, it is commented out.

# Manually update the formula code in a template regardless of status.
# @router.post("/update_template_formula_code",
#     tags=["Bug Testing"],
#     status_code=status.HTTP_200_OK,
#     responses={
#     200: {"description": "Template formula code updated successfully"},
#     404: {"description": "Template not found"},
#     })
# def update_template_formula_code(template_id: str):
#     template = couchdb_client.get_document(template_id)
#     if not template:
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    
#     formula_resolution.process_templateformulas(template)
#     couchdb_client.update_document(template_id, template)
#     return template