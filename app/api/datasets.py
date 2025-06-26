from fastapi import APIRouter, HTTPException, status, Response, Query, Path
from pydantic import BaseModel, Field
from typing import List, Optional
from app.db.couchdb_client import CouchDBClient
import uuid
import copy
import re
from datetime import datetime, UTC
import app.api.templates as templates
import app.api.compute as compute

router = APIRouter()

##########################################################
# Pydantic models
#########################################################

# RESPONSES

class Attribute_Single(BaseModel):
    """
    An attribute is a component of an entity.
    It is used to store data about the entity.
    The data stored in an attribute will adhere to the attribute definition.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    attribute_definition_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174007")
    attribute_definition_name: str = Field(..., example="City")
    creation_date: datetime = Field(..., example="2025-05-21T12:00:00")
    update_date: datetime = Field(..., example="2025-05-21T12:00:00")
    value: str = Field(..., example="John Doe")

class Calculation_Single(BaseModel):
    """
    A calculation is a component of an entity.
    When an entity is queried, calculations are evaluated and returned as part of the entity
    per the calculation definition.
    The values are calculations are never stored in the database.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    calculation_definition_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174009")
    calculation_definition_name: str = Field(..., example="Total Sales")
    entity_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174006")
    value: str = Field(..., example="John Doe")

example_attribute_single = Attribute_Single(
    template_id="18fdad59d4703844e9bbcaffa9020370",
    attribute_definition_id="123e4567-e89b-12d3-a456-426614174007",
    attribute_definition_name="City",
    creation_date="2025-05-21T12:00:00",
    update_date="2025-05-21T12:00:00",
    value="John Doe")

example_calculation_single = Calculation_Single(
    template_id="18fdad59d4703844e9bbcaffa9020370",
    calculation_definition_id="123e4567-e89b-12d3-a456-426614174009",
    calculation_definition_name="Total Sales",
    entity_id="123e4567-e89b-12d3-a456-426614174006",
    value="2001.003")

class Entity_Single(BaseModel):
    """
    An entity is a single record in the database.  It is a collection of attributes and calculations.
    Entities are linked to other entities via their parent_entity_id creating a tree structure.
    Entity linking is governed by the template definition.
    An entity can be deleted and restored.  When an entity is deleted, all descendant entities are also deleted.
    When an entity is restored, all descendant entities are also restored.
    A deleted entity will not be returned in a list of entities unless include_deleted is True in the query.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    entity_definition_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174005")
    entity_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174006")
    parent_entity_id: Optional[str] = Field(None, example="18fdad59d4703844e9bbcaffa9020980")
    entity_definition_name: str = Field(..., example="Customer")
    creation_date: datetime = Field(..., example="2025-05-21T12:00:00")
    is_deleted: bool = Field(..., example=False)
    attributes: List[Attribute_Single] = Field(..., example=[example_attribute_single])
    calculations: List[Calculation_Single] = Field(..., example=[example_calculation_single])

example_entity_single = Entity_Single(
    template_id="18fdad59d4703844e9bbcaffa9020370",
    entity_definition_id="123e4567-e89b-12d3-a456-426614174005",
    entity_id="123e4567-e89b-12d3-a456-426614174006",
    parent_entity_id="18fdad59d4703844e9bbcaffa9020980",
    entity_definition_name="Customer",
    creation_date="2025-05-21T12:00:00",
    is_deleted=False,
    attributes=[example_attribute_single],
    calculations=[example_calculation_single])

class Entity_List(BaseModel):
    """
    A list of entities.
    """
    entity_list: List[Entity_Single] = Field(..., example=[example_entity_single])

class Ancestor_Attribute(BaseModel):
    """
    An ancestor attribute is an attribute that is a component of an entity's ancestor entity.
    This is used for returning the values of ancestor attributes when a join is requested.
    """
    attribute_definition_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174007")
    attribute_definition_name: str = Field(..., example="City")
    attribute_value: str = Field(..., example="John Doe")

example_ancestor_attribute = {
    "attribute_definition_id": "123e4567-e89b-12d3-a456-426614174007",
    "attribute_definition_name": "City",
    "attribute_value": "London"}

class Entity_Join(BaseModel):
    """
    A join of an entity and its ancestor attributes.
    The set of ancestor attributes can be limited by passing a list of
    attribute_definition_ids in the request.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    entity_definition_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174005")
    entity_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174006")
    parent_entity_id: Optional[str] = Field(None, example="18fdad59d4703844e9bbcaffa9020980")
    entity_definition_name: str = Field(..., example="Customer")
    creation_date: datetime = Field(..., example="2025-05-21T12:00:00")
    is_deleted: bool = Field(..., example=False)
    attributes: List[Attribute_Single] = Field(..., example=[example_attribute_single])
    calculations: List[Calculation_Single] = Field(..., example=[example_calculation_single])
    ancestor_attribute_list: List[Ancestor_Attribute] = Field(..., example=[example_ancestor_attribute])

example_entity_join = Entity_Join(
    template_id="18fdad59d4703844e9bbcaffa9020370",
    entity_definition_id="123e4567-e89b-12d3-a456-426614174005",
    entity_id="123e4567-e89b-12d3-a456-426614174006",
    parent_entity_id="18fdad59d4703844e9bbcaffa9020980",
    entity_definition_name="Customer",
    creation_date="2025-05-21T12:00:00",
    is_deleted=False,
    attributes=[example_attribute_single],
    calculations=[example_calculation_single],
    ancestor_attribute_list=[example_ancestor_attribute])

class Entity_Join_List(BaseModel):
    """
    A list of entity joins.  Entity joins are used to return a list of entities and
    their ancestor attributes.
    """
    entity_join_list: List[Entity_Join] = Field(..., example=[example_entity_join])

example_range = {
    "fewest_characters": "4",
    "most_characters": "100",
    "non-null entries": "3000",
    "null entries": "20"
}

class Range(BaseModel):
    """
    Provides a range of attribute values for a given attribute definition.
    The range is returned as a dictionary.  The keys will depend on the data type
    of the attribute.
    """
    range: dict = Field(..., example=example_range)

# READ

class Entity_ID(BaseModel):
    """
    Used in queries that return a single entity by entity ID
    """
    entity_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020371")

example_filter = {
    "and":[
        {
            "or": [
                {
                    "attribute_definition_id": "123e4567-e89b-12d3-a456-426614174005",
                    "comparison": ">",
                    "value": "50"
                },
                {
                    "attribute_definition_id": "123e4567-e89b-12d3-a456-426614174006",
                    "comparison": "<",
                    "value": "20"
                }
            ],
        },
        {
            "attribute_definition_id": "123e4567-e89b-12d3-a456-426614174007",
            "comparison": "contains",
            "value": "John"
        }
    ]
}

class Entity_List_By_Definition(BaseModel):
    """
    Used in queries that return a list of entities by entity definition.
    The filter is an optional parameter that can be used to filter the list of entities.
    The filter is a JSON object that contains a list of conditions.
    Each condition is a JSON object that contains an attribute_definition_id, a comparison, and a value.
    The comparison is a string that contains the comparison operator.
    The value is a string that contains the value to compare to.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    entity_definition_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174005")
    filter: Optional[dict] = Field(..., example=example_filter)

class Filter(BaseModel):
    """
    The filter is a JSON object that contains a list of Boolean operators and conditions.
    Each condition is a JSON object that contains an attribute_definition_id, a comparison, and a value.
    The comparison is a string that contains the comparison operator.
    The value is a string that contains the value to compare to.
    """
    filter: Optional[dict] = Field(default={}, example=example_filter)

class Range_By_Definition(BaseModel):
    """
    Used in queries that return a range of attribute values for a given attribute definition.
    The filter is an optional parameter that can be used to filter the range of attribute values.
    The filter is a JSON object that contains a list of conditions.
    Each condition is a JSON object that contains an attribute_definition_id, a comparison, and a value.
    The comparison is a string that contains the comparison operator.
    The value is a string that contains the value to compare to.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    attribute_definition_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174005")
    filter: Optional[dict] = Field(..., example=example_filter)

# CREATE

example_attribute_create = {
    "attribute_definition_id": "123e4567-e89b-12d3-a456-426614174007",
    "value": "John Doe"
}

class Attribute_Create(BaseModel):
    """
    Used in queries that create an attribute on an entity.
    """
    attribute_definition_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174005")
    value: str = Field(..., example=example_attribute_create)

class Entity_Create(BaseModel):
    """
    Used in queries that create an entity.

    The parent_entity_id is optional.  If not provided, and if the template definition allows it, the entity
    will be created without a parent.
    If provided, the entity will be created as a child of the parent entity.
    The attributes are a list of attribute definitions and values.
    The attribute values must adhere to the attribute definition and are
    validated against the attribute definition.
    When attribute values are not provided, the attribute will be created
    using default values specified in the attribute definition.
    """
    template_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020370")
    entity_definition_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174005")
    parent_entity_id: Optional[str] = Field(None, example="18fdad59d4703844e9bbcaffa9020980")
    attributes: List[Attribute_Create] = Field(..., example=[example_attribute_create])

# UPDATE

class Attribute_Update(BaseModel):
    """
    Used in queries that update an attribute on an existing entity.

    The entity_id is the ID of the entity to update.
    The attribute_definition_id is the type of attribute to update.
    The attribute_value is the new value of the attribute.
    The attribute_value must adhere to the attribute definition and is validated against the attribute definition.
    """
    entity_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020371")
    attribute_definition_id: str = Field(..., example="123e4567-e89b-12d3-a456-426614174005")
    attribute_value: str = Field(..., example="John Doe")

# DELETE

class Entity_Delete(BaseModel):
    """
    Used in queries that update whether an entity is deleted or not.
    Deleted entities are not returned in a list of entities unless include_deleted is True in the query.
    Deleting an entity will also delete all descendant entities.
    Restoring an entity will also restore all descendant entities.
    """
    entity_id: str = Field(..., example="18fdad59d4703844e9bbcaffa9020371")

# Initialize CouchDB client
couchdb_client = CouchDBClient(url="http://localhost:5984", username="admin", password="admin", db_name="data_items")

#########################################################
# Helper Functions
#########################################################

def validate_attribute_value(entity_definition: templates.Entity_Definition, attribute_definition_id: str, value: str) -> bool:
    """
    Validate the value of an attribute based on the attribute definition.

    Validations are NOT performed on rich text attributes.

    Integer, whole number, decimal, and percentage validations are inclusive of their min and max values.

    It is assumed that the 'numerator' of a percetnage is stored.  For instance, 50% is stored as 50 and NOT 0.50.
    """
    for attribute_definition in entity_definition["attributes"]:
        if attribute_definition["id"] == attribute_definition_id:
            data_type = attribute_definition["data_type"]
            constraints = attribute_definition["data_type_constraints"].get(data_type, {})
            
            if data_type == "short_text":
                if "text_validation" in constraints and re.match(constraints["text_validation"], value):
                    return True
            elif data_type == "long_text":
                if "text_validation" in constraints and re.match(constraints["text_validation"], value):
                    return True
            elif data_type == "whole_number":
                if "min_value" in constraints and "max_value" in constraints:
                    try:
                        num_value = int(value)
                        if num_value >= constraints["min_value"] and num_value <= constraints["max_value"]:
                            return True
                    except ValueError:
                        return False
            elif data_type == "integer":
                if "min_value" in constraints and "max_value" in constraints:
                    try:
                        num_value = int(value)
                        if num_value >= constraints["min_value"] and num_value <= constraints["max_value"]:
                            return True
                    except ValueError:
                        return False
            elif data_type == "decimal":
                if "min_value" in constraints and "max_value" in constraints:
                    try:
                        num_value = float(value)
                        if num_value >= constraints["min_value"] and num_value <= constraints["max_value"]:
                            return True
                    except ValueError:
                        return False
            elif data_type == "percentage":
                if "min_value" in constraints and "max_value" in constraints:
                    try:
                        num_value = float(value)
                        if num_value >= constraints["min_value"] and num_value <= constraints["max_value"]:
                            return True
                    except ValueError:
                        return False
            elif data_type == "boolean":
                if "true_value" in constraints and "false_value" in constraints:
                    if value == constraints["true_value"] or value == constraints["false_value"]:
                        return True
            elif data_type == "categorical":
                if "values" in constraints:
                    if value in constraints["values"]:
                        return True
            elif data_type == "datetime":
                try:
                    datetime.fromisoformat(value)
                    return True
                except ValueError:
                    return False
            elif data_type == "time":
                try:
                    datetime.fromisoformat(value)
                    return True
                except ValueError:
                    return False
    return False

def evaluate_condition(condition: dict, entity: dict) -> bool:
    """
    Used for evaluating a filtering condition.
    When no condition is provided, the condition is considered to be True.
    When a condition exists but the attribute is not found OR the value is not provided,
    the condition is considered to be False.
    When a condition exists and the attribute is found and the value is provided,
    the condition is evaluated using the comparison operator.
    """

    if not condition:
        return True
    
    attribute = next((attr for attr in entity["attributes"] if attr["attribute_definition_id"] == condition.get("attribute_definition_id")), None)
    if not attribute:
        return False
        
    attribute_value = attribute["value"]
    fixed_value = condition.get("value")
    
    if not fixed_value:
        return False
    
    comparison = condition.get("comparison", "")
    
    if comparison == "=":
        return attribute_value == fixed_value
    elif comparison == "!=":
        return attribute_value != fixed_value
    elif comparison == ">":
        return attribute_value > fixed_value
    elif comparison == ">=":
        return attribute_value >= fixed_value
    elif comparison == "<":
        return attribute_value < fixed_value
    elif comparison == "<=":
        return attribute_value <= fixed_value
    elif comparison == "contains":
        return fixed_value in attribute_value
    else:
        return False

def is_filtered_in(entity: dict, filter: dict) -> bool:
    """
    Recursively evaluates the nodes of a filter to determine if an entity matches the filter.

    The filter is a JSON object that contains a list of conditions.
    Each condition is a JSON object that contains an attribute_definition_id, a comparison, and a value.
    The comparison is a string that contains the comparison operator.
    The value is a string that contains the value to compare to.

    Returns True if the entity matches the filter and must be retained and False otherwise.
    """
    if "and" in filter:
        condition_left = filter["and"][0]
        condition_right = filter["and"][1]
        return is_filtered_in(entity, condition_left) and is_filtered_in(entity, condition_right)
    elif "or" in filter:
        condition_left = filter["or"][0]
        condition_right = filter["or"][1]
        return is_filtered_in(entity, condition_left) or is_filtered_in(entity, condition_right)
    else:
        return evaluate_condition(filter, entity)

def get_ancestor_attributes_and_values(entity: dict) -> List[dict]:
    """
    Used for returning the ancestor attributes and values of an entity.
    The ancestor attributes and values are returned as a list of dictionaries.
    The list of dictionaries contains the attribute_definition_id, attribute_definition_name, and attribute_value.
    The attribute_definition_name is the name of the attribute definition.
    The attribute_value is the value of the attribute.
    """
    attribute_list = []
    parent_entity = couchdb_client.get_document(entity["parent_entity_id"])
    if parent_entity:    
        for attribute in parent_entity.get("attributes", []):
            attribute_list.append({
                "attribute_definition_id": attribute["attribute_definition_id"],
                "attribute_definition_name": attribute["attribute_definition_name"],
                "attribute_value": attribute["value"]
                })
        attribute_list.extend(get_ancestor_attributes_and_values(parent_entity))
    return attribute_list


#########################################################
# Routes
#########################################################

# - Read

@router.get("/entity_by_id/{entity_id}",
    tags=["Data"],
    response_model=Entity_Single,
    summary="Gets an entity",
    description="Get an entity by entity_id",
    responses={
        200: {
            "description": "The entity",
            "content": {
                "application/json": {
                    "example": example_entity_single
                }
            }
        },
        404: {
            "description": "Entity not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Entity not found"}
                }
            }
        }
    },
)
def get_entity_by_id(entity_id: str):
    entity = couchdb_client.get_document(entity_id)
    if not entity:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity not found")
    
    # Ensure entity_id is set
    entity["entity_id"] = entity_id
    
    # Parse datetime fields
    entity["creation_date"] = datetime.fromisoformat(entity["creation_date"])
    
    # Parse datetime fields in attributes
    for attribute in entity.get("attributes", []):
        attribute["creation_date"] = datetime.fromisoformat(attribute["creation_date"])
        attribute["update_date"] = datetime.fromisoformat(attribute["update_date"])
    
    return entity

@router.post("/entity_list/{template_id}/{entity_definition_id}",
    tags=["Data"],
    response_model=Entity_List,
    summary="Gets a list of entities",
    description="Gets a list of entities by template_id and entity_definition_id and filters them by the filter parameter.  Deleted entities are not returned unless include_deleted is True.",
    responses={
        200: {
            "description": "The entity list",
            "content": {
                "application/json": {
                    "example": "TBD"
                }
            }
        }
    },
)
def get_entity_list(template_id: str, entity_definition_id: str, filter: Filter, include_deleted: bool = False):
    template = templates.couchdb_client.get_document(template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    
    entity_definition, parent_entity = templates.find_entity_by_id(template["trunk"], entity_definition_id)
    if not entity_definition:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity definition not found")
    
    # Create a map of attribute definition IDs to names for quick lookup
    attribute_name_map = {attr["id"]: attr["name"] for attr in entity_definition["attributes"]}
    
    if include_deleted:
        query = {
        "selector": {
            "template_id": template_id,
            "entity_definition_id": entity_definition_id,
        }
    }
    else:
        query = {
        "selector": {
            "template_id": template_id,
            "entity_derfinition_id": entity_definition_id,
            "is_deleted": False
        }
    }

    # Convert map object to list and process each entity
    entity_list = []
    for entity in couchdb_client.db.find(query):
        if is_filtered_in(entity, filter.filter):
        
            # Ensure entity_id is set
            entity["entity_id"] = entity["_id"]
            
            # Add entity definition name
            entity["entity_definition_name"] = entity_definition["name"]
            
            # Parse datetime fields
            entity["creation_date"] = datetime.fromisoformat(entity["creation_date"])
            
            # Parse datetime fields in attributes and add attribute definition names
            for attribute in entity.get("attributes", []):
                attribute["creation_date"] = datetime.fromisoformat(attribute["creation_date"])
                attribute["update_date"] = datetime.fromisoformat(attribute["update_date"])
                attribute["attribute_definition_name"] = attribute_name_map.get(attribute["attribute_definition_id"])
            
            entity_list.append(entity)

            for calculation_definition in entity_definition.get("calculations", []):
                calculation_definition_id = calculation_definition["id"]
                calculation_name = calculation_definition["name"]
                calculation_value = compute.calc(template, entity, calculation_definition_id)
                entity["calculations"].append({
                    "template_id": template_id,
                    "calculation_definition_id": calculation_definition_id,
                    "calculation_definition_name": calculation_name,
                    "entity_id": entity["entity_id"],
                    "value": str(calculation_value)
                })
    
    return {"entity_list": entity_list}

@router.post("entity_list_join/{template_id}/{entity_definition_id}",
    tags=["Data"],
    response_model=Entity_Join_List,
    summary="Get a join of entities and their ancestor attributes",
    description="Gets a list of entities by template_id and entity_definition_id and their ancestor attributes and filters them by the filter parameter.  Deleted entities are not returned unless include_deleted is True.",
    responses={
        200: {
            "description": "The entity join list",
            "content": {
                "application/json": {
                    "example": example_entity_join
                }
            }
        }
    },
)
def get_entity_list_join(template_id: str, entity_definition_id: str, filter: Filter, include_deleted: bool = False, attribute_ids: List[str] = []):
    template = templates.couchdb_client.get_document(template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    
    result = templates.find_entity_by_id(template["trunk"], entity_definition_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity definition not found")
    
    entity_definition, parent_entity = result
    
    # Create a map of attribute definition IDs to names for quick lookup
    attribute_name_map = {attr["id"]: attr["name"] for attr in entity_definition["attributes"]}

    if include_deleted:
        query = {
        "selector": {
            "template_id": template_id,
            "entity_definition_id": entity_definition_id,
        }
    }
    else:
        query = {
        "selector": {
            "template_id": template_id,
            "entity_definition_id": entity_definition_id,
            "is_deleted": False
        }
    }

    entity_list = []

    for entity in couchdb_client.db.find(query):
        if not filter.filter or is_filtered_in(entity, filter.filter):
            ancestor_attribute_list = get_ancestor_attributes_and_values(entity)

            # Ensure entity_id is set
            entity["entity_id"] = entity["_id"]
            
            # Add entity definition name
            entity["entity_definition_name"] = entity_definition["name"]
            
            # Parse datetime fields
            entity["creation_date"] = datetime.fromisoformat(entity["creation_date"])
            
            # Parse datetime fields in attributes and add attribute definition names
            for attribute in entity.get("attributes", []):
                attribute["creation_date"] = datetime.fromisoformat(attribute["creation_date"])
                attribute["update_date"] = datetime.fromisoformat(attribute["update_date"])
                attribute["attribute_definition_name"] = attribute_name_map.get(attribute["attribute_definition_id"])

            if attribute_ids:
                entity["ancestor_attribute_list"] = [attr for attr in ancestor_attribute_list if attr["attribute_definition_id"] in attribute_ids]
            else:
                entity["ancestor_attribute_list"] = ancestor_attribute_list
            
            entity_list.append(entity)
    
    return {"entity_join_list": entity_list}

@router.post('/attribute_range/{template_id}/{attribute_definition_id}',
    tags=["Data"],
    response_model=Range,
    summary="Gets a range of attribute values",
    description="Get a range of attribute values by template_id and attribute_definition_id for non-deleted entities.  Deleted entities are not returned. Entities are filtered by the filter parameter.",
    responses={
        200: {
            "description": "The range",
            "content": {
                "application/json": {
                    "example": example_range
                }
            }
        }
    },
)
def get_attribute_range(template_id: str, entity_definition_id: str, attribute_definition_id: str, filter: Filter):
    template = templates.couchdb_client.get_document(template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    
    attribute_definition, parent_entity = templates.find_attribute_by_id(template["trunk"], attribute_definition_id)
    if not attribute_definition:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attribute definition not found")

    query = {
        "selector": {
            "template_id": template_id,
            "entity_definition_id": entity_definition_id,
            "is_deleted": False
        }
    }

    value_list = []

    for entity in couchdb_client.db.find(query):
        if is_filtered_in(entity, filter.filter):
            for attr in entity["attributes"]:
                if attr["attribute_definition_id"] == attribute_definition_id:
                    value_list.append(attr["value"])

    # Handle empty value list
    if not value_list:
        if attribute_definition["data_type"] in ["short_text", "long_text"]:
            return {"range": {"fewest_characters": 0,
                    "most_characters": 0,
                    "non_null_count": 0,
                    "null_count": 0}}
        elif attribute_definition["data_type"] == "rich_text":
            return {"range": {"non_null_count": 0,
                    "null_count": 0}}
        elif attribute_definition["data_type"] in ["whole_number", "integer", "decimal", "percentage"]:
            return {"range": {"minimum": 0,
                    "maximum": 0,
                    "mean": 0,
                    "non_null_count": 0,
                    "null_count": 0}}
        elif attribute_definition["data_type"] == "boolean":
            return {"range": {"number_of_true": 0,
                    "number_of_false": 0,
                    "non_null_count": 0,
                    "null_count": 0}}
        elif attribute_definition["data_type"] == "categorical":
            return {"range": {"values": []}}
        elif attribute_definition["data_type"] in ["datetime", "time"]:
            return {"range": {"minimum": None,
                    "maximum": None,
                    "non_null_count": 0,
                    "null_count": 0}}
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid attribute data type")

    # Handle non-empty value list
    if attribute_definition["data_type"] in ["short_text", "long_text"]:
        return {"range": {"fewest_characters": len(min(value_list, key=len)),
                "most_characters": len(max(value_list, key=len)),
                "non_null_count": len([v for v in value_list if v != ""]),
                "null_count": len([v for v in value_list if v == ""])}}
    elif attribute_definition["data_type"] == "rich_text":
        return {"range": {"non_null_count": len([v for v in value_list if v != ""]),
                "null_count": len([v for v in value_list if v == ""])}}
    elif attribute_definition["data_type"] in ["whole_number", "integer", "decimal", "percentage"]:
        numeric_values = [float(v) for v in value_list if v != ""]
        return {"range": {"minimum": min(numeric_values) if numeric_values else 0,
                "maximum": max(numeric_values) if numeric_values else 0,
                "mean": sum(numeric_values) / len(numeric_values) if numeric_values else 0,
                "non_null_count": len(numeric_values),
                "null_count": len([v for v in value_list if v == ""])}}
    elif attribute_definition["data_type"] == "boolean":
        return {"range": {"number_of_true": value_list.count("true"),
                "number_of_false": value_list.count("false"),
                "non_null_count": len([v for v in value_list if v != ""]),
                "null_count": len([v for v in value_list if v == ""])}}
    elif attribute_definition["data_type"] == "categorical":
        return {"range": {"values": list(set(value_list))}}
    elif attribute_definition["data_type"] in ["datetime", "time"]:
        datetime_values = [v for v in value_list if v != ""]
        return {"range": {"minimum": min(datetime_values) if datetime_values else None,
                "maximum": max(datetime_values) if datetime_values else None,
                "non_null_count": len(datetime_values),
                "null_count": len([v for v in value_list if v == ""])}}
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid attribute data type")

# - Create

@router.post("/entity_create",
    tags=["Data"],
    response_model=Entity_Single,
    summary="Creates an entity",
    description="Create an entity given a requested parent entity, entity definition, and attributes.  Any attributes not provided will be created with default values specified in the attribute definition.",
    responses={
        200: {
            "description": "The entity",
            "content": {
                "application/json": {
                    "example": example_entity_single
                }
            }
        }
    },
)
def create_entity(entity: Entity_Create):

    requested_parent_entity_id = entity.parent_entity_id
    requested_entity_definition_id = entity.entity_definition_id
    requested_template_id = entity.template_id

    # Check that the template exists and is published
    template = templates.couchdb_client.get_document(requested_template_id)
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    
    if not template["status"] == "Published":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Template is not published")
    
    # Check that the entity definition exists
    entity_definition, parent_entity_definition = templates.find_entity_by_id(template["trunk"], requested_entity_definition_id)

    if not entity_definition:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity definition not found")
    
    # Check if the parent entity exists
    if requested_parent_entity_id != "None":
        parent_entity = couchdb_client.get_document(requested_parent_entity_id)
        if not parent_entity:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parent entity not found")
        
    # Check that the parent entity's entity definition is the same as the requested entity definition's parent entity definition
    mismatch_message = "There's a mismatch between the parent entity definition of the requested entity definition and the entity definition of the requested parent entity."
    
    if requested_parent_entity_id == "None":
        if parent_entity_definition != None:
            # This is a mismatch
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=mismatch_message + "[A]")
        else:
            # This is not a mismatch
            pass
    else:
        if parent_entity_definition == None:
            # This is a mismatch
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=mismatch_message + "[B]")
        else:
            # This is not a mismatch. Check that the parent entity's entity definition is the same as the requested entity definition's parent entity definition
            if parent_entity["entity_definition_id"] != parent_entity_definition["id"]:
                # This is a mismatch
                # print(entity_definition)
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=mismatch_message + " [C] " + parent_entity["entity_definition_id"] + " != " + parent_entity_definition["id"])
            pass 
        
    # Create the entity
    entity_object = {
        "template_id": requested_template_id,
        "entity_definition_id": requested_entity_definition_id,
        "entity_id": None,
        "parent_entity_id": requested_parent_entity_id,
        "entity_definition_name": entity_definition["name"],
        "creation_date": datetime.now(UTC).isoformat(),
        "is_deleted": False,
        "attributes": [],
        "calculations": []
    }

    # Add the attributes requests to a list
    attribute_request_list = entity.attributes
    attribute_request_set = set()
    for attribute_request in attribute_request_list:
        attribute_request_set.add(attribute_request.attribute_definition_id)

    # Add the attribute definitions IDs to a set
    attribute_definition_list = entity_definition["attributes"]
    
    # Cycle through the attribute definitions
    for attribute_definition in attribute_definition_list:
        # Cycle through the attribute requests
        attribute_found = False
        for attribute_request in attribute_request_list:
            # If there's an attribute request that matches the attribute definition and it has not been processed
            if attribute_request.attribute_definition_id == attribute_definition["id"]:
                if attribute_request.attribute_definition_id in attribute_request_set:
                    attribute_found = True
                    # If the attribute value is valid
                    if validate_attribute_value(entity_definition, attribute_definition["id"], attribute_request.value):
                        # Add the attribute value to the entity
                        attribute_object = {
                            "template_id": entity.template_id,
                            "attribute_definition_id": attribute_definition["id"],
                            "value": attribute_request.value,
                            "attribute_definition_name": attribute_definition["name"],
                            "creation_date": datetime.now(UTC).isoformat(),
                            "update_date": datetime.now(UTC).isoformat()
                        }
                        entity_object["attributes"].append(attribute_object)
                    else:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid attribute value")
                    # Remove the attribute request from the set
                    if attribute_request.attribute_definition_id in attribute_request_set:
                        attribute_request_set.remove(attribute_request.attribute_definition_id)
                    # Break out of the attribute request loop
                    break
        if not attribute_found:
            # Add the attribute default value to the entity
            attribute_object = {
                "template_id": entity.template_id,
                "attribute_definition_id": attribute_definition["id"],
                "value": attribute_definition["defaultvalue"],
                "attribute_definition_name": attribute_definition["name"],
                "creation_date": datetime.now(UTC).isoformat(),
                "update_date": datetime.now(UTC).isoformat()
            }
            entity_object["attributes"].append(attribute_object)
            # Remove the attribute request from the set
            if attribute_request.attribute_definition_id in attribute_request_set:
                attribute_request_set.remove(attribute_request.attribute_definition_id)

    # If the request set is not empty, raise an error
    if attribute_request_set:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid attribute ids")

    # Create the entity in the database
    entity_id = couchdb_client.create_document(entity_object)
    entity_object["entity_id"] = entity_id

    # Return the entity
    return entity_object

# - Update

@router.put("/attribute_update/{entity_id}/{attribute_definition_id}/{attribute_value}",
    tags=["Data"],
    response_model=Attribute_Single,
    summary="Updates an attribute",
    description="Update an attribute given an entity_id and attribute_definition_id.  Validates that the attribute value is valid for the attribute definition.",
    responses={
        200: {
            "description": "The attribute",
            "content": {
                "application/json": {
                    "example": example_attribute_single
                }
            }
        }
    },
)
def update_attribute(entity_id: str, attribute_definition_id: str, attribute_value: str):
    entity = couchdb_client.get_document(entity_id)
    if not entity:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity not found")
    
    # Get the template and entity definition
    template = templates.couchdb_client.get_document(entity["template_id"])
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    
    if not template["status"] == "Published":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Template is not published")

    entity_definition, parent_entity = templates.find_entity_by_id(template["trunk"], entity["entity_definition_id"])
    if not entity_definition:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity definition not found")
    
    if entity["is_deleted"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Updates are not allowed on deleted entities")
    
    for attribute_object in entity["attributes"]:
        if attribute_object["attribute_definition_id"] == attribute_definition_id:
            if validate_attribute_value(entity_definition, attribute_definition_id, attribute_value):
                attribute_object["value"] = attribute_value
                attribute_object["update_date"] = datetime.now(UTC).isoformat()
                couchdb_client.update_document(entity_id, entity)
                return attribute_object
            else:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid attribute value")
    
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attribute not found")

# DELETE

@router.delete("/entity_delete/{entity_id}",
    tags=["Data"],
    response_model=Entity_Single,
    summary="Deletes an entity",
    description="Delete an entity and all descendant entities given an entity_id",
    responses={
        200: {
            "description": "The entity",
            "content": {
                "application/json": {
                    "example": example_entity_single
                }
            }
        }
    },
)
def delete_entity(entity_id: str):
    entity = couchdb_client.get_document(entity_id)

    if not entity:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity not found")

    template = templates.couchdb_client.get_document(entity["template_id"])
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    
    if not template["status"] == "Published":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Template is not published")

    def delete_entity_recursive(entity_id: str, entity: dict):
        query = {
        "selector": {
                "parent_entity_id": entity_id,
            }
        }
        for entity_child in couchdb_client.db.find(query):
            delete_entity_recursive(entity_child["_id"], entity_child)
        
        # Ensure all required fields are set
        entity["entity_id"] = entity_id
        entity["is_deleted"] = True
        entity["update_date"] = datetime.now(UTC).isoformat()
        
        # Parse datetime fields for response
        entity["creation_date"] = datetime.fromisoformat(entity["creation_date"])
        
        # Parse datetime fields in attributes for response
        for attribute in entity.get("attributes", []):
            attribute["creation_date"] = datetime.fromisoformat(attribute["creation_date"])
            attribute["update_date"] = datetime.fromisoformat(attribute["update_date"])
        
        # Create a copy for CouchDB with ISO format strings
        entity_for_db = copy.deepcopy(entity)
        entity_for_db["creation_date"] = entity_for_db["creation_date"].isoformat()
        for attribute in entity_for_db.get("attributes", []):
            attribute["creation_date"] = attribute["creation_date"].isoformat()
            attribute["update_date"] = attribute["update_date"].isoformat()
        
        couchdb_client.update_document(entity_id, entity_for_db)

    delete_entity_recursive(entity_id, entity)    

    return entity

@router.post("/entity_restore/{entity_id}",
    tags=["Data"],
    response_model=Entity_Single,
    summary="Restores an entity",
    description="Restore an entity and all of its descendant entities given an entity_id",
    responses={
        200: {
            "description": "The entity",
            "content": {
                "application/json": {
                    "example": example_entity_single
                }
            }
        }
    },
)
def restore_entity(entity_id: str):
    entity = couchdb_client.get_document(entity_id)

    if not entity:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity not found")

    template = templates.couchdb_client.get_document(entity["template_id"])
    if not template:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found")
    
    if not template["status"] == "Published":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Template is not published")

    def delete_entity_recursive(entity_id: str, entity: dict):
        query = {
        "selector": {
                "parent_entity_id": entity_id,
            }
        }
        for entity_child in couchdb_client.db.find(query):
            delete_entity_recursive(entity_child["_id"], entity_child)
        
        # Ensure all required fields are set
        entity["entity_id"] = entity_id
        entity["is_deleted"] = False
        entity["update_date"] = datetime.now(UTC).isoformat()
        
        # Parse datetime fields for response
        entity["creation_date"] = datetime.fromisoformat(entity["creation_date"])
        
        # Parse datetime fields in attributes for response
        for attribute in entity.get("attributes", []):
            attribute["creation_date"] = datetime.fromisoformat(attribute["creation_date"])
            attribute["update_date"] = datetime.fromisoformat(attribute["update_date"])
        
        # Create a copy for CouchDB with ISO format strings
        entity_for_db = copy.deepcopy(entity)
        entity_for_db["creation_date"] = entity_for_db["creation_date"].isoformat()
        for attribute in entity_for_db.get("attributes", []):
            attribute["creation_date"] = attribute["creation_date"].isoformat()
            attribute["update_date"] = attribute["update_date"].isoformat()
        
        couchdb_client.update_document(entity_id, entity_for_db)

    delete_entity_recursive(entity_id, entity)    

    return entity