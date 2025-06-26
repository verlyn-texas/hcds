import app.api.datasets as datasets
from app.db.couchdb_client import CouchDBClient
from typing import Optional, List, Dict, Any

# This file is used to create data manually.
# Is is meant to be used for testing purposed and NOT to replace the API.
# File will be exectuted in a terminal window and interested with by the user.
# The file will call the functions in the datasets.py file as needed.
# When this file is run, it will guide the user through the process of creating data by:
#  - Allowing users to select an entity definition from a list
#    - Names will be sourced from the "Published" template. 
#    - The names will be the entity definition path (e.g. policy.location.building)
#  - Enabling users to select an allowed parent entity
#    - The entities must belong to the published template
#    - The entities must be of the same type as the parent of the entity definition
#  - Enabling users to set attribute values
#    - The user will be prompted to provide a value for each of the attribute defintions associated with the entity definition
#      - The attribute definition type will be displayed so the user can provide the correct type of value
#      - The default value will be available for selection

def get_published_template() -> Optional[Dict[str, Any]]:
    """Get the published template from the database."""
    couchdb_client = CouchDBClient(url="http://localhost:5984", username="admin", password="admin", db_name="structure")
    query = {
        "selector": {
            "status": "Published"
        }
    }
    results = list(couchdb_client.db.find(query))
    return results[0] if results else None

def get_entity_path(entity: Dict[str, Any], path: str = "") -> str:
    """Get the full path of an entity definition (e.g., policy.location.building), excluding the trunk entity."""
    if entity['name'] == 'Trunk':
        # Skip the trunk entity and only process its children
        if 'entities' in entity:
            for child in entity['entities']:
                yield from get_entity_path(child, "")
        return
    
    current_path = f"{path}.{entity['name']}" if path else entity['name']
    if 'entities' in entity:
        for child in entity['entities']:
            yield from get_entity_path(child, current_path)
    yield current_path

def get_entity_by_path(template: Dict[str, Any], path: str) -> Optional[Dict[str, Any]]:
    """Find an entity definition by its path."""
    parts = path.split('.')
    current = template['trunk']
    
    for part in parts:
        found = False
        if 'entities' in current:
            for entity in current['entities']:
                if entity['name'].lower() == part.lower():
                    current = entity
                    found = True
                    break
        if not found:
            return None
    return current

def get_parent_entities(parent_entity_definition_id: str) -> List[Dict[str, Any]]:
    """
    Get all entities of a specific entity defintion ID.
    This will use the dataset database and not the structure database.
    """
    couchdb_client = CouchDBClient(url="http://localhost:5984", username="admin", password="admin", db_name="data_items")
    query = {
        "selector": {
            "entity_definition_id": parent_entity_definition_id,
        }
    }
    results = list(couchdb_client.db.find(query))
    return results

def validate_attribute_value(value: str, data_type: str, constraints: Dict[str, Any]) -> bool:
    """Validate an attribute value against its type and constraints."""
    try:
        if data_type == 'decimal':
            num = float(value)
            if 'min_value' in constraints and num < constraints['min_value']:
                return False
            if 'max_value' in constraints and num > constraints['max_value']:
                return False
        elif data_type == 'whole_number':
            num = int(value)
            if 'min_value' in constraints and num < constraints['min_value']:
                return False
            if 'max_value' in constraints and num > constraints['max_value']:
                return False
        elif data_type == 'boolean':
            if value.lower() not in ['true', 'false', 'yes', 'no']:
                return False
        # Add more type validations as needed
        return True
    except ValueError:
        return False

def main():
    # Get the published template
    template = get_published_template()
    if not template:
        print("No published template found.")
        return
    
    while True:
        # Get all entity definition paths
        print('######################################')
        print("\nAvailable entity definitions:")
        entity_paths = list(get_entity_path(template['trunk']))
        for i, path in enumerate(entity_paths, 1):
            print(f"{i}. {path}")

        # Let user select an entity definition
        while True:
            try:
                choice = int(input("\nSelect an entity definition (number): ")) - 1
                if 0 <= choice < len(entity_paths):
                    selected_path = entity_paths[choice]
                    break
                print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a number.")

        # Get the selected entity definition
        selected_entity = get_entity_by_path(template, selected_path)
        if not selected_entity:
            print("Error: Could not find selected entity definition.")
            return

        # Get parent entities of the same type as the selected entity's parent
        # Display attributes so they can be distinguised from each other
        parent_type = selected_entity['parent_id']
        parent_entities = get_parent_entities(parent_type)
        
        print('######################################')
        print("\nAvailable parent entities:")
        for i, parent in enumerate(parent_entities, 1):
            print(f"\n{i}. {parent['entity_definition_name']}")
            if 'attributes' in parent:
                print("   Attributes:")
                for attr in parent['attributes']:
                    print(f"   - {attr['attribute_definition_name']}: {attr['value']}")

        # Let user select a parent
        while True:
            try:
                choice = int(input("\nSelect a parent entity (number): ")) - 1
                if 0 <= choice < len(parent_entities):
                    selected_parent = parent_entities[choice]
                    break
                print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a number.")

        # Get attribute values
        attribute_list = []
        if 'attributes' in selected_entity:
            print("\nSetting attribute values:")
            for attr in selected_entity['attributes']:
                print(f"\nAttribute: {attr['name']}")
                print(f"Type: {attr['data_type']}")
                print(f"Default value: {attr['defaultvalue']}")
                
                while True:
                    value = input("Enter value (or press Enter for default): ").strip()
                    if not value:
                        value = attr['defaultvalue']
                    
                    if validate_attribute_value(value, attr['data_type'], attr['data_type_constraints']):
                        attribute = datasets.Attribute_Create(
                            attribute_definition_id=attr['id'],
                            value=value
                        )
                        attribute_list.append(attribute)
                        break
                    print("Invalid value. Please try again.")

        # Create the data
        try:

            entity = datasets.Entity_Create(
                template_id=template['_id'],
                parent_entity_id=selected_parent['_id'],
                entity_definition_id=selected_entity['id'],
                attributes=attribute_list
            )

            print(f'entity: {entity}')
            
            datasets.create_entity(entity)

            print("\nData created successfully!")
        except Exception as e:
            print(f"\nError creating data: {str(e)}")

if __name__ == "__main__":
    main()

