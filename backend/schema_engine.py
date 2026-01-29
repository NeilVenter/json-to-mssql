from typing import Any, Dict, List, Optional
from pydantic import BaseModel

class ColumnSchema(BaseModel):
    name: str
    type: str  # NVARCHAR(MAX), INT, BIT, FLOAT, DATETIME
    is_pk: bool = False
    is_fk: bool = False
    fk_table: Optional[str] = None
    fk_column: Optional[str] = None

class TableSchema(BaseModel):
    name: str # Table Name
    columns: List[ColumnSchema]
    is_root: bool = False # If it's the main object(s)

class SchemaMap(BaseModel):
    tables: List[TableSchema]

def determine_sql_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BIT"
    if isinstance(value, int):
        return "INT"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, str):
        # We could try to detect dates, but for now safe default
        return "NVARCHAR(MAX)"
    return "NVARCHAR(MAX)"

def analyze_json_structure(data: Any, root_name: str = "Root") -> SchemaMap:
    tables = {} # Name -> TableSchema
    
    # Queue for BFS/DFS: (name, data_list, parent_table, foreign_key_value_col)
    # But wait, generic JSON might not have IDs. 
    # We will generate synthetic PKs (id INT IDENTITY) for everything.
    # And synthetic FKs (parent_id INT).
    
    # We treat everything as a list of objects.
    # If root is a dict, wrap it in list.
    
    queue = [] # (table_name, list_of_objects, parent_table_name)
    
    if isinstance(data, dict):
        queue.append((root_name, [data], None))
    elif isinstance(data, list):
        queue.append((root_name, data, None))
    else:
        # Scalar root? Unlikely for "JSON file" but possible.
        pass
        
    processed_tables = set()

    while queue:
        current_table_name, objects, parent_table_name = queue.pop(0)
        
        if not objects:
            continue
            
        if current_table_name in processed_tables:
            # Already processed this table name? 
            # In a real recursive structure we might verify if schema matches, 
            # but for simplicity assume consistent schema per key.
            continue
            
        processed_tables.add(current_table_name)
        
        # Analyze columns based on the first non-empty occurrences
        columns_map = {} # col_name -> type
        child_arrays = {} # key -> list of lists (to flatten)
        
        # We verify up to N rows to guess types
        sample_rows = objects[:100] 
        
        for obj in sample_rows:
            if not isinstance(obj, dict):
                continue
                
            for k, v in obj.items():
                if v is None:
                    continue
                    
                if isinstance(v, (dict, list)):
                    # potential relationship
                     if isinstance(v, dict):
                         # One-to-one or embedded object? 
                         # Treat as child table with 1 row per parent, or flatten?
                         # Let's treat as child table for cleaner normalized design.
                         # Relationship: "Table_Key"
                         child_name = f"{current_table_name}_{k}"
                         if k not in child_arrays:
                             child_arrays[k] = []
                         child_arrays[k].append(v)
                         
                     elif isinstance(v, list):
                         # One-to-many
                         # Check if list of scalars or objects
                         if v and isinstance(v[0], (dict, list)):
                              child_name = f"{current_table_name}_{k}"
                              if k not in child_arrays:
                                  child_arrays[k] = []
                              child_arrays[k].extend(v)
                         elif v:
                             # List of scalars (e.g. tags: ["a", "b"])
                             # Create a table "Table_Tags" with "Value" column
                             child_name = f"{current_table_name}_{k}"
                             if k not in child_arrays:
                                 child_arrays[k] = []
                             # Transform scalar to object for consistent processing
                             child_arrays[k].extend([{"Value": item} for item in v])
                             
                else:
                    # Scalar
                    if k not in columns_map:
                        columns_map[k] = determine_sql_type(v)
                    else:
                        # Upgrade type if needed (INT -> FLOAT -> STRING)
                        curr = columns_map[k]
                        new = determine_sql_type(v)
                        if curr != new:
                            if "NVARCHAR" in (curr, new):
                                columns_map[k] = "NVARCHAR(MAX)"
                            elif "FLOAT" in (curr, new):
                                columns_map[k] = "FLOAT"
                                
        # Construct Column List
        cols = []
        # Add Synthetic PK
        cols.append(ColumnSchema(name="id", type="INT IDENTITY(1,1)", is_pk=True))
        
        # Add FK to parent if exists
        if parent_table_name:
            cols.append(ColumnSchema(
                name=f"{parent_table_name}_id", 
                type="INT", 
                is_fk=True, 
                fk_table=parent_table_name, 
                fk_column="id"
            ))
            
        for k, v in columns_map.items():
            if k.lower() == "id":
                cols.append(ColumnSchema(name="original_id", type=v))
            else:
                cols.append(ColumnSchema(name=k, type=v))
            
        tables[current_table_name] = TableSchema(
            name=current_table_name,
            columns=cols,
            is_root=(parent_table_name is None)
        )
        
        # Add children to queue
        for k, v_list in child_arrays.items():
            child_table_name = f"{current_table_name}_{k}"
            # Only add to queue if we haven't seen it or want to process
            # Note: We need to pass flattened list
            queue.append((child_table_name, v_list, current_table_name))
            
    return SchemaMap(tables=list(tables.values()))
