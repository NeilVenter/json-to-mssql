from typing import Any, Dict, List, Optional
from pydantic import BaseModel
import hashlib

MAX_NAME_LENGTH = 110

def truncate_name(name: str, max_length: int = MAX_NAME_LENGTH) -> str:
    """
    Truncate identifiers to max_length characters from the start.
    Keeps the end of the name (more meaningful part) and prepends a letter-prefixed hash for uniqueness.
    """
    if len(name) <= max_length:
        return name
    
    # Reserve 10 characters for "T" + "_" + 8-char hash
    hash_prefix_length = 10
    available_for_name = max_length - hash_prefix_length
    
    # Create hash of full name for uniqueness
    name_hash = hashlib.md5(name.encode()).hexdigest()[:8]
    
    # Truncate from start and prepend "T_" + hash to ensure valid identifier starting with letter
    truncated = "T_" + name_hash + "_" + name[-available_for_name:]
    return truncated

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
    
    # Check if data has only 1 root element - if so, ignore the synthetic root
    single_root = False
    if isinstance(data, dict):
        # Single root object - process it directly without the wrapper
        queue.append((root_name, [data], None))
        single_root = True
    elif isinstance(data, list):
        # If it's a list, check if we should treat it as single root
        if len(data) == 1 and isinstance(data[0], dict):
            # Single root object in a list
            queue.append((root_name, [data[0]], None))
            single_root = True
        else:
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
                         child_name = truncate_name(f"{current_table_name}_{k}")
                         if k not in child_arrays:
                             child_arrays[k] = []
                         child_arrays[k].append(v)
                         
                     elif isinstance(v, list):
                         # One-to-many
                         # Check if list of scalars or objects
                         if v and isinstance(v[0], (dict, list)):
                              child_name = truncate_name(f"{current_table_name}_{k}")
                              if k not in child_arrays:
                                  child_arrays[k] = []
                              child_arrays[k].extend(v)
                         elif v:
                             # List of scalars (e.g. tags: ["a", "b"])
                             # Create a table "Table_Tags" with "Value" column
                             child_name = truncate_name(f"{current_table_name}_{k}")
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
            fk_col_name = truncate_name(f"{parent_table_name}_id")
            cols.append(ColumnSchema(
                name=fk_col_name, 
                type="INT", 
                is_fk=True, 
                fk_table=parent_table_name, 
                fk_column="id"
            ))
            
        for k, v in columns_map.items():
            if k.lower() == "id":
                cols.append(ColumnSchema(name="original_id", type=v))
            else:
                col_name = truncate_name(k)
                cols.append(ColumnSchema(name=col_name, type=v))
            
        tables[current_table_name] = TableSchema(
            name=current_table_name,
            columns=cols,
            is_root=(parent_table_name is None)
        )
        
        # Add children to queue
        for k, v_list in child_arrays.items():
            child_table_name = truncate_name(f"{current_table_name}_{k}")
            # Only add to queue if we haven't seen it or want to process
            # Note: We need to pass flattened list
            queue.append((child_table_name, v_list, current_table_name))
    
    # If single root was detected, remove the synthetic Root table and promote its children
    result_tables = list(tables.values())
    if single_root and root_name in tables:
        root_table = tables[root_name]
        # Remove the synthetic root table
        result_tables = [t for t in result_tables if t.name != root_name]
        # Update parent references for direct children of root (set to None)
        result_tables = [
            TableSchema(
                name=t.name,
                columns=[
                    c for c in t.columns 
                    if not (c.is_fk and c.fk_table == root_name)
                ] if any(c.is_fk and c.fk_table == root_name for c in t.columns) else t.columns,
                is_root=True if any(c.is_fk and c.fk_table == root_name for c in t.columns) else t.is_root
            )
            for t in result_tables
        ]
            
    return SchemaMap(tables=result_tables)
