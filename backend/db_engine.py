from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean, ForeignKey, text, inspect
from sqlalchemy.engine import URL
import urllib.parse
from typing import Dict, List, Any
from schema_engine import SchemaMap, TableSchema

def get_engine(connection_string: str):
    # connection_string might be raw ODBC or SQLAlchemy URL
    # if it doesn't start with mssql+pyodbc, wrap it?
    # Assume standard ODBC string from user, convert to SQLAlchemy URL
    if "mssql" not in connection_string:
        # Auto-append driver if missing
        if "driver" not in connection_string.lower():
            if not connection_string.strip().endswith(";"):
                connection_string += ";"
            # Prefer ODBC Driver 17, but we could try others. 
            # Given user has 17 and 18, 17 is a safe bet.
            connection_string += "Driver={ODBC Driver 17 for SQL Server};"
            
        # Auto-append TrustServerCertificate=yes for local dev (fixes SSL errors with newer drivers)
        if "trustservercertificate" not in connection_string.lower():
            if not connection_string.strip().endswith(";"):
                connection_string += ";"
            connection_string += "TrustServerCertificate=yes;"

        # Sanitize Trusted_Connection (ODBC expects 'yes', .NET uses 'True')
        connection_string = connection_string.replace("Trusted_Connection=True", "Trusted_Connection=yes")

        params = urllib.parse.quote_plus(connection_string)
        url = f"mssql+pyodbc:///?odbc_connect={params}"
        return create_engine(url)
    return create_engine(connection_string)

def flatten_data(schema_map: SchemaMap, json_data: Any) -> Dict[str, List[Dict]]:
    # 1. Map schemas by name for easy lookup
    table_schemas = {t.name: t for t in schema_map.tables}
    
    # 2. Store for rows: table_name -> list of dicts
    flat_rows = {t.name: [] for t in schema_map.tables}
    
    # 3. ID Counters
    id_counters = {t.name: 1 for t in schema_map.tables}
    
    # Queue: (table_name, list_objects, parent_id)
    queue = []
    
    root_table = next((t for t in schema_map.tables if t.is_root), None)
    if not root_table:
        return {} # Should not happen if analyze worked
        
    if isinstance(json_data, dict):
        queue.append((root_table.name, [json_data], None))
    elif isinstance(json_data, list):
        queue.append((root_table.name, json_data, None))
        
    while queue:
        current_table_name, objects, parent_id = queue.pop(0)
        schema = table_schemas.get(current_table_name)
        if not schema: continue
        
        for obj in objects:
            if not isinstance(obj, dict):
                 # Scalar in list? Handled as "Value" in analyze, need to match here.
                 # If analyze saw scalars, it expected [{"Value": x}].
                 # We must adapt if raw data is scalar list.
                 obj = {"Value": obj}
            
            # Generate ID
            current_id = id_counters[current_table_name]
            id_counters[current_table_name] += 1
            
            # Prepare Row
            row = {"id": current_id}
            if parent_id and f"{schema.name.rsplit('_', 1)[0]}_id" in [c.name for c in schema.columns]:
                # Heuristic: verify if column exists matching parent logic
                # The schema engine uses: parent_table_name + "_id"
                # Need to find WHICH column is the FK. 
                # Better: schema says which col is FK.
                fk_col = next((c for c in schema.columns if c.is_fk), None)
                if fk_col:
                     row[fk_col.name] = parent_id
            
            # Process fields
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    # Child table
                    child_table_name = f"{current_table_name}_{k}"
                    if child_table_name in table_schemas:
                        child_list = v if isinstance(v, list) else [v]
                        queue.append((child_table_name, child_list, current_id))
                else:
                    # Scalar
                    col_name = k
                    if k.lower() == "id":
                        col_name = "original_id"
                    
                    if any(c.name == col_name for c in schema.columns):
                        row[col_name] = v
                        
            flat_rows[current_table_name].append(row)
            
    return flat_rows

def sync_to_db(connection_string: str, schema_map: SchemaMap, flat_data: Dict[str, List[Dict]]):
    engine = get_engine(connection_string)
    metadata = MetaData()
    
    # 1. Define Tables in SQLAlchemy Metadata
    # We need to define them to create them.
    # Order matters for creation (Parents first)
    
    # Sort tables by dependency (Parents first)
    # A simple topological sort or just repeated passes
    sorted_tables = []
    pending = list(schema_map.tables)
    while pending:
        progress = False
        for t in pending[:]:
            # Check if parent is already in sorted or doesn't exist
            fk_col = next((c for c in t.columns if c.is_fk), None)
            if not fk_col or fk_col.fk_table in [st.name for st in sorted_tables] or fk_col.fk_table not in [x.name for x in schema_map.tables]:
                sorted_tables.append(t)
                pending.remove(t)
                progress = True
        if not progress:
            # Cycle or issue, just add remainder
            sorted_tables.extend(pending)
            break
            
    # Define SQLAlchemy Table objects
    sa_tables = {}
    for t_schema in sorted_tables:
        columns = []
        for c in t_schema.columns:
            if c.name == "id":
                # We turn OFF identity on insert if we want to force our own IDs
                # SQL Server: IDENTITY_INSERT ON
                # Easier: Don't use IDENTITY in definition, just INT PK.
                col = Column(c.name, Integer, primary_key=True, autoincrement=False)
            elif c.is_fk:
                 col = Column(c.name, Integer, ForeignKey(f"{c.fk_table}.id"))
            elif c.type == "INT":
                col = Column(c.name, Integer)
            elif c.type == "FLOAT":
                col = Column(c.name, Float)
            elif c.type == "BIT":
                col = Column(c.name, Boolean)
            else:
                col = Column(c.name, String) # VARCHAR(MAX) default usually
            columns.append(col)
            
        sa_tables[t_schema.name] = Table(t_schema.name, metadata, *columns)

    # 2. Operations
    with engine.begin() as conn:
        # Disable constraints?
        # Better: Drop in reverse order
        for t_schema in reversed(sorted_tables):
            # Check existence? Metadata.drop_all handles this if bound?
            # Or use raw SQL for safety against schema drift
            conn.execute(text(f"IF OBJECT_ID('{t_schema.name}', 'U') IS NOT NULL DROP TABLE {t_schema.name}"))
            
        # Create Tables
        metadata.create_all(conn)
        
        # Insert Data
        for t_schema in sorted_tables:
            rows = flat_data.get(t_schema.name, [])
            if rows:
                # Batch insert
                # 500 rows at a time?
                chunk_size = 500
                for i in range(0, len(rows), chunk_size):
                    conn.execute(sa_tables[t_schema.name].insert(), rows[i:i+chunk_size])
                    
    return {"status": "success", "rows_inserted": {k: len(v) for k, v in flat_data.items()}}
