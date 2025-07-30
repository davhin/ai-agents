#!/usr/bin/env python3

import asyncio
import json
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
    LoggingLevel
)
import mcp.types as types

# Database drivers - will be imported conditionally based on database type
try:
    import psycopg2
    import psycopg2.extras
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import pymysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import sqlite3
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

try:
    import pyodbc
    SQLSERVER_AVAILABLE = True
except ImportError:
    SQLSERVER_AVAILABLE = False

server = Server("database-schema-mcp-server")

class DatabaseConnection:
    """Manages database connections and provides unified interface for different DB types."""
    
    def __init__(self, db_type: str, connection_params: Dict[str, Any]):
        self.db_type = db_type.lower()
        self.connection_params = connection_params
        self.connection = None
        self._schema_cache = {}
        
    def connect(self):
        """Establish database connection based on database type."""
        if self.db_type == "postgresql":
            if not POSTGRES_AVAILABLE:
                raise ImportError("psycopg2 not available. Install with: pip install psycopg2-binary")
            self.connection = psycopg2.connect(**self.connection_params)
            
        elif self.db_type == "mysql":
            if not MYSQL_AVAILABLE:
                raise ImportError("pymysql not available. Install with: pip install pymysql")
            self.connection = pymysql.connect(**self.connection_params)
            
        elif self.db_type == "sqlite":
            if not SQLITE_AVAILABLE:
                raise ImportError("sqlite3 not available")
            db_path = self.connection_params.get("database", ":memory:")
            self.connection = sqlite3.connect(db_path)
            
        elif self.db_type == "sqlserver":
            if not SQLSERVER_AVAILABLE:
                raise ImportError("pyodbc not available. Install with: pip install pyodbc")
            connection_string = self.connection_params.get("connection_string")
            if not connection_string:
                # Build connection string from parameters
                server = self.connection_params.get("server", "localhost")
                database = self.connection_params.get("database")
                username = self.connection_params.get("username")
                password = self.connection_params.get("password")
                driver = self.connection_params.get("driver", "ODBC Driver 17 for SQL Server")
                
                connection_string = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
            
            self.connection = pyodbc.connect(connection_string)
            
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries."""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Get column names
            if self.db_type == "postgresql":
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
            elif self.db_type == "mysql":
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
            elif self.db_type == "sqlite":
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
            elif self.db_type == "sqlserver":
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch results
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            results = []
            for row in rows:
                if isinstance(row, dict):
                    results.append(row)
                else:
                    results.append(dict(zip(columns, row)))
            
            return results
            
        finally:
            cursor.close()
    
    def get_table_names(self) -> List[str]:
        """Get list of all table names in the database."""
        if "table_names" in self._schema_cache:
            return self._schema_cache["table_names"]
        
        if self.db_type == "postgresql":
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """
        elif self.db_type == "mysql":
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE()
                AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """
        elif self.db_type == "sqlite":
            query = """
                SELECT name as table_name 
                FROM sqlite_master 
                WHERE type = 'table' 
                AND name NOT LIKE 'sqlite_%'
                ORDER BY name;
            """
        elif self.db_type == "sqlserver":
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                ORDER BY table_name;
            """
        
        results = self.execute_query(query)
        table_names = [row["table_name"] for row in results]
        self._schema_cache["table_names"] = table_names
        return table_names
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get detailed schema information for a specific table."""
        cache_key = f"table_schema_{table_name}"
        if cache_key in self._schema_cache:
            return self._schema_cache[cache_key]
        
        if self.db_type == "postgresql":
            query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    ordinal_position
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND table_schema = 'public'
                ORDER BY ordinal_position;
            """
            params = (table_name,)
            
        elif self.db_type == "mysql":
            query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    ordinal_position
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND table_schema = DATABASE()
                ORDER BY ordinal_position;
            """
            params = (table_name,)
            
        elif self.db_type == "sqlite":
            query = f"PRAGMA table_info({table_name});"
            params = None
            
        elif self.db_type == "sqlserver":
            query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    ordinal_position
                FROM information_schema.columns 
                WHERE table_name = ?
                ORDER BY ordinal_position;
            """
            params = (table_name,)
        
        results = self.execute_query(query, params)
        
        # Normalize results for SQLite (which returns different column names)
        if self.db_type == "sqlite":
            normalized_results = []
            for row in results:
                normalized_results.append({
                    "column_name": row["name"],
                    "data_type": row["type"],
                    "is_nullable": "YES" if not row["notnull"] else "NO",
                    "column_default": row["dflt_value"],
                    "character_maximum_length": None,
                    "numeric_precision": None,
                    "numeric_scale": None,
                    "ordinal_position": row["cid"] + 1
                })
            results = normalized_results
        
        # Get additional information
        indexes = self.get_table_indexes(table_name)
        foreign_keys = self.get_table_foreign_keys(table_name)
        primary_keys = self.get_table_primary_keys(table_name)
        
        schema_info = {
            "table_name": table_name,
            "columns": results,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys,
            "indexes": indexes
        }
        
        self._schema_cache[cache_key] = schema_info
        return schema_info
    
    def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """Get index information for a table."""
        try:
            if self.db_type == "postgresql":
                query = """
                    SELECT 
                        i.relname as index_name,
                        array_agg(a.attname ORDER BY t.seq) as column_names,
                        ix.indisunique as is_unique
                    FROM pg_class t
                    JOIN pg_index ix ON t.oid = ix.indrelid
                    JOIN pg_class i ON i.oid = ix.indexrelid
                    JOIN (SELECT generate_subscripts(indkey, 1) as seq, unnest(indkey) as col, indexrelid FROM pg_index) t ON ix.indexrelid = t.indexrelid
                    JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = t.col
                    WHERE t.relname = %s
                    GROUP BY i.relname, ix.indisunique;
                """
                params = (table_name,)
                
            elif self.db_type == "mysql":
                query = """
                    SELECT 
                        index_name,
                        column_name,
                        non_unique = 0 as is_unique
                    FROM information_schema.statistics 
                    WHERE table_name = %s 
                    AND table_schema = DATABASE()
                    ORDER BY index_name, seq_in_index;
                """
                params = (table_name,)
                
            elif self.db_type == "sqlite":
                query = f"PRAGMA index_list({table_name});"
                params = None
                
            elif self.db_type == "sqlserver":
                query = """
                    SELECT 
                        i.name as index_name,
                        c.name as column_name,
                        i.is_unique
                    FROM sys.indexes i
                    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    JOIN sys.tables t ON i.object_id = t.object_id
                    WHERE t.name = ?
                    ORDER BY i.name, ic.key_ordinal;
                """
                params = (table_name,)
            
            return self.execute_query(query, params)
        except:
            return []
    
    def get_table_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """Get foreign key information for a table."""
        try:
            if self.db_type == "postgresql":
                query = """
                    SELECT 
                        conname as constraint_name,
                        a.attname as column_name,
                        cl.relname as referenced_table,
                        af.attname as referenced_column
                    FROM pg_constraint con
                    JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
                    JOIN pg_class c ON c.oid = con.conrelid
                    JOIN pg_class cl ON cl.oid = con.confrelid
                    JOIN pg_attribute af ON af.attrelid = con.confrelid AND af.attnum = ANY(con.confkey)
                    WHERE c.relname = %s AND con.contype = 'f';
                """
                params = (table_name,)
                
            elif self.db_type == "mysql":
                query = """
                    SELECT 
                        constraint_name,
                        column_name,
                        referenced_table_name as referenced_table,
                        referenced_column_name as referenced_column
                    FROM information_schema.key_column_usage 
                    WHERE table_name = %s 
                    AND table_schema = DATABASE()
                    AND referenced_table_name IS NOT NULL;
                """
                params = (table_name,)
                
            elif self.db_type == "sqlite":
                query = f"PRAGMA foreign_key_list({table_name});"
                params = None
                
            elif self.db_type == "sqlserver":
                query = """
                    SELECT 
                        fk.name as constraint_name,
                        c.name as column_name,
                        rt.name as referenced_table,
                        rc.name as referenced_column
                    FROM sys.foreign_keys fk
                    JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                    JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
                    JOIN sys.tables t ON fk.parent_object_id = t.object_id
                    JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
                    JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
                    WHERE t.name = ?;
                """
                params = (table_name,)
            
            return self.execute_query(query, params)
        except:
            return []
    
    def get_table_primary_keys(self, table_name: str) -> List[str]:
        """Get primary key columns for a table."""
        try:
            if self.db_type == "postgresql":
                query = """
                    SELECT a.attname as column_name
                    FROM pg_constraint con
                    JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
                    JOIN pg_class c ON c.oid = con.conrelid
                    WHERE c.relname = %s AND con.contype = 'p';
                """
                params = (table_name,)
                
            elif self.db_type == "mysql":
                query = """
                    SELECT column_name
                    FROM information_schema.key_column_usage 
                    WHERE table_name = %s 
                    AND table_schema = DATABASE()
                    AND constraint_name = 'PRIMARY';
                """
                params = (table_name,)
                
            elif self.db_type == "sqlite":
                # SQLite primary keys are identified in the table_info pragma
                schema = self.get_table_schema(table_name)
                return [col["column_name"] for col in schema["columns"] if col.get("pk") == 1]
                
            elif self.db_type == "sqlserver":
                query = """
                    SELECT c.name as column_name
                    FROM sys.key_constraints kc
                    JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
                    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    JOIN sys.tables t ON kc.parent_object_id = t.object_id
                    WHERE t.name = ? AND kc.type = 'PK';
                """
                params = (table_name,)
            
            results = self.execute_query(query, params)
            return [row["column_name"] for row in results]
        except:
            return []

class SQLQueryGenerator:
    """Generates SQL queries based on schema information and user requirements."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        
    def generate_select_query(self, table_name: str, columns: Optional[List[str]] = None, 
                            where_conditions: Optional[Dict[str, Any]] = None,
                            order_by: Optional[List[str]] = None,
                            limit: Optional[int] = None) -> str:
        """Generate a SELECT query based on parameters."""
        schema = self.db.get_table_schema(table_name)
        
        # Build column list
        if columns:
            # Validate columns exist
            valid_columns = [col["column_name"] for col in schema["columns"]]
            invalid_columns = [col for col in columns if col not in valid_columns]
            if invalid_columns:
                raise ValueError(f"Invalid columns: {invalid_columns}. Available columns: {valid_columns}")
            column_list = ", ".join(columns)
        else:
            column_list = "*"
        
        # Build base query
        query = f"SELECT {column_list} FROM {table_name}"
        
        # Add WHERE clause
        if where_conditions:
            conditions = []
            for column, value in where_conditions.items():
                if isinstance(value, str):
                    conditions.append(f"{column} = '{value}'")
                elif isinstance(value, (int, float)):
                    conditions.append(f"{column} = {value}")
                elif value is None:
                    conditions.append(f"{column} IS NULL")
                else:
                    conditions.append(f"{column} = '{value}'")
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
        
        # Add ORDER BY
        if order_by:
            valid_columns = [col["column_name"] for col in schema["columns"]]
            valid_order_columns = [col for col in order_by if col.replace(" DESC", "").replace(" ASC", "") in valid_columns]
            if valid_order_columns:
                query += " ORDER BY " + ", ".join(valid_order_columns)
        
        # Add LIMIT
        if limit:
            if self.db.db_type in ["postgresql", "mysql", "sqlite"]:
                query += f" LIMIT {limit}"
            elif self.db.db_type == "sqlserver":
                query = query.replace("SELECT", f"SELECT TOP {limit}")
        
        return query
    
    def generate_insert_query(self, table_name: str, data: Dict[str, Any]) -> str:
        """Generate an INSERT query."""
        schema = self.db.get_table_schema(table_name)
        valid_columns = [col["column_name"] for col in schema["columns"]]
        
        # Validate columns
        invalid_columns = [col for col in data.keys() if col not in valid_columns]
        if invalid_columns:
            raise ValueError(f"Invalid columns: {invalid_columns}. Available columns: {valid_columns}")
        
        columns = list(data.keys())
        values = []
        for value in data.values():
            if isinstance(value, str):
                values.append(f"'{value}'")
            elif value is None:
                values.append("NULL")
            else:
                values.append(str(value))
        
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})"
        return query
    
    def generate_update_query(self, table_name: str, data: Dict[str, Any], 
                            where_conditions: Dict[str, Any]) -> str:
        """Generate an UPDATE query."""
        schema = self.db.get_table_schema(table_name)
        valid_columns = [col["column_name"] for col in schema["columns"]]
        
        # Validate columns
        all_columns = list(data.keys()) + list(where_conditions.keys())
        invalid_columns = [col for col in all_columns if col not in valid_columns]
        if invalid_columns:
            raise ValueError(f"Invalid columns: {invalid_columns}. Available columns: {valid_columns}")
        
        # Build SET clause
        set_clauses = []
        for column, value in data.items():
            if isinstance(value, str):
                set_clauses.append(f"{column} = '{value}'")
            elif value is None:
                set_clauses.append(f"{column} = NULL")
            else:
                set_clauses.append(f"{column} = {value}")
        
        # Build WHERE clause
        where_clauses = []
        for column, value in where_conditions.items():
            if isinstance(value, str):
                where_clauses.append(f"{column} = '{value}'")
            elif value is None:
                where_clauses.append(f"{column} IS NULL")
            else:
                where_clauses.append(f"{column} = {value}")
        
        query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)}"
        return query
    
    def generate_delete_query(self, table_name: str, where_conditions: Dict[str, Any]) -> str:
        """Generate a DELETE query."""
        schema = self.db.get_table_schema(table_name)
        valid_columns = [col["column_name"] for col in schema["columns"]]
        
        # Validate columns
        invalid_columns = [col for col in where_conditions.keys() if col not in valid_columns]
        if invalid_columns:
            raise ValueError(f"Invalid columns: {invalid_columns}. Available columns: {valid_columns}")
        
        # Build WHERE clause
        where_clauses = []
        for column, value in where_conditions.items():
            if isinstance(value, str):
                where_clauses.append(f"{column} = '{value}'")
            elif value is None:
                where_clauses.append(f"{column} IS NULL")
            else:
                where_clauses.append(f"{column} = {value}")
        
        query = f"DELETE FROM {table_name} WHERE {' AND '.join(where_clauses)}"
        return query
    
    def suggest_joins(self, primary_table: str, related_tables: Optional[List[str]] = None) -> List[str]:
        """Suggest possible JOIN queries based on foreign key relationships."""
        primary_schema = self.db.get_table_schema(primary_table)
        foreign_keys = primary_schema["foreign_keys"]
        
        if not foreign_keys:
            return ["No foreign key relationships found for JOIN suggestions."]
        
        join_suggestions = []
        
        for fk in foreign_keys:
            referenced_table = fk.get("referenced_table")
            if referenced_table and (not related_tables or referenced_table in related_tables):
                column_name = fk.get("column_name")
                referenced_column = fk.get("referenced_column")
                
                join_query = f"""
SELECT *
FROM {primary_table} p
JOIN {referenced_table} r ON p.{column_name} = r.{referenced_column};
                """.strip()
                
                join_suggestions.append(join_query)
        
        return join_suggestions if join_suggestions else ["No relevant JOIN relationships found."]

# Global database connection
current_db_connection = None

@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="connect_database",
            description="Connect to a database and inspect its schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_type": {
                        "type": "string",
                        "enum": ["postgresql", "mysql", "sqlite", "sqlserver"],
                        "description": "Type of database to connect to"
                    },
                    "host": {
                        "type": "string",
                        "description": "Database host (not needed for SQLite)"
                    },
                    "port": {
                        "type": "integer",
                        "description": "Database port (not needed for SQLite)"
                    },
                    "database": {
                        "type": "string",
                        "description": "Database name or file path for SQLite"
                    },
                    "username": {
                        "type": "string",
                        "description": "Database username (not needed for SQLite)"
                    },
                    "password": {
                        "type": "string",
                        "description": "Database password (not needed for SQLite)"
                    },
                    "connection_string": {
                        "type": "string",
                        "description": "Full connection string (alternative to individual parameters, mainly for SQL Server)"
                    }
                },
                "required": ["db_type"]
            }
        ),
        Tool(
            name="get_database_schema",
            description="Get complete database schema information including all tables",
            inputSchema={
                "type": "object",
                "properties": {
                    "include_data_samples": {
                        "type": "boolean",
                        "description": "Whether to include sample data from each table",
                        "default": False
                    },
                    "sample_limit": {
                        "type": "integer",
                        "description": "Number of sample rows to retrieve per table",
                        "default": 5
                    }
                }
            }
        ),
        Tool(
            name="get_table_info",
            description="Get detailed information about a specific table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to inspect"
                    },
                    "include_sample_data": {
                        "type": "boolean",
                        "description": "Whether to include sample data from the table",
                        "default": True
                    },
                    "sample_limit": {
                        "type": "integer", 
                        "description": "Number of sample rows to retrieve",
                        "default": 10
                    }
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="generate_sql_query",
            description="Generate SQL queries based on requirements and table schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "query_type": {
                        "type": "string",
                        "enum": ["select", "insert", "update", "delete"],
                        "description": "Type of SQL query to generate"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name of the primary table"
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Columns to select (for SELECT queries)"
                    },
                    "where_conditions": {
                        "type": "object",
                        "description": "WHERE clause conditions as key-value pairs"
                    },
                    "data": {
                        "type": "object", 
                        "description": "Data to insert/update as key-value pairs"
                    },
                    "order_by": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "ORDER BY columns (can include ASC/DESC)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "LIMIT number of results"
                    }
                },
                "required": ["query_type", "table_name"]
            }
        ),
        Tool(
            name="suggest_joins",
            description="Suggest JOIN queries based on foreign key relationships",
            inputSchema={
                "type": "object",
                "properties": {
                    "primary_table": {
                        "type": "string",
                        "description": "Primary table for JOIN suggestions"
                    },
                    "related_tables": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of specific tables to consider for JOINs"
                    }
                },
                "required": ["primary_table"]
            }
        ),
        Tool(
            name="execute_query",
            description="Execute a SQL query and return results (READ-ONLY operations recommended)",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SQL query to execute"
                    },
                    "limit_results": {
                        "type": "integer",
                        "description": "Limit number of results returned",
                        "default": 100
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="explain_query",
            description="Get query execution plan and optimization suggestions",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SQL query to explain"
                    }
                },
                "required": ["query"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """Handle tool calls."""
    global current_db_connection
    
    if name == "connect_database":
        db_type = arguments.get("db_type")
        
        # Build connection parameters
        connection_params = {}
        
        if db_type == "sqlite":
            connection_params["database"] = arguments.get("database", ":memory:")
        else:
            if arguments.get("connection_string"):
                connection_params["connection_string"] = arguments["connection_string"]
            else:
                connection_params.update({
                    "host": arguments.get("host", "localhost"),
                    "database": arguments.get("database"),
                    "user": arguments.get("username"),
                    "password": arguments.get("password")
                })
                
                if arguments.get("port"):
                    connection_params["port"] = arguments["port"]
        
        try:
            current_db_connection = DatabaseConnection(db_type, connection_params)
            current_db_connection.connect()
            
            # Test connection by getting table names
            tables = current_db_connection.get_table_names()
            
            response = f"""Successfully connected to {db_type} database!

Database contains {len(tables)} tables:
{', '.join(tables[:20])}{'...' if len(tables) > 20 else ''}

Use 'get_database_schema' to see complete schema information.
Use 'get_table_info' to inspect specific tables.
Use 'generate_sql_query' to create optimized queries.
"""
            
            return [types.TextContent(type="text", text=response)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Failed to connect to database: {str(e)}")]
    
    elif name == "get_database_schema":
        if not current_db_connection:
            return [types.TextContent(type="text", text="No database connection. Use 'connect_database' first.")]
        
        include_samples = arguments.get("include_data_samples", False)
        sample_limit = arguments.get("sample_limit", 5)
        
        try:
            tables = current_db_connection.get_table_names()
            schema_info = {"database_type": current_db_connection.db_type, "tables": {}}
            
            for table_name in tables:
                table_schema = current_db_connection.get_table_schema(table_name)
                
                if include_samples:
                    try:
                        sample_query = f"SELECT * FROM {table_name} LIMIT {sample_limit}"
                        sample_data = current_db_connection.execute_query(sample_query)
                        table_schema["sample_data"] = sample_data
                    except:
                        table_schema["sample_data"] = "Unable to retrieve sample data"
                
                schema_info["tables"][table_name] = table_schema
            
            # Format response
            response = f"""Database Schema Information ({current_db_connection.db_type})

Total Tables: {len(tables)}

"""
            
            for table_name, table_info in schema_info["tables"].items():
                response += f"TABLE: {table_name}\n"
                response += f"Columns ({len(table_info['columns'])}):\n"
                
                for col in table_info["columns"]:
                    nullable = "NULL" if col["is_nullable"] == "YES" else "NOT NULL"
                    default = f" DEFAULT {col['column_default']}" if col["column_default"] else ""
                    response += f"  - {col['column_name']} ({col['data_type']}) {nullable}{default}\n"
                
                if table_info["primary_keys"]:
                    response += f"Primary Keys: {', '.join(table_info['primary_keys'])}\n"
                
                if table_info["foreign_keys"]:
                    response += "Foreign Keys:\n"
                    for fk in table_info["foreign_keys"]:
                        response += f"  - {fk['column_name']} -> {fk['referenced_table']}.{fk['referenced_column']}\n"
                
                if include_samples and "sample_data" in table_info:
                    response += f"Sample Data ({sample_limit} rows):\n"
                    if isinstance(table_info["sample_data"], list) and table_info["sample_data"]:
                        for row in table_info["sample_data"][:3]:  # Show only first 3 for brevity
                            response += f"  {row}\n"
                
                response += "\n"
            
            return [types.TextContent(type="text", text=response)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error retrieving schema: {str(e)}")]
    
    elif name == "get_table_info":
        if not current_db_connection:
            return [types.TextContent(type="text", text="No database connection. Use 'connect_database' first.")]
        
        table_name = arguments.get("table_name")
        include_samples = arguments.get("include_sample_data", True)
        sample_limit = arguments.get("sample_limit", 10)
        
        try:
            schema = current_db_connection.get_table_schema(table_name)
            
            response = f"""Table Information: {table_name}

COLUMNS ({len(schema['columns'])}):
"""
            
            for col in schema["columns"]:
                nullable = "NULL" if col["is_nullable"] == "YES" else "NOT NULL"
                default = f" DEFAULT {col['column_default']}" if col["column_default"] else ""
                length = f"({col['character_maximum_length']})" if col["character_maximum_length"] else ""
                precision = f"({col['numeric_precision']},{col['numeric_scale']})" if col["numeric_precision"] else ""
                
                response += f"  {col['ordinal_position']}. {col['column_name']} - {col['data_type']}{length}{precision} {nullable}{default}\n"
            
            if schema["primary_keys"]:
                response += f"\nPRIMARY KEYS: {', '.join(schema['primary_keys'])}\n"
            
            if schema["foreign_keys"]:
                response += "\nFOREIGN KEYS:\n"
                for fk in schema["foreign_keys"]:
                    response += f"  - {fk['column_name']} -> {fk['referenced_table']}.{fk['referenced_column']}\n"
            
            if schema["indexes"]:
                response += "\nINDEXES:\n"
                for idx in schema["indexes"]:
                    unique = " (UNIQUE)" if idx.get("is_unique") else ""
                    response += f"  - {idx.get('index_name', 'Unknown')}{unique}\n"
            
            if include_samples:
                try:
                    sample_query = f"SELECT * FROM {table_name} LIMIT {sample_limit}"
                    sample_data = current_db_connection.execute_query(sample_query)
                    
                    response += f"\nSAMPLE DATA ({len(sample_data)} rows):\n"
                    if sample_data:
                        # Show column headers
                        columns = list(sample_data[0].keys())
                        response += "  " + " | ".join(columns) + "\n"
                        response += "  " + "-" * (len(" | ".join(columns)) + 2) + "\n"
                        
                        # Show data
                        for row in sample_data:
                            values = [str(row.get(col, "NULL"))[:20] for col in columns]
                            response += "  " + " | ".join(values) + "\n"
                    else:
                        response += "  No data found in table\n"
                        
                except Exception as e:
                    response += f"\nSample data unavailable: {str(e)}\n"
            
            return [types.TextContent(type="text", text=response)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error retrieving table info: {str(e)}")]
    
    elif name == "generate_sql_query":
        if not current_db_connection:
            return [types.TextContent(type="text", text="No database connection. Use 'connect_database' first.")]
        
        query_type = arguments.get("query_type")
        table_name = arguments.get("table_name")
        
        try:
            query_generator = SQLQueryGenerator(current_db_connection)
            
            if query_type == "select":
                query = query_generator.generate_select_query(
                    table_name,
                    columns=arguments.get("columns"),
                    where_conditions=arguments.get("where_conditions"),
                    order_by=arguments.get("order_by"),
                    limit=arguments.get("limit")
                )
                
            elif query_type == "insert":
                data = arguments.get("data")
                if not data:
                    return [types.TextContent(type="text", text="Data is required for INSERT queries")]
                query = query_generator.generate_insert_query(table_name, data)
                
            elif query_type == "update":
                data = arguments.get("data")
                where_conditions = arguments.get("where_conditions")
                if not data or not where_conditions:
                    return [types.TextContent(type="text", text="Both data and where_conditions are required for UPDATE queries")]
                query = query_generator.generate_update_query(table_name, data, where_conditions)
                
            elif query_type == "delete":
                where_conditions = arguments.get("where_conditions")
                if not where_conditions:
                    return [types.TextContent(type="text", text="where_conditions are required for DELETE queries")]
                query = query_generator.generate_delete_query(table_name, where_conditions)
            
            response = f"""Generated {query_type.upper()} Query:

```sql
{query}
```

Query is optimized for {current_db_connection.db_type} database.
Use 'execute_query' to run this query.
Use 'explain_query' to see execution plan.
"""
            
            return [types.TextContent(type="text", text=response)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error generating query: {str(e)}")]
    
    elif name == "suggest_joins":
        if not current_db_connection:
            return [types.TextContent(type="text", text="No database connection. Use 'connect_database' first.")]
        
        primary_table = arguments.get("primary_table")
        related_tables = arguments.get("related_tables")
        
        try:
            query_generator = SQLQueryGenerator(current_db_connection)
            join_suggestions = query_generator.suggest_joins(primary_table, related_tables)
            
            response = f"JOIN Query Suggestions for table '{primary_table}':\n\n"
            
            for i, suggestion in enumerate(join_suggestions, 1):
                response += f"{i}. {suggestion}\n\n"
            
            return [types.TextContent(type="text", text=response)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error generating JOIN suggestions: {str(e)}")]
    
    elif name == "execute_query":
        if not current_db_connection:
            return [types.TextContent(type="text", text="No database connection. Use 'connect_database' first.")]
        
        query = arguments.get("query")
        limit_results = arguments.get("limit_results", 100)
        
        try:
            # Add LIMIT if not present and it's a SELECT query
            if query.strip().upper().startswith("SELECT") and "LIMIT" not in query.upper():
                if current_db_connection.db_type in ["postgresql", "mysql", "sqlite"]:
                    query += f" LIMIT {limit_results}"
                elif current_db_connection.db_type == "sqlserver":
                    query = query.replace("SELECT", f"SELECT TOP {limit_results}", 1)
            
            results = current_db_connection.execute_query(query)
            
            if not results:
                return [types.TextContent(type="text", text="Query executed successfully. No results returned.")]
            
            response = f"Query Results ({len(results)} rows):\n\n"
            
            # Show column headers
            if results:
                columns = list(results[0].keys())
                response += "| " + " | ".join(columns) + " |\n"
                response += "|" + "|".join(["-" * (len(col) + 2) for col in columns]) + "|\n"
                
                # Show data
                for row in results[:50]:  # Limit display to 50 rows
                    values = [str(row.get(col, "NULL")) for col in columns]
                    response += "| " + " | ".join(values) + " |\n"
                
                if len(results) > 50:
                    response += f"\n... and {len(results) - 50} more rows\n"
            
            return [types.TextContent(type="text", text=response)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error executing query: {str(e)}")]
    
    elif name == "explain_query":
        if not current_db_connection:
            return [types.TextContent(type="text", text="No database connection. Use 'connect_database' first.")]
        
        query = arguments.get("query")
        
        try:
            # Generate EXPLAIN query based on database type
            if current_db_connection.db_type == "postgresql":
                explain_query = f"EXPLAIN ANALYZE {query}"
            elif current_db_connection.db_type == "mysql":
                explain_query = f"EXPLAIN {query}"
            elif current_db_connection.db_type == "sqlite":
                explain_query = f"EXPLAIN QUERY PLAN {query}"
            elif current_db_connection.db_type == "sqlserver":
                explain_query = f"SET SHOWPLAN_ALL ON; {query}"
            
            results = current_db_connection.execute_query(explain_query)
            
            response = f"Query Execution Plan:\n\n"
            
            if results:
                for row in results:
                    for key, value in row.items():
                        response += f"{key}: {value}\n"
                    response += "\n"
            else:
                response += "No execution plan information available.\n"
            
            return [types.TextContent(type="text", text=response)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error explaining query: {str(e)}")]
    
    else:
        raise ValueError(f"Unknown tool: {name}")

async def main():
    from mcp.server.stdio import stdio_server
    
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream, 
            write_stream, 
            InitializationOptions(
                server_name="database-schema-mcp-server",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={}
                )
            )
        )

if __name__ == "__main__":
    asyncio.run(main())