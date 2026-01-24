import datetime
import decimal
import os
from typing import Any, Dict, List, Optional

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def _db_config() -> Dict[str, Any]:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME", "mydb"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
    }


def _get_connection():
    conn = psycopg2.connect(**_db_config())
    conn.set_session(readonly=True, autocommit=True)
    return conn


def _is_readonly_sql(sql: str) -> bool:
    normalized = " ".join(sql.strip().strip(";").split()).lower()
    if not normalized:
        return False
    if normalized.startswith("select ") or normalized.startswith("with "):
        forbidden = (
            " insert ",
            " update ",
            " delete ",
            " drop ",
            " alter ",
            " create ",
            " truncate ",
            " grant ",
            " revoke ",
            " commit ",
            " rollback ",
        )
        return not any(token in f" {normalized} " for token in forbidden)
    return False


def _fetch_schema() -> Dict[str, Any]:
    schema: Dict[str, Any] = {"tables": {}}
    allowed_tables = {"chocolate_sales", "car_sales", "walmart_grocery_sales"}
    with _get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_schema, table_name, column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                    AND table_name = ANY(%s)
                ORDER BY table_schema, table_name, ordinal_position;
                """,
                (list(allowed_tables),),
            )
            for table_schema, table_name, column_name, data_type, is_nullable in cursor.fetchall():
                key = f"{table_schema}.{table_name}"
                table = schema["tables"].setdefault(
                    key,
                    {"columns": [], "primary_key": [], "foreign_keys": []},
                )
                table["columns"].append(
                    {
                        "name": column_name,
                        "type": data_type,
                        "nullable": is_nullable == "YES",
                    }
                )

            cursor.execute(
                """
                SELECT tc.table_schema, tc.table_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
                    AND tc.table_name = ANY(%s)
                ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position;
                """,
                (list(allowed_tables),),
            )
            for table_schema, table_name, column_name in cursor.fetchall():
                key = f"{table_schema}.{table_name}"
                table = schema["tables"].setdefault(
                    key,
                    {"columns": [], "primary_key": [], "foreign_keys": []},
                )
                table["primary_key"].append(column_name)

            cursor.execute(
                """
                SELECT tc.table_schema, tc.table_name, kcu.column_name,
                       ccu.table_schema, ccu.table_name, ccu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
                    AND tc.table_name = ANY(%s)
                ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position;
                """,
                (list(allowed_tables),),
            )
            for (
                table_schema,
                table_name,
                column_name,
                foreign_schema,
                foreign_table,
                foreign_column,
            ) in cursor.fetchall():
                key = f"{table_schema}.{table_name}"
                table = schema["tables"].setdefault(
                    key,
                    {"columns": [], "primary_key": [], "foreign_keys": []},
                )
                table["foreign_keys"].append(
                    {
                        "column": column_name,
                        "references": f"{foreign_schema}.{foreign_table}({foreign_column})",
                    }
                )
    schema["table_count"] = len(schema["tables"])
    return schema


def _format_schema(schema: Dict[str, Any]) -> str:
    lines: List[str] = []
    for table_name, table in sorted(schema.get("tables", {}).items()):
        lines.append(f"Table {table_name}:")
        for column in table.get("columns", []):
            nullable = "NULL" if column["nullable"] else "NOT NULL"
            lines.append(f"  - {column['name']} ({column['type']}, {nullable})")
        if table.get("primary_key"):
            lines.append(f"  Primary key: {', '.join(table['primary_key'])}")
        for fk in table.get("foreign_keys", []):
            lines.append(
                f"  Foreign key: {fk['column']} -> {fk['references']}"
            )
        lines.append("")
    return "\n".join(lines).strip()


def _visualization_ready_result(result: Dict[str, Any]) -> Dict[str, Any]:
    if result.get("status") != "success":
        return result
    columns = result.get("columns", [])
    rows = result.get("rows", [])
    data = [dict(zip(columns, row)) for row in rows]
    result["data"] = data
    result["metadata"] = {
        "columns": columns,
        "row_count": result.get("row_count", 0),
        "truncated": result.get("truncated", False),
    }
    return result


def get_sales_schema() -> Dict[str, Any]:
    """Return the database schema (tables, columns, primary keys, foreign keys)."""
    schema = _fetch_schema()
    return {"status": "success", "schema": schema, "schema_text": _format_schema(schema)}


def run_readonly_query(sql: str, max_rows: int = 200) -> Dict[str, Any]:
    """Execute a read-only SQL query and return rows and column metadata.

    Args:
        sql: A read-only SQL statement (SELECT or WITH). Must not modify data.
        max_rows: Maximum number of rows to return in the response.
    """
    if not _is_readonly_sql(sql):
        return {
            "status": "error",
            "error_message": "Only read-only SELECT/WITH queries are allowed.",
        }

    with _get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchmany(max_rows + 1)
            truncated = len(rows) > max_rows
            if truncated:
                rows = rows[:max_rows]
            columns = [desc[0] for desc in cursor.description or []]
            rows = [_json_safe_row(row) for row in rows]
    return _visualization_ready_result(
        {
            "status": "success",
            "sql": sql,
            "columns": columns,
            "row_count": len(rows),
            "truncated": truncated,
            "rows": rows,
        }
    )


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    return value


def _json_safe_row(row: Any) -> Any:
    if isinstance(row, (list, tuple)):
        return [_json_safe_value(value) for value in row]
    return _json_safe_value(row)


def _intent_to_sql(question: str, schema: Dict[str, Any]) -> Optional[str]:
    normalized = " ".join(question.lower().split())
    if "list tables" in normalized or "show tables" in normalized:
        return (
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY table_schema, table_name;"
        )
    if normalized.startswith("describe ") or normalized.startswith("show schema for "):
        table_name = normalized.replace("describe ", "").replace("show schema for ", "").strip()
        if table_name and "." not in table_name:
            matches = [name for name in schema.get("tables", {}) if name.endswith(f".{table_name}")]
            if len(matches) == 1:
                table_name = matches[0]
        return (
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            f"WHERE table_schema || '.' || table_name = '{table_name}' "
            "ORDER BY ordinal_position;"
        )
    if "total sales" in normalized or "sum amount" in normalized:
        return "SELECT SUM(amount) AS total_amount FROM chocolate_sales;"
    if "total boxes" in normalized or "sum boxes" in normalized:
        return "SELECT SUM(boxes_shipped) AS total_boxes FROM chocolate_sales;"
    if "row count" in normalized or "count rows" in normalized:
        for name in schema.get("tables", {}):
            if name.endswith(f".{normalized.split()[-1]}"):
                return f"SELECT COUNT(*) AS row_count FROM {name};"
    if "sample" in normalized or "example rows" in normalized:
        for name in schema.get("tables", {}):
            if name.endswith(f".{normalized.split()[-1]}"):
                return f"SELECT * FROM {name} LIMIT 5;"
    if "top" in normalized and "sales" in normalized:
        return (
            "SELECT sales_person, SUM(amount) AS total_amount "
            "FROM chocolate_sales "
            "GROUP BY sales_person "
            "ORDER BY total_amount DESC LIMIT 10;"
        )
    if "sales by country" in normalized:
        return (
            "SELECT country, SUM(amount) AS total_amount "
            "FROM chocolate_sales "
            "GROUP BY country "
            "ORDER BY total_amount DESC;"
        )
    if "sales by product" in normalized:
        return (
            "SELECT product, SUM(amount) AS total_amount "
            "FROM chocolate_sales "
            "GROUP BY product "
            "ORDER BY total_amount DESC;"
        )
    if "sales by month" in normalized or "monthly sales" in normalized:
        return (
            "SELECT DATE_TRUNC('month', date) AS month, SUM(amount) AS total_amount "
            "FROM chocolate_sales "
            "GROUP BY month "
            "ORDER BY month;"
        )
    return None


def query_sales(question: str, sql: Optional[str] = None, max_rows: int = 200) -> Dict[str, Any]:
    """Answer a user question by reading schema, generating SQL, and querying.

    Args:
        question: Natural language question about the database.
        sql: Optional SQL to run. If omitted, simple intent patterns are applied.
        max_rows: Maximum number of rows to return.
    """
    schema = _fetch_schema()
    schema_text = _format_schema(schema)
    query = sql or _intent_to_sql(question, schema)
    if not query:
        return {
            "status": "needs_sql",
            "error_message": "Provide SQL for this request. Use the schema to craft a read-only query.",
            "schema_text": schema_text,
        }
    result = run_readonly_query(query, max_rows=max_rows)
    result["schema_text"] = schema_text
    result["generated_sql"] = sql is None
    return result
