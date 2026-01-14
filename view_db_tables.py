"""
Скрипт для просмотра таблиц и структуры базы данных PostgreSQL.
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import argparse

# Устанавливаем UTF-8 для вывода (для Windows)
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

load_dotenv()


def get_db_connection():
    """
    Получение подключения к БД.

    Returns:
        Подключение к PostgreSQL
    """
    db_url = os.getenv('DB_URL')
    if not db_url:
        raise ValueError("DB_URL not set in environment")

    return psycopg2.connect(db_url)


def list_tables(conn):
    """
    Получение списка всех таблиц в БД.

    Args:
        conn: Подключение к БД

    Returns:
        Список таблиц
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    return tables


def describe_table(conn, table_name: str):
    """
    Получение структуры таблицы.

    Args:
        conn: Подключение к БД
        table_name: Имя таблицы

    Returns:
        Список колонок с информацией
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    
    columns = cur.fetchall()
    cur.close()
    return columns


def get_table_row_count(conn, table_name: str):
    """
    Получение количества строк в таблице.

    Args:
        conn: Подключение к БД
        table_name: Имя таблицы

    Returns:
        Количество строк
    """
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cur.fetchone()[0]
    cur.close()
    return count


def show_table_data(conn, table_name: str, limit: int = 10):
    """
    Показать данные из таблицы.

    Args:
        conn: Подключение к БД
        table_name: Имя таблицы
        limit: Максимальное количество строк
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    rows = cur.fetchall()
    cur.close()
    return rows


def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(
        description="View database tables and structure"
    )
    parser.add_argument(
        "--table",
        type=str,
        help="Show structure and data for specific table"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Limit number of rows to show (default: 10)"
    )
    parser.add_argument(
        "--count-only",
        action="store_true",
        help="Show only row counts"
    )

    args = parser.parse_args()

    try:
        conn = get_db_connection()
        
        if args.table:
            # Показать информацию о конкретной таблице
            print("=" * 60)
            print(f"Table: {args.table}")
            print("=" * 60)
            
            # Структура таблицы
            columns = describe_table(conn, args.table)
            print("\nStructure:")
            print("-" * 60)
            print(f"{'Column':<30} {'Type':<20} {'Nullable':<10} {'Default'}")
            print("-" * 60)
            for col in columns:
                col_name, data_type, max_length, nullable, default = col
                type_str = data_type
                if max_length:
                    type_str += f"({max_length})"
                default_str = str(default) if default else ""
                print(f"{col_name:<30} {type_str:<20} {nullable:<10} {default_str}")
            
            # Количество строк
            count = get_table_row_count(conn, args.table)
            print(f"\nRow count: {count}")
            
            # Данные (если не только счетчик)
            if not args.count_only and count > 0:
                print(f"\nFirst {args.limit} rows:")
                print("-" * 60)
                rows = show_table_data(conn, args.table, args.limit)
                if rows:
                    # Показываем заголовки
                    headers = list(rows[0].keys())
                    print(" | ".join(headers))
                    print("-" * 60)
                    # Показываем данные
                    for row in rows:
                        values = []
                        for h in headers:
                            val = str(row[h]) if row[h] is not None else ""
                            # Обрезаем длинные значения для лучшего отображения
                            if len(val) > 40:
                                val = val[:37] + "..."
                            values.append(val)
                        # Используем разделитель для лучшей читаемости
                        print(" | ".join(values))
        else:
            # Показать список всех таблиц
            print("=" * 60)
            print("Database Tables")
            print("=" * 60)
            
            tables = list_tables(conn)
            if not tables:
                print("No tables found.")
                return
            
            print(f"\nFound {len(tables)} table(s):\n")
            
            if args.count_only:
                # Показать только количество строк
                print(f"{'Table':<30} {'Rows'}")
                print("-" * 40)
                for table in tables:
                    try:
                        count = get_table_row_count(conn, table)
                        print(f"{table:<30} {count}")
                    except Exception as e:
                        print(f"{table:<30} Error: {e}")
            else:
                # Показать таблицы с количеством строк и структурой
                for table in tables:
                    print(f"\n{table}")
                    print("-" * 60)
                    try:
                        count = get_table_row_count(conn, table)
                        print(f"Rows: {count}")
                        
                        columns = describe_table(conn, table)
                        print("Columns:")
                        for col in columns:
                            col_name, data_type, max_length, nullable, default = col
                            type_str = data_type
                            if max_length:
                                type_str += f"({max_length})"
                            nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                            default_str = f" DEFAULT {default}" if default else ""
                            print(f"  - {col_name}: {type_str} {nullable_str}{default_str}")
                    except Exception as e:
                        print(f"Error: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
