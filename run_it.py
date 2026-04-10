#!/usr/bin/env python3
"""
Simple script to execute WFS dialect queries with parameterized URL and SQL.
"""
from superset_wfs_dialect import connect
import argparse


def execute_wfs_query(wfs_url, sql_query, username=None, password=None):
  connection = connect(base_url=wfs_url, username=username, password=password)
  cursor = connection.cursor()
  cursor.execute(sql_query)
  results = cursor.fetchall()
  print(f'number of features: {len(results)}')


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Execute SQL queries against a WFS service"
    )
    parser.add_argument(
        "--url",
        required=True,
        help="WFS service URL (e.g., 'example.com/geoserver/ows')"
    )
    parser.add_argument(
        "--sql",
        required=True,
        help="SQL query to execute (e.g., 'SELECT * FROM my_layer LIMIT 10')"
    )
    parser.add_argument(
        "--username",
        help="Username for BasicAuth (optional)"
    )
    parser.add_argument(
        "--password",
        help="Password for BasicAuth (optional)"
    )
    
    args = parser.parse_args()
    
    # Execute the query
    execute_wfs_query(
        wfs_url=args.url,
        sql_query=args.sql,
        username=args.username,
        password=args.password
    )


if __name__ == "__main__":
    main()
