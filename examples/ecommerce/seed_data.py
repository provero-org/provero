#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Seed sample e-commerce data into a DuckDB database for the example."""

from __future__ import annotations

import duckdb


def seed() -> None:
    conn = duckdb.connect("ecommerce.duckdb")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            email VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            description VARCHAR,
            price DECIMAL(10, 2) NOT NULL,
            category VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
            order_date TIMESTAMP NOT NULL,
            status VARCHAR NOT NULL,
            total_amount DECIMAL(10, 2) NOT NULL,
            shipping_address VARCHAR
        )
    """)

    # Insert sample customers
    conn.execute("""
        INSERT INTO customers VALUES
        (1, 'alice@example.com', 'Alice Smith', '2024-01-15 10:00:00'),
        (2, 'bob@example.com', 'Bob Jones', '2024-02-20 14:30:00'),
        (3, 'carol@example.com', 'Carol White', '2024-03-10 09:15:00'),
        (4, 'dave@example.com', 'Dave Brown', '2024-04-05 16:45:00'),
        (5, 'eve@example.com', 'Eve Davis', '2024-05-01 11:00:00')
        ON CONFLICT DO NOTHING
    """)

    # Insert sample products
    conn.execute("""
        INSERT INTO products VALUES
        (1, 'Laptop', 'High-performance laptop', 999.99, 'electronics'),
        (2, 'Mouse', 'Wireless mouse', 29.99, 'electronics'),
        (3, 'Desk', NULL, 299.99, 'furniture'),
        (4, 'Chair', 'Ergonomic office chair', 449.99, 'furniture'),
        (5, 'Monitor', '4K display', 599.99, 'electronics')
        ON CONFLICT DO NOTHING
    """)

    # Insert sample orders (with current timestamps for freshness checks)
    conn.execute("""
        INSERT INTO orders VALUES
        (1, 1, CURRENT_TIMESTAMP - INTERVAL '2 hours', 'delivered', 999.99, '123 Main St'),
        (2, 2, CURRENT_TIMESTAMP - INTERVAL '5 hours', 'shipped', 329.98, '456 Oak Ave'),
        (3, 3, CURRENT_TIMESTAMP - INTERVAL '1 hour', 'confirmed', 29.99, NULL),
        (4, 1, CURRENT_TIMESTAMP - INTERVAL '30 minutes', 'pending', 449.99, '123 Main St'),
        (5, 4, CURRENT_TIMESTAMP - INTERVAL '3 hours', 'delivered', 1599.98, '789 Pine Rd'),
        (6, 5, CURRENT_TIMESTAMP - INTERVAL '45 minutes', 'pending', 599.99, '321 Elm Dr'),
        (7, 2, CURRENT_TIMESTAMP - INTERVAL '6 hours', 'cancelled', 29.99, '456 Oak Ave'),
        (8, 3, CURRENT_TIMESTAMP - INTERVAL '15 minutes', 'confirmed', 749.98, '555 Maple Ln'),
        (9, 4, CURRENT_TIMESTAMP - INTERVAL '4 hours', 'shipped', 299.99, '789 Pine Rd'),
        (10, 5, CURRENT_TIMESTAMP - INTERVAL '10 minutes', 'pending', 1049.98, '321 Elm Dr')
        ON CONFLICT DO NOTHING
    """)

    conn.close()
    print("Seeded ecommerce.duckdb with sample data.")
    print("Run: assay run -c assay.yaml")


if __name__ == "__main__":
    seed()
