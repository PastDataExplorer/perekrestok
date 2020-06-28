# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import logging
import psycopg2

from dotenv import load_dotenv

load_dotenv()

# --- CREATE ----
CREATE_CATEGORIES_TABLE = """CREATE TABLE IF NOT EXISTS categories(
    category_id INT PRIMARY KEY,
    category_name TEXT,
    first_level_cat TEXT,
    second_level_cat TEXT
);"""

CREATE_VENDORS_TABLE = """CREATE TABLE IF NOT EXISTS vendors(
--    vendor_key SERIAL PRIMARY KEY,
    vendor_id INT PRIMARY KEY,
    vendor TEXT
);"""

CREATE_PRODUCTS_TABLE = """CREATE TABLE IF NOT EXISTS products(
    product_id INT PRIMARY KEY,
    category_id INT REFERENCES categories(category_id),
    product_name TEXT,
    vendor_id INT REFERENCES vendors(vendor_id),
    country TEXT,
    unit TEXT,
    link TEXT
);"""

CREATE_PRICES_TABLE = """CREATE TABLE IF NOT EXISTS prices(
    date_time DATE DEFAULT CURRENT_DATE, 
    product_id INT,
    regular_price NUMERIC(10, 2),
    sale_price NUMERIC(10, 2),
    availability BOOLEAN,
    UNIQUE (date_time, product_id),
    CONSTRAINT date_product_id PRIMARY KEY (date_time, product_id)
--    FOREIGN KEY (product_id) REFERENCES products(product_id)
);"""

# --- INSERT ----
INSERT_CATEGORIES = """INSERT INTO categories
        (category_id,
        category_name,
        first_level_cat,
        second_level_cat)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT(category_id)
    DO NOTHING
--    DO UPDATE SET
--        category_name = categories.category_name,
--        first_level_cat = categories.first_level_cat,
--        second_level_cat = categories.second_level_cat
    ;"""

INSERT_VENDORS = """INSERT INTO vendors
        (vendor_id,
        vendor)
    VALUES (%s, %s)
    ON CONFLICT(vendor_id)
    DO NOTHING
--    DO UPDATE SET
--        vendor = vendors.vendor
    ;"""

INSERT_PRODUCTS = """INSERT INTO products
        (product_id,
        category_id, 
        product_name,
        vendor_id,
        country,
        unit,
        link)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(product_id)
    DO UPDATE SET
        category_id = products.category_id,
        product_name = products.product_name,
        vendor_id = products.vendor_id,
        country = products.country,
        unit = products.unit,
        link = products.link
    ;"""

INSERT_PRICES = """INSERT INTO prices
        (date_time, 
        product_id,
        regular_price,
        sale_price,
        availability)
    VALUES (NOW(), %s, %s, %s, %s)
    ON CONFLICT(date_time, product_id)
    DO NOTHING
--    DO UPDATE SET
--        regular_price = prices.regular_price,
--        sale_price = prices.sale_price,
--        availability = prices.availability
    ;"""




class PostgresqlPipeline(object):

    def open_spider(self, spider):
        self.connection = psycopg2.connect(os.environ['DATABASE_URL'])

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(CREATE_CATEGORIES_TABLE)
                cursor.execute(CREATE_VENDORS_TABLE)
                cursor.execute(CREATE_PRODUCTS_TABLE)
                cursor.execute(CREATE_PRICES_TABLE)
            
    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(INSERT_CATEGORIES,
                    (item.get('category_id'),
                    item.get('category_name'),
                    item.get('first_level_cat'),
                    item.get('second_level_cat'))
                )

                if item.get('vendor_id'):
                    cursor.execute(INSERT_VENDORS,
                        (item.get('vendor_id'),
                        item.get('vendor'))
                    )

                cursor.execute(INSERT_PRODUCTS,
                    (item.get('product_id'),
                    item.get('category_id'),
                    item.get('product_name'),
                    item.get('vendor_id'),
                    item.get('country'),
                    item.get('unit'),
                    item.get('link')))
                
                cursor.execute(INSERT_PRICES,
                    (
                    item.get('product_id'),
                    item.get('regular_price'),
                    item.get('sale_price'),
                    item.get('availability'))
                )

        return item