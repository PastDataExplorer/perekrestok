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

CREATE_PRODUCTS_TABLE = """CREATE TABLE IF NOT EXISTS products(
    product_id INT PRIMARY KEY,
    product_name TEXT,
    first_level_cat TEXT,
    second_level_cat TEXT,
    category_id INT,
    category_name TEXT,
    vendor TEXT,
    vendor_id INT,
    country TEXT,
    unit TEXT,
    link TEXT
);"""

CREATE_PRICES_TABLE = """CREATE TABLE IF NOT EXISTS prices(
    timestamp INT, 
    product_id INT PRIMARY KEY,
    regular_price NUMERIC(10, 2),
    sale_price NUMERIC(10, 2),
    availability BOOLEAN,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);"""

INSERT_PRODUCTS = """INSERT INTO products
    (product_id, 
    product_name,
    first_level_cat,
    second_level_cat,
    category_id,
    category_name,
    vendor,
    vendor_id,
    country,
    unit,
    link)

    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

INSERT_PRICES = """INSERT INTO prices
    (timestamp, 
    product_id,
    regular_price,
    sale_price,
    availability)

    VALUES (%s, %s, %s, %s, %s);"""

class PostgresqlPipeline(object):

    def open_spider(self, spider):
        self.connection = psycopg2.connect(os.environ['DATABASE_URL'])

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(CREATE_PRODUCTS_TABLE)
                cursor.execute(CREATE_PRICES_TABLE)
            
    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(INSERT_PRODUCTS,
                    (item.get('product_id'),
                    item.get('product_name'),
                    item.get('first_level_cat'),
                    item.get('second_level_cat'),
                    item.get('category_id'),
                    item.get('category_name'),
                    item.get('vendor'),
                    item.get('vendor_id'),
                    item.get('country'),
                    item.get('unit'),
                    item.get('link')))
                
                cursor.execute(INSERT_PRICES,
                    (item.get('timestamp'),
                    item.get('product_id'),
                    item.get('regular_price'),
                    item.get('sale_price'),
                    item.get('availability'))
                )
        return item
