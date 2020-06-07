# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader.processors import TakeFirst, MapCompose, Join, Compose
import datetime
from scrapy.selector import Selector
from w3lib.html import remove_tags

# How MapCopmpose and Compose works
# from scrapy.loader.processors import Compose, MapCompose
# proc = Compose(lambda v: v[0], str.upper)
# proc(['hello', 'world']) # HELLO
# mproc = MapCompose(lambda v: v[0], str.upper)
# mproc(['hello', 'world']) # ['H', 'W']

def to_float(number):
    if number:
        return float(number)
    else:
        return None

def to_int(number):
    if number:
        return int(number)
    else:
        return None
def to_bool(number):
    if number == 1:
        return True
    else:
        return False

class PerekrestokAllItem(scrapy.Item):
    date_time = scrapy.Field(
        output_processor=TakeFirst()
    )
    product_id = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    category_id = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    category_name = scrapy.Field(
        output_processor=TakeFirst()
    )
    product_name = scrapy.Field(
        output_processor=TakeFirst()
    )    
    vendor = scrapy.Field(
        output_processor=TakeFirst()
    )
    vendor_id = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    country = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=Join()
    )
    regular_price = scrapy.Field(
        input_processor=MapCompose(to_float),
        output_processor=Compose(lambda x: max(x))
        #Compose applies finction to the whole list, while MapCompose apply finction to each element in the list
    )
    sale_price = scrapy.Field(
        input_processor=MapCompose(to_float),
        output_processor=Compose(lambda x: max(x))
    )
    unit = scrapy.Field(
        output_processor=TakeFirst()
    )
    availability = scrapy.Field(
        input_processor=MapCompose(to_bool),
        output_processor=TakeFirst()
    )
    link = scrapy.Field(
        output_processor=TakeFirst()
    )
    second_level_cat = scrapy.Field(
        output_processor=TakeFirst()
    )
    first_level_cat = scrapy.Field(
        output_processor=TakeFirst()
    )
