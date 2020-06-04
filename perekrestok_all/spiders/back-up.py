# -*- coding: utf-8 -*-
import datetime
import scrapy
import json
from scrapy.selector import Selector
from scrapy.exceptions import CloseSpider
import logging
import time
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class ProductsAllSpider(CrawlSpider):
    name = 'products_all_back-up'
    allowed_domains = ['www.perekrestok.ru']
    start_urls = [
        # 'https://www.perekrestok.ru/catalog/ovoschi-frukty-griby/',
        # 'https://www.perekrestok.ru/catalog/ryba-i-moreprodukty/',
        'https://www.perekrestok.ru/catalog/posuda'
    ]

    rules = [Rule(LinkExtractor(
        # allow=,
        deny='(/reviews)',
        restrict_xpaths=(
            "//main[@class = 'xf-products-section__products']",
            "//div[@class='xf-paginator__items']"
        )
    ), callback='parse_product', follow=True)]


    def parse_product(self, response):
        exists = response.xpath("//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-product-id").get()
        if exists:
            product_id = response.xpath("//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-product-id").get()
            category_id = response.xpath("//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-category-id").get()
            category_name = response.xpath(
                "//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-category-name").get()
            product_name = response.xpath("//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-product-name").get()
            vendor = response.xpath("//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-product-vendor-name").get()
            vendor_id = response.xpath(
                "//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-product-vendor-id").get()
            country = response.xpath("normalize-space(//td[@class = 'xf-product-table__col']/a[1]/text())").get()
            regular_price = response.xpath("//@data-cost").get()
            sale_price = response.xpath("//@data-quantum-cost").get()
            unit = response.xpath("(//span[@class = 'js-fraction-text']/text())[1]").get()
            availability = response.xpath("//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-is-available").get()
            link = response.xpath("//link[@rel='canonical']/@href").get()

            yield {
                'date': datetime.datetime.now(),
                'product_id': product_id,
                'category_id': category_id,
                'category_name': category_name,
                'product_name': product_name,
                'vendor': vendor,
                'vendor_id': vendor_id,
                'country': country,
                'regular_price': regular_price,
                'sale_price': sale_price,
                'unit': unit,
                'availability': availability,
                'link': link
            }




