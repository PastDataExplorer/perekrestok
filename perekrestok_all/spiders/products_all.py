# -*- coding: utf-8 -*-
from datetime import datetime

from scrapy.loader import ItemLoader
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import PerekrestokAllItem


class ProductsAllSpider(CrawlSpider):
    name = 'products_all'
    allowed_domains = ['www.perekrestok.ru']
    start_urls = [
        'https://www.perekrestok.ru/catalog'
    ]

    rules = [Rule(LinkExtractor(
        deny='(/reviews)',
        #need to restrict xpaths in order to make requests to products web-pages
        restrict_xpaths = [
            "//div[@data-id]/div[@class='xf-product__picture xf-product-picture']",
            "//li[@class='xf-catalog-categories__item']/a",
            "//div[@class='xf-paginator__items']/a"
        ]
    ), callback='parse_product', follow=True)]


    def parse_product(self, response):

        #first check if dataframe exists. It helps to skip web-pages which do not contain product information
        exists = response.xpath("//div[@id='main-app']/div[@itemtype]/div/@data-id").get()
        if exists:
            loader = ItemLoader(item=PerekrestokAllItem(), selector=response)
            loader.add_xpath('product_id', "//div[@id='main-app']/div[@itemtype]/div/@data-id")
            loader.add_xpath("category_id", "//div[@id='main-app']/div[@itemtype]/div/@data-gtm-category-id")
            loader.add_xpath("category_name", "//div[@id='main-app']/div[@itemtype]/div/@data-gtm-category-name")
            loader.add_xpath("product_name", "//div[@id='main-app']/div[@itemtype]/div/@data-gtm-product-name")
            loader.add_xpath("vendor", "//div[@id='main-app']/div[@itemtype]/div/@data-gtm-product-vendor-name")
            loader.add_xpath("vendor_id", "//div[@id='main-app']/div[@itemtype]/div/@data-gtm-product-vendor-id")
            loader.add_xpath("country", "//*[contains(text(),'Страна')]/following-sibling::*/a/text()")
            loader.add_xpath("regular_price", "(//@data-cost)[1]")
            loader.add_xpath("regular_price", "(//@data-cost)[last()]")
            loader.add_xpath("sale_price", "(//@data-cost)[1]")
            loader.add_xpath("unit", "(//span[@class = 'js-fraction-text']/text())[1]")
            loader.add_xpath("availability", "//div[@id='main-app']/div[@itemtype]/div/@data-gtm-is-available")
            loader.add_xpath("link", "//link[@rel='canonical']/@href")
            loader.add_xpath("second_level_cat", "//ul[@itemscope='itemscope']/li[4]/a/span/text()")
            loader.add_xpath("first_level_cat", "//ul[@itemscope='itemscope']/li[3]/a/span/text()")
            loader.add_value("date_time", datetime.now())

            yield loader.load_item()




