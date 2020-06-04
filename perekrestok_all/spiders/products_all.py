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
        # 'https://www.perekrestok.ru/catalog/ovoschi-frukty-griby/',
        # 'https://www.perekrestok.ru/catalog/ryba-i-moreprodukty/',
        # 'https://www.perekrestok.ru/catalog/posuda',
        'https://www.perekrestok.ru/catalog'
    ]

    rules = [Rule(LinkExtractor(
        # allow=,
        deny='(/reviews)',
        restrict_xpaths=(
            "//main[@class = 'xf-products-section__products']",
            "//div[@class = 'xf-paginator__items']",
            "//ul[@class = 'xf-catalog-categories__list _grid']"
        )
    ), callback='parse_product', follow=True)]


    def parse_product(self, response):

        exists = response.xpath("//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-product-id").get()
        if exists:
            loader = ItemLoader(item=PerekrestokAllItem(), selector=response)
            loader.add_xpath('product_id', "//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-product-id")
            loader.add_xpath("category_id", "//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-category-id")
            loader.add_xpath("category_name", "//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-category-name")
            loader.add_xpath("product_name", "//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-product-name")
            loader.add_xpath("vendor", "//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-product-vendor-name")
            loader.add_xpath("vendor_id", "//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-product-vendor-id")
            loader.add_xpath("country", "//*[contains(text(),'Страна')]/following-sibling::*/a/text()")
            loader.add_xpath("regular_price", "(//@data-cost)[1]")
            loader.add_xpath("regular_price", "(//@data-cost)[last()]")
            loader.add_xpath("sale_price", "(//@data-cost)[last()]")
            loader.add_xpath("unit", "(//span[@class = 'js-fraction-text']/text())[1]")
            loader.add_xpath("availability", "//section[@class = 'xf-wrapper js-card-page']/div/@data-gtm-is-available")
            loader.add_xpath("link", "//link[@rel='canonical']/@href")
            loader.add_xpath("second_level_cat", "//ul[@itemscope='itemscope']/li[4]/a/span/text()")
            loader.add_xpath("first_level_cat", "//ul[@itemscope='itemscope']/li[3]/a/span/text()")
            loader.add_value("date_time", datetime.now())

            yield loader.load_item()




