# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CastItem(scrapy.Item):
    film = scrapy.Field()
    actor_name = scrapy.Field()


class DirectorItem(scrapy.Item):
    film = scrapy.Field()
    director = scrapy.Field()


class MovieItem(scrapy.Item):
    film = scrapy.Field()
    budget = scrapy.Field()
    box_office = scrapy.Field()
    release_date = scrapy.Field()


class ProductionCoItem(scrapy.Item):
    film = scrapy.Field()
    prod_co = scrapy.Field()
