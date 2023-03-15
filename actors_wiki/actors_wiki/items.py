# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class CastItem(scrapy.Item):
    film = scrapy.Field()
    actor_name = scrapy.Field()
