from __future__ import print_function

from time import sleep

import scrapy

from actors_wiki.items import CastItem


# Try it for a different set of years (and therefore a different initial film)
class Actorswiki(scrapy.Spider):
    """
    This class is used to scrape the list of films by year and scrape
    each film's wikipedia page for the list of starring actors.
    """
    name = 'actors_wiki_spider'
    # allowed_domains = ["en.wikipedia.org/"]
    def start_requests(self):
        for num in range(2022, 2023):
            movie_by_year = "https://en.wikipedia.org/wiki/List_of_American_films_of_{}".format(num)
            yield scrapy.Request(url=movie_by_year, callback=self.parse_films)


# When on a film's page, use the following xpath to get the cast names (under Starring):
# response.xpath('//tr[contains(th, "Starring")]/td/div/ul/li/text()').getall()
# response.xpath('//tr[contains(th, "Starring")]/td/div/ul/li/a/text()').getall()
# and this xpath to get the anchor tags
# response.xpath('//tr[contains(th, "Starring")]/td/div/ul/li/a/@href').getall().
# Use this xpath to get the name of the film:
# response.xpath('//h1[@id="firstHeading"]/i/text()').getall()

# When on an actor's page, use the following xpath to get the names of the films they have
# starred in with hyperlinks
# response.xpath('//div/h3[contains(span, "Film")]/following-sibling::table[1]/tbody/tr/td/i/a/text()').getall()
# use this xpath to get the years
# response.xpath('//div/h3[contains(span, "Film")]/following-sibling::table[1]/tbody/tr[td[2]/i/a]/td[1]/text()').getall()
# (remember that the years have a '/n' following, so that needs to be stripped
# and use this xpath to get the anchor tags
# response.xpath('//div/h3[contains(span, "Film")]/following-sibling::table[1]/tbody/tr/td/i/a/@href').getall()

    def parse_films(self, response):
        """
        Scrape the list of films in each page of films by year to get the link to the film page.

        Args:
             response (scrapy Response object): This is scrapy's representation pf the
                HTTP response object arising from the request for one of the List of American
                films of (year) pages.
        """
        months = ["January", "April", "July", "October"]
        for month in months:
            table = response.xpath('//div/h2[contains(span[2], "'+month+'")]/following-sibling::table[1]/tbody/tr')
            for row in table:
                sleep(2)
                href = row.xpath('td/i/a/@href').get()
                yield scrapy.Request(url=response.urljoin(href), callback=self.parse_cast)


    def parse_cast(self, response):
        """
        Scrape for the cast list of the film.
        """
        film = response.xpath('//h1[@id="firstHeading"]/i/text()').get()
        cast_table = response.xpath('//tr[contains(th, "Starring")]/td/div/ul')
        for item in cast_table.xpath('li'):
            sleep(2)
            c_item = CastItem()
            c_item["film"] = film
            if item.xpath('a/text()').get():
                c_item["actor_name"] = item.xpath('a/text()').get()
            else:
                c_item["actor_name"] = item.xpath('text()').get()
            yield c_item




