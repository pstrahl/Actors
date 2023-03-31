from __future__ import print_function

import logging
from time import sleep

import scrapy
from scrapy.utils.log import configure_logging

from actors_wiki.items import CastItem, DirectorItem, DistributorItem, MovieItem, ProductionCoItem

class Actorswiki(scrapy.Spider):
    """
    This class is used to scrape the list of films by year and subsequently scrape
    each film's wikipedia page for the list of starring actors, director(s), distributor(s),
    production companies, budget, box office, and release date.
    """

    name = 'actors_wiki_spider'
    # allowed_domains = ["en.wikipedia.org/"]

    def start_requests(self):
        for num in range(2018, 2023):
            movie_by_year = "https://en.wikipedia.org/wiki/List_of_American_films_of_{}".format(num)
            yield scrapy.Request(url=movie_by_year, callback=self.parse_list)

    def parse_list(self, response):
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
                yield scrapy.Request(url=response.urljoin(href), callback=self.parse_films)

    def parse_films(self, response):
        """
        Get the cast list, director(s), production companies, budget, box office, and release date for the film.

        First, we produce a MovieItem with fields 'film', 'budget', 'box_office', and 'release_date'.
        As outlined below, there are several cases to consider for extracting the release date:
        1) it is in a list of release dates, and we want the US release date, 2) there is a single
        Release date listed, or 3) it is listed as the Original air date.

        Next, we use build_items() to produce: DirectorItems with fields 'film', 'director',
         ProductionCoItem with fields 'film' and 'prod_co' and CastItems with
        fields 'film' and 'actor_name'.

        Args:
            response (scrapy Response object): This is scrapy's representation of the
                HTTP response object arising from the request for one of the film pages.
        """
        # Construct a MovieItem
        film = response.xpath('//h1[@id="firstHeading"]/i/text()').get()
        if not film:
            film = response.xpath('//h1[@id="firstHeading"]/span/text()').get()
        budget = response.xpath('//tr[contains(th, "Budget")]/td/text()').get()
        box_office = response.xpath('//tr[contains(th, "Box office")]/td/text()').get()

        row = response.xpath('//tr[contains(th/div, "date") or contains(th, "date")]/td')
        release_date = row.xpath('//li[text()[contains(., "States") or contains(., "US")]]/span/span/text()').get()
        if not release_date:
            release_date = row.xpath('//li[1]/span/span/text()').get()
        if not release_date:
            release_date = row.xpath('span/span/text()').get()
        if not release_date:
            release_date = row.xpath('text()').get()
        m_item = MovieItem()
        m_item["film"] = film
        m_item["budget"] = budget
        m_item["box_office"] = box_office
        m_item["release_date"] = release_date
        yield m_item

        # Construct the ProductionCoItem(s).
        prod_co_list = response.xpath('//tr[contains(th/div, "Production")]/td/div/div/ul')

        def gen_build_items(resp, movie, item_field, item, first, second):
            """
            Build CastItems, DirectorItems, or ProductionCoItems.

            If the items to be constructed are CastItems, single_link and single
            are both None. Otherwise, single_link and single are the xpaths to use
            in the case that there is a single director or production company, depending
            on whether the text is inside an anchor tag.

            Args:
                resp (scrapy Response object): This is scrapy's representation of an HTTP
                    response object arising from an HTTP request.
                movie (str): This is the film title.
                item_field (str): This is either 'actor_name', 'director', 'distributor',
                or 'prod_co' (the fields of the items CastItem, DirectorItem, DistributorItem
                and ProductionCoItem, respectively, excluding 'film').
                item (class): This is a reference to either a CastItem, DirectorItem,
                     or ProductionCoItem.
                first (str): This is the first xpath to try in the case that there are more than one of
                    the items of this type (CastItem, DirectorItem, or ProductionCoItem) to be
                    associated with the movie. For CastItem, this will always be the case.
                second (str): This is the xpath to try if the first fails to
                    produce any results.

            Returns:
                None
            """
            first_path = resp.xpath(first)
            if first_path and first_path.xpath('li'):
                if first_path.xpath('li/a/text()'):
                    for field in first_path.xpath('li/a/text()').getall():
                        built_item = item()
                        built_item["film"] = movie
                        built_item[item_field] = field
                        yield built_item
                elif first_path.xpath('li/text()'):
                    for field in first_path.xpath('li/text()').getall():
                        built_item = item()
                        built_item["film"] = movie
                        built_item[item_field] = field
                        yield built_item

            else:
                second_path = resp.xpath(second)
                if second_path and second_path.xpath('a/text()'):
                    for field in second_path.xpath('a/text()').getall():
                        built_item = item()
                        built_item["film"] = film
                        built_item[item_field] = field
                        yield built_item
                elif second_path.xpath('text()'):
                    for field in second_path.xpath('text()').getall():
                        built_item = item()
                        built_item["film"] = movie
                        built_item[item_field] = field
                        yield built_item

        def build_items(resp, movie, item_field):
            """
            Construct the items corresponding to item_field using inner_build_items.

            Args:
                resp (scrapy Response object): This is scrapy's representation of an HTTP
                    response object arising from an HTTP request.
                movie (str): This is the film title.
                item_field (str): This is either 'actor_name', 'director', or 'prod_co' (the
                    fields of the items CastItem, DirectorItem, and ProductionCoItem,
                     respectively, excluding 'film').

            Returns:
                gen_build_item (generator): This is the generator of items constructed
                    using gen_build_item based on the type of the item (CastItem, DirectorItem,
                    DistributorItem, ProductionCoItem).


            """
            if item_field == "actor_name":
                item = CastItem
                first = '//tr[contains(th, "Starring")]/td/div/ul'
                second = '//tr[contains(th, "Starring")]/td'
                return gen_build_items(resp, movie, item_field, item, first, second)
            elif item_field == "director":
                item = DirectorItem
                first = '//tr[contains(th, "Directed by")]/td/div/ul'
                second = '//tr[contains(th, "Directed by")]/td'
                return gen_build_items(resp, movie, item_field, item, first, second)
            elif item_field == "prod_co":
                item = ProductionCoItem
                first = '//tr[contains(th/div, "Production")]/td/div/div/ul'
                second = '//tr[contains(th/div,"Production")]/td/div'
                return gen_build_items(resp, movie, item_field, item, first, second)
            elif item_field == "distributor":
                item = DistributorItem
                first = '//tr[contains(th, "Distributed")]/td/div/ul'
                second = '//tr[contains(th, "Distributed")]/td'
                return gen_build_items(resp, movie, item_field, item, first, second)

        # Construct the CastItems, DirectorItems, DistributorItems, and ProductionCoItems.
        field_names = ["actor_name", "director", "distributor", "prod_co"]
        for field_name in field_names:
            if build_items(response, film, field_name):
                for b_item in build_items(response, film, field_name):
                    yield b_item





