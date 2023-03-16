from __future__ import print_function

from time import sleep

import scrapy

from actors_wiki.items import CastItem, DirectorItem, MovieItem, ProductionCoItem


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
        Next, we produce a DirectorItem with fields 'film', 'director' for each director of the film.
        After this, for each production company of the film, we produce a ProductionCoItem with fields
        'film' and 'prod_co'. Finally, for each starring cast member of the film, we produce a CastItem with
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
        release_date = response.xpath('//tr[contains(th/div, "Release date")]/td/div/ul/li/span/span/text()').get()
        m_item = MovieItem()
        m_item["film"] = film
        m_item["budget"] = budget
        m_item["box_office"] = box_office
        m_item["release_date"] = release_date
        yield m_item

        # Construct the DirectorItem(s).
        directors = response.xpath('//tr[contains(th, "Directed by")]/td/div/ul')
        if directors:
            for item in directors.xpath('li'):
                sleep(2)
                if item.xpath('a/text()').get():
                    director = item.xpath('a/text()').get()
                else:
                    director = item.xpath('text()').get()
                d_item = DirectorItem()
                d_item["film"] = film
                d_item["director"] = director
                yield d_item
        else:
            director = response.xpath('//tr[contains(th, "Directed by")]/td/a/text()').get()
            if not director:
                director = response.xpath('//tr[contains(th, "Directed by")]/td/text()').get()
            d_item = DirectorItem()
            d_item["film"] = film
            d_item["director"] = director
            yield d_item

        # Construct the ProductionCoItem(s).
        prod_co_list = response.xpath('//tr[contains(th/div, "Production")]/td/div/div/ul')
        if prod_co_list:
            for item in prod_co_list.xpath('li'):
                sleep(2)
                if item.xpath('a/text()').get():
                    prod_co = item.xpath('a/text()').get()
                else:
                    prod_co = item.xpath('text()').get()
                p_item = ProductionCoItem()
                p_item["film"] = film
                p_item["prod_co"] = prod_co
                yield p_item
        else:
            prod_co = response.xpath('//tr[contains(th/div, "Production")]/td/div/a/text()').get()
            if not prod_co:
                prod_co = response.xpath('//tr[contains(th/div,"Production")]/td/div/text()').get()
            p_item = ProductionCoItem()
            p_item["film"] = film
            p_item["prod_co"] = prod_co
            yield p_item

        # Construct the CastItem(s).
        cast_list = response.xpath('//tr[contains(th, "Starring")]/td/div/ul')
        for item in cast_list.xpath('li'):
            sleep(2)
            c_item = CastItem()
            c_item["film"] = film
            if item.xpath('a/text()').get():
                c_item["actor_name"] = item.xpath('a/text()').get()
            else:
                c_item["actor_name"] = item.xpath('text()').get()
            yield c_item




