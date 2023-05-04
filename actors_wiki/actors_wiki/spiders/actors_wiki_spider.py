from __future__ import print_function

import re
from time import sleep

import scrapy

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
        for num in range(2003, 2023):
            movie_by_year = f"https://en.wikipedia.org/wiki/List_of_American_films_of_{num}"
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
                href = row.xpath('./td/i/a/@href').get()
                yield scrapy.Request(url=response.urljoin(href), callback=self.parse_films)

    def parse_films(self, response):
        """
        Get the cast list, director(s), production companies, budget, box office, and release date for the film.

        First, we produce a MovieItem with fields 'film', 'budget', 'box_office', and 'release_date'.
        As outlined below, there are several cases to consider for extracting the release date:
        1) it is in a list of release dates, and we want the US release date, 2) there is a single
        Release date listed, or 3) it is listed as the Original air date.

        Next, we use build_items() to produce: DirectorItems with fields 'film', 'director',
        DistributorItems with fields 'film' and 'distributor', ProductionCoItem with fields
        'film' and 'prod_co' and CastItems with fields 'film' and 'actor_name'.

        Args:
            response (scrapy Response object): This is scrapy's representation of the
                HTTP response object arising from the request for one of the film pages.
        """
        # Construct a MovieItem
        # from scrapy.shell import inspect_response
        # inspect_response(response, self)
        def get_movie_fields(paths):
            """Get the film's name, budget, or box office.

            Args:
                 paths (List[str]): This is the list of xpaths to use
                     to get the field.
            Returns:
                This returns the film title, budget, or box office as a string.
            """
            for field_path in paths:
                if response.xpath(field_path):
                    return response.xpath(field_path).get()
                else:
                    continue
            return None

        def get_release_date(start_path, relative_paths, conditions):
            """Get the release date of the film.

            Args:
                start_path (str): This is the path containing the rows of a table with a potential
                    release date.
                relative_paths (List(str)): This is the list of relative paths to try from the first
                    row in rows_path.
                conditions (List(function)): This is the list of conditions to check to verify
                    that the result is the release date.

            Returns:
                This function returns the release date
            """
            rows = response.xpath(start_path)
            if rows:
                row = rows[0]
                for rel_path, condition in zip(relative_paths, conditions):
                    potential_date = row.xpath(rel_path).get()
                    if potential_date and not condition(potential_date):
                        return potential_date
                    else:
                        continue
                return None
            else:
                return None

        # This is for the film, budget, and box office.
        film_paths = ['//h1[@id="firstHeading"]/i/text()',
                      '//h1[@id="firstHeading"]/span/text()'
                      ]
        budget_paths = ['//tr[contains(th, "Budget")]/td/text()',
                        '//tr[contains(th, "Budget")]/td/span/text()',
                        '//tr[contains(th, "Budget")]/td//li[1]/text()'
                        ]
        box_office_paths = ['//tr[contains(th, "Box office")]/td/text()',
                            '//tr[contains(th, "Box office")]/td/span/text()',
                            '//tr[contains(th, "Box office")]/td//li[contains(., "total")]/text()',
                            '//tr[contains(th, "Box office")]/td//li[1]/text()'
                            ]
        # This is for the release date.
        rows_path = '//tr[contains(th/., "date") or contains(th/., "elease")]/td'
        rel_paths = ['.//li[text()[contains(., "States") or contains(., "US")]]/span/span/text()',
                     './/li[1]/span/span/text()',
                     './span/span/text()',
                     './text()']
        condition_list = [lambda x: re.sub(r"\(|\)", "", x) == "United States",
                          lambda x: len(x) < 4,
                          lambda x: len(x) < 4,
                          lambda x: len(x) < 4
                          ]
        # Now we build the movie item, keeping in mind that we will need access to these
        # variables (film in particular) later when constructing the CastItems, DirectorItems,
        # DistributorItems, and ProductionCoItems.
        m_item = MovieItem()
        film = get_movie_fields(film_paths)
        m_item["film"] = film
        budget = get_movie_fields(budget_paths)
        m_item["budget"] = budget
        box_office = get_movie_fields(box_office_paths)
        m_item["box_office"] = box_office
        release_date = get_release_date(rows_path, rel_paths, condition_list)
        m_item["release_date"] = release_date
        yield m_item

        def gen_build_items(resp, movie, item_field, item, first, second):
            """
            Build CastItems, DirectorItems, DistributorItems, or ProductionCoItems.

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
                item (class): This is a reference to either a CastItem, DirectorItem, DistributorItem,
                     or ProductionCoItem.
                first (str): This is the first xpath to try in the case that there are more than one of
                    the items of this type (CastItem, DirectorItem, DistributorItem, or ProductionCoItem)
                    to be associated with the movie. For CastItems, this will always be the case.
                second (str): This is the xpath to try if the first fails to
                    produce any results.

            Returns:
                None
            """
            first_path = resp.xpath(first)
            if first_path and first_path.xpath('li'):
                if first_path.xpath('./li/a/text()'):
                    for field in first_path.xpath('./li/a/text()').getall():
                        built_item = item()
                        built_item["film"] = movie
                        built_item[item_field] = field
                        yield built_item
                elif first_path.xpath('./li/text()'):
                    for field in first_path.xpath('./li/text()').getall():
                        built_item = item()
                        built_item["film"] = movie
                        built_item[item_field] = field
                        yield built_item

            else:
                second_path = resp.xpath(second)
                if second_path and second_path.xpath('./a/text()'):
                    for field in second_path.xpath('./a/text()').getall():
                        built_item = item()
                        built_item["film"] = film
                        built_item[item_field] = field
                        yield built_item
                elif second_path.xpath('./text()'):
                    for field in second_path.xpath('./text()').getall():
                        built_item = item()
                        built_item["film"] = movie
                        built_item[item_field] = field
                        yield built_item

        def build_items(resp, movie, item_field):
            """
            Construct the items corresponding to item_field using gen_build_items.

            Args:
                resp (scrapy Response object): This is scrapy's representation of an HTTP
                    response object arising from an HTTP request.
                movie (str): This is the film title.
                item_field (str): This is either 'actor_name', 'director', 'distributor', or 'prod_co'
                    (the fields of the items CastItem, DirectorItem, DistributorItem, and ProductionCoItem,
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





