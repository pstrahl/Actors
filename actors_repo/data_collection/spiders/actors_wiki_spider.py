from __future__ import print_function

import re
from typing import Callable, Generator, List, Optional, Text,  Type

import scrapy

from ..items import CastItem, DirectorItem, DistributorItem, MovieItem, ProductionCoItem


class Actorswiki(scrapy.Spider):
    """
    This class is used to scrape the list of films by year and subsequently scrape
    each film's wikipedia page for the list of starring actors, director(s), distributor(s),
    production companies, budget, box office, and release date.
    """

    name = 'actors_wiki_spider'

    def start_requests(self) -> Generator[scrapy.http.Request, None, None]:
        """
        Visit the wikipedia page for films released each year and parse each page using parselist.

        Yields:
            (scrapy.http.Request): The Request object for each wikipedia page with the
            list of American films released that year.
        """
        for num in range(2003, 2023):
            movie_by_year = f"https://en.wikipedia.org/wiki/List_of_American_films_of_{num}"
            yield scrapy.Request(url=movie_by_year, callback=self.parse_list)

    def parse_list(self, response: scrapy.http.Response) -> Generator[scrapy.http.Request, None, None]:
        """
        Scrape the list of films in each page of films by year to get the link to the film page.

        Args:
            response (scrapy.http.Response): Scrapy's representation of the HTTP Response
                object arising from the request for one of the List of American
                films of (year) pages.

        Yields:
            (scrapy.http.Request): The Request object for each film's wikipedia page for the
            given year.
        """
        months = ["January", "April", "July", "October"]
        for month in months:
            table = response.xpath('//div/h2[contains(span[2], "'+month+'")]/following-sibling::table[1]/tbody/tr')
            for row in table:
                href = row.xpath('./td/i/a/@href').get()
                yield scrapy.Request(url=response.urljoin(href), callback=self.parse_films)

    def parse_films(self, response: scrapy.http.Response) -> Generator[scrapy.Item, None, None]:
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
            response (scrapy.http.Response): Scrapy's representation of the HTTP Response object
                arising from the request for one of the film pages.

        Yields:
            (scrapy.Item): A MovieItem, CastItem(s), DirectorItem(s), ProductionCoItem(s),
            and DistributorItem(s) for each film for the given year.
        """
        # The next two functions are used to construct a MovieItem.
        def get_movie_fields(paths: List[Text]) -> Optional[Text]:
            """Get the film's title, budget, or box office.

            Args:
                 paths (List[Text]): The list of xpaths to use
                     to get the title, budget, or box office.

            Returns:
                (Optional[Text]): The film title, budget, or box office as a string, or None.
            """
            for field_path in paths:
                if response.xpath(field_path):
                    return response.xpath(field_path).get()
                else:
                    continue
            return None

        def get_release_date(start_path: Text, relative_paths: List[Text],
                             conditions: List[Callable[[Text], bool]]) -> Optional[Text]:
            """Get the release date of the film.

            Args:
                start_path (Text): The path containing the rows of a table with a potential
                    release date.
                relative_paths (List(Text)): The list of relative paths to try from the first
                    row in rows_path.
                conditions (List(function)): The list of conditions to check to verify
                    that the result is the release date.

            Returns:
                (Optional[Text]):  The film's release date or None.
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

        # This is used to construct CastItems, DirectorItems, DistributorItems, or ProductionCoItems.
        def build_items(resp: scrapy.http.Response, movie: Text,
                        item_field: Text) -> Generator[scrapy.Item, None, None]:
            """
            Construct the items corresponding to item_field using gen_items.

            Args:
                resp (scrapy.http.Response): Scrapy's representation of an HTTP
                    response object arising from the request for the film's wikipedia page.
                movie (Text): The film's title.
                item_field (Text): Either 'actor_name', 'director', 'distributor', or 'prod_co'
                    (the fields of the items CastItem, DirectorItem, DistributorItem, and ProductionCoItem,
                    respectively, excluding 'film').

            Returns:
                (Generator[scrapy.Item, None, None): The generator of items constructed
                using gen_items based on the subtype of the item (CastItem, DirectorItem,
                DistributorItem, ProductionCoItem).


            """
            def gen_items(item: Type[scrapy.Item], first: Text, second: Text) -> Generator[scrapy.Item, None, None]:
                """
                Build CastItems, DirectorItems, DistributorItems, or ProductionCoItems.

                Args:
                    item (Type[scrapy.Item]): A CastItem, DirectorItem, DistributorItem,
                         or ProductionCoItem class object
                    first (Text): The first xpath to try in the case that there are more than one of
                        the items of this type for this film (CastItem, DirectorItem, DistributorItem,
                        or ProductionCoItem) to be associated with the movie. For CastItems, this will
                        always be the case.
                    second (Text): The xpath to try if the first fails to
                        produce any results.

                Yields:
                    (scrapy.Item): An instance of either a CastItem, DirectorItem, DistributorItem,
                     or ProductionCoItem.
                """
                def item_helper(field: Text) -> scrapy.Item:
                    """
                    Having found the fields of the item, build the item.

                    Args:
                        field (Text): The remaining field in the item other than 'film'; this
                            is either the actor_name, director, distributor, or prod_co.

                    Returns:
                        (scrapy.Item): An instance of either a CastItem, DirectorItem, DistributorItem,
                        or ProductionCoItem.
                    """
                    built_item = item()
                    built_item["film"] = movie
                    built_item[item_field] = field
                    return built_item

                first_path = resp.xpath(first)
                if first_path and first_path.xpath('li'):
                    if first_path.xpath('.//li/a/text()'):
                        for field in first_path.xpath('.//li/a/text()').getall():
                            field = field.strip()
                            if field:
                                yield item_helper(field)
                    if first_path.xpath('.//li/text()'):
                        for field in first_path.xpath('.//li/text()').getall():
                            field = field.strip()
                            if field:
                                yield item_helper(field)
                else:
                    second_path = resp.xpath(second)
                    if second_path and second_path.xpath('./a/text()'):
                        for field in second_path.xpath('./a/text()').getall():
                            field = field.strip()
                            if field:
                                yield item_helper(field)
                    if second_path.xpath('./text()'):
                        for field in second_path.xpath('./text()').getall():
                            field = field.strip()
                            if field:
                                yield item_helper(field)

            if item_field == "actor_name":
                item = CastItem
                first = '//tr[contains(th, "Starring")]/td/div/ul'
                second = '//tr[contains(th, "Starring")]/td'
                return gen_items(item, first, second)
            elif item_field == "director":
                item = DirectorItem
                first = '//tr[contains(th, "Directed by")]/td/div/ul'
                second = '//tr[contains(th, "Directed by")]/td'
                return gen_items(item, first, second)
            elif item_field == "prod_co":
                item = ProductionCoItem
                first = '//tr[contains(th/div, "Production")]/td/div/div/ul'
                second = '//tr[contains(th/div,"Production")]/td/div'
                return gen_items(item, first, second)
            elif item_field == "distributor":
                item = DistributorItem
                first = '//tr[contains(th, "Distributed")]/td/div/ul'
                second = '//tr[contains(th, "Distributed")]/td'
                return gen_items(item, first, second)

        # Now we construct and yield a MovieItem.
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
                     './text()'
                     ]
        condition_list = [lambda x: re.sub(r"\(|\)", "", x) == "United States",
                          lambda x: len(x) < 4,
                          lambda x: len(x) < 4,
                          lambda x: len(x) < 4
                          ]
        # As we build the MovieItem, we store the film title in the variable film since we will need it
        # later when constructing the CastItems, DirectorItems, DistributorItems, and ProductionCoItems.
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
        # Construct and yield the CastItems, DirectorItems, DistributorItems, and ProductionCoItems.
        field_names = ["actor_name", "director", "distributor", "prod_co"]
        for field_name in field_names:
            if build_items(response, film, field_name):
                for b_item in build_items(response, film, field_name):
                    yield b_item





