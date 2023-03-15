from __future__ import print_function

import sys
import os
from time import sleep

from dotenv import load_dotenv

import scrapy
import pymysql

load_dotenv()

db_user = os.environ.get('DB_USER')
db_pswd = os.environ.get('DB_PSWD')
db_host = os.environ.get('DB_HOST')
connection = pymysql.connect(user=db_user, password=db_pswd, host=db_host)
cursor = connection.cursor()

DB_name = 'Actors_wiki'

def create_database(cursor):
    """
    Create the database Actors_wiki.

    Arguments:
        cursor (MySQL cursor): This is the cursor constructed using the MySQL connection.

    Returns:
         None
    """
    try:
        cursor.execute(
            """CREATE DATABASE IF NOT EXISTS {}
             DEFAULT CHARACTER SET {}""".format(DB_name, 'utf8')
             )
    except pymysql.Error as err:
        print("Failed creating database: {}".format(err))
        sys.exit()

try:
    cursor.execute("USE {}".format(DB_name))
except pymysql.Error as err:
    print("Database {} does not exist.".format(DB_name))
    print(err)
    create_database(cursor)
    print("Database {} created successfully.".format(DB_name))
    connection.database = DB_name
    #else:
        # sys.exit()

Actors_Table = """CREATE TABLE IF NOT EXISTS Actors(
                Actor_id INT AUTO_INCREMENT PRIMARY KEY,
                Actor VARCHAR(100) NOT NULL UNIQUE,
                Actor_href VARCHAR(100),
                Retrieved BOOLEAN DEFAULT 0
                )"""
Movies_Table = """CREATE TABLE IF NOT EXISTS Movies(
                Movie_id INT AUTO_INCREMENT PRIMARY KEY,
                Movie VARCHAR(200) NOT NULL UNIQUE,
                Movie_href VARCHAR(200) NOT NULL UNIQUE,
                Retrieved BOOLEAN DEFAULT 0
                )
                """
Cast_Table = """CREATE TABLE IF NOT EXISTS Cast(
            Movie_id INT,
             Actor_id INT,
            CONSTRAINT fk_movie
            FOREIGN KEY (Movie_id)
                REFERENCES Movies(Movie_id),
            CONSTRAINT fk_actor
            FOREIGN KEY (Actor_id)
                REFERENCES Actors(Actor_id)
            """

# Try it for a different set of years (and therefore a different initial film)
class Actors_wiki(scrapy.Spider):
    """
    This class is used to scrape each film's wikipedia page for the list
    of starring actors, and scrape each star's wikipedia page for their
    filmography.
    """
    name = 'test'
    # allowed_domains = ["en.wikipedia.org/"]
    def start_requests(self):
        yield scrapy.Request(url="https://en.wikipedia.org/wiki/Top_Gun:_Maverick",
                             callback=self.parse_cast)
        # yield scrapy.Request(url="https://en.wikipedia.org/wiki/Guardians_of_the_Galaxy_(film)",
          #                   callback=self.parse_cast)

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




    def parse_cast(self, response):
        """
        Scrape for the cast list of the film.
        """
        film = response.xpath('//h1[@id="firstHeading"]/i/text()').get()
        cast_table = response.xpath('//tr[contains(th, "Starring")]/td/div/ul')
        for item in cast_table.xpath('li'):
            sleep(5)
            actor_dict = dict()
            actor_dict['movie'] = film
            if item.xpath('a/text()').get():
                actor_dict['actor_name'] = item.xpath('a/text()').get()
                actor_dict['actor_href'] = item.xpath('a/@href').get()
            else:
                actor_dict['actor_name'] = item.xpath('text()').get()
                actor_dict['actor_href'] = None
            yield actor_dict
        for item in cast_table.xpath('li'):
            sleep(5)
            if item.xpath('a/text()').get():
                link = item.xpath('a/@href').get()
                yield scrapy.Request(url=response.urljoin(link), callback=self.parse_films)
            else:
                link = r"/wiki/" + item.xpath('text()').get().replace(" ", "_")
                try:
                    yield scrapy.Request(url=response.urljoin(link), callback=self.parse_films)
                except:
                    pass


    def parse_films(self, response):
        """
        Scrape for the actor's filmography.

        For some actors, the filmography is not located on their page
        so we must follow the link to their filmography.
        """
        # In the first case the filmography is located on the actor's page.
        film_table = response.xpath('//div/h3[contains(span, "Film")]/following-sibling::table[1]/tbody')
        if film_table:
            # Get the rows with a film href in them and which contain the year.
            # The year is the first child of the row
            for item in film_table.xpath('tr[count(td)=4 and td/i/a] | tr[th and td/i/a]'):
                year = item.xpath('normalize-space(/*[1]/text())').get()
                if year != '2022':
                    pass
                else:
                    # If the first child has the attribute rowspan, then the value is equal to the number of
                    # films starred in in that year.
                    sleep(5)
                    no_of_films = int(item.xpath('*[1]/@rowspan').get(default=1))
                    film_dict = dict()
                    film_dict['movie_name'] = item.xpath('*[2]/i/a/text()').get()
                    film_dict['movie_href'] = item.xpath('*[2]/i/a/@href').get()
                    yield film_dict
                    for k in range(2, no_of_films + 1):
                        row = item.xpath('following-sibling::tr["+str(k)+"]')
                        if row.xpath('td[1]/i/a').get():
                            film_dict = dict()
                            film_dict['movie_name'] = row.xpath('td[1]/i/a/text()').get()
                            film_dict['movie_href'] = row.xpath('td[1]/i/a/@href').get()
                            yield film_dict
                        else:
                            pass
            for item in film_table.xpath('tr[count(td)=4 and td[2]/i/a] | tr[th and td/i/a]'):
                year = item.xpath('normalize-space(*[1]/text())').get()
                if year != '2022':
                    pass
                else:
                    sleep(5)
                    no_of_films = int(item.xpath('*[1]/@rowspan').get(default=1))
                    href = item.xpath('*[2]/i/a/@href').get()
                    yield scrapy.Request(url=response.urljoin(href), callback=self.parse_cast)
                    for k in range(2, no_of_films + 1):
                        row = item.xpath('following-sibling::tr["+str(k)+"]')
                        if row.xpath('td[1]/i/a').get():
                            href = row.xpath('td[1]/i/a/@href').get()
                            yield scrapy.Request(url=response.urljoin(href), callback=self.parse_cast)
                        else:
                            pass
        else:
            # Otherwise the filmography is in a link.
            heading = response.xpath('//div/h2[contains(span, "Filmography")]')
            href = heading.xpath('following-sibling::div[text()="Main Article:"]/a/href').get()
            yield scrapy.Request(url=response.urljoin(href), callback=self.parse_films)



