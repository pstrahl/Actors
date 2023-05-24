# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter

import os
import re
import sys

from datetime import datetime
from typing import Text

import scrapy
import pymysql
from dotenv import load_dotenv
from scrapy.exceptions import DropItem

load_dotenv()


class DatePipeline:
    """
    This class is used to clean up the date field for insertion into a MySQL database.
    """
    def process_item(self, item: scrapy.Item, actors_wiki_spider: scrapy.Spider) -> scrapy.Item:
        """
        Rewrite the date in YYYY-MM-DD format.

        If the item is a MovieItem and the release date is not None, we reformat
        the date in the YYYY-MM-DD format, and return the item with the new date.
        If the item is a MovieItem and the release date is None, we raise a DropItem
        exception. Otherwise, the item is returned unmodified.

        Args:
            item (scrapy.Item): A CastItem, a DirectorItem, a DistributorItem, a MovieItem,
                or a ProductionCoItem; CastItems have two fields: 'film' and 'actor_name'; DirectorItems
                have 2 fields: 'film' and 'director'; DistributorItems have 2 fields: 'film' and 'distributor';
                MovieItems have 4 fields: 'film', 'budget','box_office', and 'release_date';
                ProductionCoItems have 2 fields: 'film' and 'prod_co'.

            actors_wiki_spider (scrapy.Spider): The spider used to scrape wikipedia
                for the movie info for each movie in the US over the years 2003-2022 (inclusive).

        Returns:
            item (scrapy.Item): The MovieItem with a modified date, otherwise the item unmodified.

        Raises:
            scrapy.exceptions.DropItem: if the item is a MovieItem and release_date is None.
        """
        if "release_date" in item.keys():
            release_date = item.get("release_date", None)
            if release_date:
                junk_pattern = r"\(.+\)"
                release_date = re.sub(junk_pattern, "", release_date)
                alpha_pattern = r"[a-z]+"
                if re.findall(alpha_pattern, release_date):
                    alpha_mdy = r"(?P<Month>[A-Z][a-z]+)\s(?P<Day>[0-3][0-9]|[0-9]),\s(?P<Year>[0-9]{4})"
                    alpha_dmy = r"(?P<Day>[0-3][0-9]|[0-9])\s(?P<Month>[A-Z][a-z]+)\s(?P<Year>[0-9]{4})"
                    alpha_ymd = r"(P<Year>[0-9]{4})[/\-]{1}(?P<Month>[A-Z][a-z]+)[/\-]{1}(?P<Day>[0-3][0-9]|[0-9])"
                    alpha_ym = r"(?P<Year>[0-9]{4})[/\- ]{1}(?P<Month>[A-Z][a-z]+)"
                    alpha_my = r"(?P<Month>[A-Z][a-z]+)[/\- ]{1}(?P<Year>[0-9]{4})"
                    alpha_patterns = [alpha_mdy, alpha_dmy,
                                      alpha_ymd, alpha_ym,
                                      alpha_my]
                    for pattern in alpha_patterns:
                        match = re.search(pattern, release_date)
                        if match:
                            month = match.group("Month")
                            year = match.group("Year")
                            if "Day" in match.groupdict().keys():
                                day = match.group("Day")
                                if len(day) == 1:
                                    day = "0" + day
                            else:
                                day = "01"

                            try:
                                new_date = " ".join([month, day, year])
                                dt = datetime.strptime(new_date, "%B %d %Y")
                                item["release_date"] = dt.strftime("%Y-%m-%d")
                                return item
                            except:
                                new_date = " ".join([month, day, year])
                                dt = datetime.strptime(new_date, "%b %d %Y")
                                item["release_date"] = dt.strftime("%Y-%m-%d")
                                return item

                else:
                    ymd = r"(?P<Year>[0-9]{4})[/\- ]{1}(?P<Month>[0-1][0-9]|[0-9])[/\- ](?P<Day>[0-3][0-9]|[0-9])"
                    ym = r"(?P<Year>[0-9]{4})[/\- ]{1}(?P<Month>[0-1][0-9]|[0-9])"
                    my = r"(?P<Month>[0-1][0-9]|[0-9])[/\- ]{1}(?P<Year>[0-9]{4})"
                    y = r"(?P<Year>[0-9]{4})"
                    num_patterns = [ymd, ym, my, y]
                    for pattern in num_patterns:
                        match = re.search(pattern, release_date)
                        if match:
                            year = match.group("Year")
                            if "Day" in match.groupdict().keys():
                                day = match.group("Day")
                                if len(day) == 1:
                                    day = "0" + day
                            else:
                                day = "01"
                            if "Month" in match.groupdict().keys():
                                month = match.group("Month")
                                if len(month) == 1:
                                    month = "0" + month
                            else:
                                month = "01"
                            item["release_date"] = "-".join([year, month, day])
                            return item
            else:
                raise DropItem(f"Release date missing from item {item}")
        else:
            return item


class MoneyPipeline:
    """
    This class is used to clean the budget and box_office strings.
    """
    def process_item(self, item: scrapy.Item, actors_wiki_spider: scrapy.Spider) -> scrapy.Item:
        """
        Remove "$", "\xa0" strings and convert strings to numbers.

        If the item is a MovieItem, remove '$' signs, '\xa0' strings, and convert words
        such as million or thousand to 1000000 or 1000, respectively. Otherwise,
        return the item unmodified.

        Args:
            item (scrapy.Item): A CastItem, a DirectorItem, a DistributorItem, a MovieItem,
                or a ProductionCoItem; CastItems have two fields: 'film' and 'actor_name'; DirectorItems
                have 2 fields: 'film' and 'director'; DistributorItems have 2 fields: 'film' and 'distributor';
                MovieItems have 4 fields: 'film', 'budget','box_office', and 'release_date';
                ProductionCoItems have 2 fields: 'film' and 'prod_co'.

            actors_wiki_spider (Scrapy Spider): The spider we used to scrape wikipedia
                for movie info for each movie in the US over the years 2003-2022 (inclusive).

        Returns:
            item (scrapy.Item): The original item with the box office and budget strings modified or
            the original item unmodified (if the item is not a MovieItem).
        """
        def number_cleaner(num_string: Text) -> Text:
            """
            Clean the num_string so it only consists of digits where the units are millions.

            Args:
                num_string (Text): The string which possibly consists of digits and
                    words representing numbers
            Returns:
                (Text): A string representing the number in num_string in digits where the
                units are in millions.
                """
            pattern = r"\\xa0|\$|,"
            new_string = re.sub(pattern, "", num_string)
            nums_pattern = r"(?P<upper>[\d]+\.[\d]+|[\d]+)[-–—]{1}(?P<lower>[\d]+\.[\d]+|[\d]+)"
            nums_match = re.search(nums_pattern, new_string)
            mil_pattern = r"million|Million"
            bil_pattern = r"billion|Billion"
            mil_match = re.search(mil_pattern, new_string)
            bil_match = re.search(bil_pattern, new_string)
            if nums_match:
                upper = float(nums_match.group("upper"))
                lower = float(nums_match.group("lower"))
                number = (upper + lower)/2
                if mil_match:
                    return f"{number:.6f}"
                elif bil_match:
                    return f"{number*1000:.6f}"
                else:
                    return f"{number/1000000:.6f}"
            else:
                num_pattern = r"(?P<decimal>[\d]+\.[\d]+|[\d]+)"
                num_match = re.search(num_pattern, new_string)
                if num_match:
                    number = float(num_match.group("decimal"))
                else:
                    number = 1
                if mil_match:
                    return f"{number:.6f}"
                elif bil_match:
                    return f"{number*1000:.6f}"
                else:
                    return f"{number/1000000:.6f}"

        if "budget" in item.keys():
            budget = item["budget"]
            box_office = item["box_office"]
            if budget:
                item["budget"] = number_cleaner(item["budget"])
            if box_office:
                item["box_office"] = number_cleaner(item["box_office"])
        return item


class DBPipeline:
    """
    This class is used to store the movie info in a MySQL database.

    Attributes:
        u (Text): the username to use for logging into MySQL (set
            as an environment variable)
        p (Text): the password to use for logging into MySQL (set
            as an environment variable)
        h (Text): the hostname to use for logging into MySQL (set
            as an environment variable)
        conn (pymysql.connections.Connection): the Connection object created to
            connect to MySQL
        cursor (pymysql.cursors.Cursor): the resulting cursor created from the
            Connection object
    """
    def __init__(self):
        self.u = os.environ.get('DB_USER')
        self.p = os.environ.get('DB_PSWD')
        self.h = os.environ.get('DB_HOST')
        self.conn = pymysql.connect(user=self.u, password=self.p, host=self.h)
        self.cursor = self.conn.cursor()
        try:
            self.cursor.execute(
                """CREATE DATABASE IF NOT EXISTS actors_wiki
                DEFAULT CHARACTER SET utf8"""
                )
        except pymysql.Error as err:
            print(f"Failed creating database: {err}")
            sys.exit()

        self.cursor.execute("USE actors_wiki")
        self.conn.database = 'actors_wiki'
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS actors(
                               actor_id INT AUTO_INCREMENT PRIMARY KEY,
                               actor VARCHAR(100) NOT NULL UNIQUE
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS directors(
                               director_id INT AUTO_INCREMENT PRIMARY KEY,
                               director VARCHAR(100) NOT NULL UNIQUE
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS distributors(
                               distributor_id INT AUTO_INCREMENT PRIMARY KEY,
                               distributor VARCHAR(200) NOT NULL UNIQUE
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS productionco(
                               prod_co_id INT AUTO_INCREMENT PRIMARY KEY,
                               prod_co VARCHAR(200) NOT NULL UNIQUE
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS movies(
                               movie_id INT AUTO_INCREMENT PRIMARY KEY,
                               movie VARCHAR(200) NOT NULL UNIQUE,
                               budget DECIMAL(12,6) DEFAULT NULL,
                               box_office DECIMAL (12,6) DEFAULT NULL,
                               release_date DATE DEFAULT NULL
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS filmdirectors(
                               movie_id INT,
                               director_id INT,
                               PRIMARY KEY(movie_id, director_id),
                               FOREIGN KEY(movie_id)
                                   REFERENCES movies(movie_id),
                               FOREIGN KEY(director_id)
                                   REFERENCES directors(director_id)
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS filmdistributors(
                               movie_id INT,
                               distributor_id INT,
                               PRIMARY KEY(movie_id, distributor_id),
                               FOREIGN KEY(movie_id)
                                   REFERENCES movies(movie_id),
                               FOREIGN KEY(distributor_id)
                                   REFERENCES distributors(distributor_id)
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS filmprodco(
                               movie_id INT,
                               prod_co_id INT,
                               PRIMARY KEY(movie_id, prod_co_id),
                               FOREIGN KEY(movie_id)
                                   REFERENCES movies(movie_id),
                               FOREIGN KEY(prod_co_id)
                                   REFERENCES productionCo(prod_co_id)
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS castlist(
                               movie_id INT,
                               actor_id INT,
                               PRIMARY KEY(movie_id, actor_id),
                               FOREIGN KEY(movie_id)
                                   REFERENCES movies(movie_id),
                               FOREIGN KEY(actor_id)
                                   REFERENCES actors(actor_id)
                               )
                            """
                            )

    def process_item(self, item: scrapy.Item, actors_wiki_spider: scrapy.Spider) -> scrapy.Item:
        """
        Insert the information from the items into the tables.

        Find the Movie_id for the film in the movies table or insert the film into the movies table
        with budget, box_office, and release_date as Null values.

        In the first case, the item is a MovieItem. We start by updating the budget, box_office, and
        release_date in the Movies table.

        In the other cases, we use the fill_tables function to fill the respective tables.

        Args:
            item (scrapy.Item): A CastItem, a DirectorItem, a DistributorItem, a MovieItem,
                or a ProductionCoItem; CastItems have two fields: 'film' and 'actor_name'; DirectorItems
                have 2 fields: 'film' and 'director'; DistributorItems have 2 fields: 'film' and 'distributor';
                MovieItems have 4 fields: 'film', 'budget','box_office', and 'release_date';
                ProductionCoItems have 2 fields: 'film' and 'prod_co'.

            actors_wiki_spider (scrapy.Spider): The spider we used to scrape wikipedia
                for movie info for each movie in the US over the years 2003-2022 (inclusive).

        Returns:
            item (scrapy.Item): The item is returned unmodified.
        """

        def fill_tables(movie_id: Text) -> None:
            """
            Fill the tables according to item subtype.

            Args:
                movie_id (Text): The id of the movie in the movies table.

            Returns:
                None
            """

            def fill_helper(item_field: Text, id_query: Text, primary_insert_query: Text,
                            foreign_insert_query: Text) -> None:
                """
                Populate the tables.

                Args:
                    item_field (Text): Either 'actor_name', 'director', 'distributor', or 'prod_co', depending on
                        the subtype of the item.
                    id_query (Text): The query used to obtain the id of the value of the item_field
                        in the actors table, directors table, distributors table or productionco table.
                    primary_insert_query (Text): The query used to insert the value of the item_field
                        into the actors table, directors table, distributors table, or productionco table.
                    foreign_insert_query (Text): The query used to insert the movie_id and the id of the
                        value of the item_field into the castlist table, filmdirectors table, filmdistributors table,
                        or filmprodco table as foreign keys.

                Returns:
                    None
                """
                field = item.get(item_field)
                cur = self.cursor
                cur.execute(id_query, (field,))
                field_id_tup = cur.fetchone()
                if field_id_tup:
                    field_id = field_id_tup[0]
                else:
                    cur.execute(primary_insert_query, (field,))
                    cur.execute(id_query, (field,))
                    field_id = cur.fetchone()[0]
                cur.execute(foreign_insert_query, (movie_id, field_id))

            if "actor_name" in item.keys() and item.get("actor_name", None):
                item_field = "actor_name"
                id_query = """SELECT actor_id
                              FROM actors 
                              WHERE actor = %s
                           """
                primary_insert_query = """INSERT IGNORE INTO actors(actor)
                                          VALUES (%s)
                                       """
                foreign_insert_query = """INSERT IGNORE INTO castlist(movie_id, actor_id)
                                          VALUES (%s, %s)
                                       """
                fill_helper(item_field, id_query, primary_insert_query, foreign_insert_query)
            elif "director" in item.keys() and item.get("director", None):
                item_field = "director"
                id_query = """SELECT director_id
                              FROM directors
                              WHERE director = %s
                           """
                primary_insert_query = """INSERT IGNORE INTO directors(director)
                                          VALUES (%s)
                                       """
                foreign_insert_query = """INSERT IGNORE INTO filmdirectors(movie_id, director_id)
                                          VALUES (%s, %s)
                                       """
                fill_helper(item_field, id_query, primary_insert_query, foreign_insert_query)
            elif "distributor" in item.keys() and item.get("distributor", None):
                item_field = "distributor"
                id_query = """SELECT distributor_id
                              FROM distributors
                              WHERE distributor = %s
                           """
                primary_insert_query = """INSERT IGNORE INTO distributors(distributor)
                                          VALUES (%s)
                                       """
                foreign_insert_query = """INSERT IGNORE INTO filmdistributors(movie_id, distributor_id)
                                          VALUES (%s, %s)
                                       """
                fill_helper(item_field, id_query, primary_insert_query, foreign_insert_query)
            elif "prod_co" in item.keys() and item.get("prod_co", None):
                item_field = "prod_co"
                id_query = """SELECT prod_co_id
                              FROM productionco
                              WHERE prod_co = %s
                           """
                primary_insert_query = """INSERT IGNORE INTO productionco(prod_co)
                                          VALUES (%s)
                                       """
                foreign_insert_query = """INSERT IGNORE INTO filmprodco(movie_id, prod_co_id)
                                          VALUES (%s, %s)
                                       """
                fill_helper(item_field, id_query, primary_insert_query, foreign_insert_query)

        # Get rid of any double quotation marks in any fields of the item.
        for key in item.keys():
            if isinstance(item[key], str):
                item[key].replace('"', '')

        # Get the movie_id, this is used in all cases in what follows.
        film = item.get("film")
        movie_id_query = """SELECT movie_id
                            FROM movies
                            WHERE movie = %s
                         """
        movie_insert_query = """INSERT IGNORE INTO movies(movie)
                                VALUES (%s)
                             """
        self.cursor.execute(movie_id_query, (film,))
        movie_id_tup = self.cursor.fetchone()
        if movie_id_tup:
            movie_id = movie_id_tup[0]
        else:
            self.cursor.execute(movie_insert_query, (film,))
            self.cursor.execute(movie_id_query, (film,))
            movie_id = self.cursor.fetchone()[0]

        # In the first case the item is a MovieItem.
        if "budget" in item.keys():
            movies_update_query = """UPDATE movies
                                     SET 
                                        budget = %s,
                                        box_office = %s,
                                        release_date = %s
                                     WHERE movie_id = %s
                                  """
            budget = item.get("budget")
            box_office = item.get("box_office")
            release_date = item.get("release_date")
            self.cursor.execute(movies_update_query, (budget, box_office, release_date, movie_id))
        # In the remaining cases the item is either a CastItem, DirectorItem, DistributorItem, or
        # a ProductionCoItem.
        else:
            fill_tables(movie_id)
        self.conn.commit()
        self.conn.close()
        return item
