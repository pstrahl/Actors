# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter

import os
import re
import sys

from datetime import datetime, date

import pymysql
from dotenv import load_dotenv

load_dotenv()


class DatePipeline:
    """
    This class is used to clean up the date field for insertion into a MySQL database.
    """
    def process_item(self, item, actors_wiki_spider):
        """
        Rewrite the date in YYYY-MM-DD format.

        If the item is a MovieItem, we will reformat the date in the YYYY-MM-DD format, and return the
        item with the new date. Otherwise, the item is returned unchanged.

        Args:
             item (Scrapy Item object): This is a CastItem, a DirectorItem, a DistributorItem, a MovieItem,
            or a ProductionCoItem. CastItems have two fields: 'film' and 'actor_name'. DirectorItems
            have 2 fields: 'film' and 'director'. DistributorItems have 2 fields: 'film' and 'distributor'/
            MovieItems have 5 fields: 'film', 'director', 'budget','box_office', and 'release_date'.
            ProductionCoItems have 2 fields: 'film' and 'prod_co'.

            actors_wiki_spider (Scrapy Spider): This is the spider we used to scrape wikipedia
                for movies and their starring cast in the specified time range.

        Returns:
            item
        """
        if "release_date" in item.keys():
            print("beginning DatePipeline")
            release_date = item.get("release_date", None)
            pattern = r"[a-z]+"
            if release_date and re.findall(pattern, release_date):
                print("Assessing release_date")
                pattern1 = r"(?P<Month>[A-Z][a-z]+)\s(?P<Day>[1-3]*?[1-9]),\s(?P<Year>[0-9]{4})"
                pattern2 = r"(?P<Day>[1-3]*?[1-9])\s(?P<Month>[A-Z]{1}[a-z]+)\s(?P<Year>[0-9]{4})"
                match1 = re.search(pattern1, release_date)
                match2 = re.search(pattern2, release_date)
                if match1:
                    print("match1")
                    month = match1.group("Month")
                    day = match1.group("Day")
                    year = match1.group("Year")
                    if len(day) == 1:
                        day = '0' + day
                    try:
                        new_date = " ".join([month, day, year])
                        dt = datetime.strptime(new_date, "%B %d %Y")
                        item["release_date"] = dt.strftime("%Y-%m-%d")
                        print(item["release_date"])
                    except:
                        new_date = " ".join([month, day, year])
                        dt = datetime.strptime(new_date, "%b %d %Y")
                        item["release_date"] = dt.strftime("%Y-%m-%d")
                        print(item["release_date"])
                elif match2:
                    print("match2")
                    month = match2.group("Month")
                    day = match2.group("Day")
                    year = match2.group("Year")
                    if len(day) == 1:
                        day = '0' + day
                    try:
                        new_date = " ".join([month, day, year])
                        dt = datetime.strptime(new_date, "%B %d %Y")
                        item["release_date"] = dt.strftime("%Y-%m-%d")
                        print(item["release_date"])
                    except:
                        new_date = " ".join([month, day, year])
                        dt = datetime.strptime(new_date, "%b %d %Y")
                        item["release_date"] = dt.strftime("%Y-%m-%d")
                        print(item["release_date"])
            elif len(release_date) == 4:
                item["release_date"] = "-".join([release_date, "01", "01"])
                print(item["release_date"])
        return item


class DBPipeline:
    """
    This class is used to store the actor and movie info in a MySQL database.

    Attributes:
        u (string): the username to use for logging into MySQL (set
            as an environment variable)
        p (string): the password to use for logging into MySQL (set
            as an environment variable)
        h (string): the hostname to use for logging into MySQL (set
            as an environment variable)
        conn (pymysql Connection object): the Connection object created to
            connect to MySQL
        cursor (pymysql Cursor object): the resulting cursor created from the
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
                """CREATE DATABASE IF NOT EXISTS Actors_wiki
                DEFAULT CHARACTER SET utf8"""
                )
        except pymysql.Error as err:
            print("Failed creating database: {}".format(err))
            sys.exit()

        self.cursor.execute("USE Actors_wiki")

        # Add in better error handling here
        self.conn.database = 'Actors_wiki'

        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Actors(
                               actor_id INT AUTO_INCREMENT PRIMARY KEY,
                               actor VARCHAR(100) NOT NULL UNIQUE
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Directors(
                               director_id INT AUTO_INCREMENT PRIMARY KEY,
                               director VARCHAR(100) NOT NULL UNIQUE
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Distributors(
                               distributor_id INT AUTO_INCREMENT PRIMARY KEY,
                               distributor VARCHAR(200) NOT NULL UNIQUE
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS ProductionCo(
                               prod_co_id INT AUTO_INCREMENT PRIMARY KEY,
                               prod_co VARCHAR(200) NOT NULL UNIQUE
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Movies(
                               movie_id INT AUTO_INCREMENT PRIMARY KEY,
                               movie VARCHAR(200) NOT NULL UNIQUE,
                               budget VARCHAR(100) DEFAULT NULL,
                               box_office VARCHAR(100) DEFAULT NULL,
                               release_date DATE DEFAULT NULL
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Filmdirectors(
                               movie_id INT,
                               director_id INT,
                               PRIMARY KEY(movie_id, director_id),
                               FOREIGN KEY(movie_id)
                                   REFERENCES Movies(movie_id),
                               FOREIGN KEY(director_id)
                                   REFERENCES Directors(director_id)
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Filmdistributors(
                               movie_id INT,
                               distributor_id INT,
                               PRIMARY KEY(movie_id, distributor_id),
                               FOREIGN KEY(movie_id)
                                   REFERENCES Movies(movie_id),
                               FOREIGN KEY(distributor_id)
                                   REFERENCES Distributors(distributor_id)
                               )
                            """
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Filmprodco(
                               movie_id INT,
                               prod_co_id INT,
                               PRIMARY KEY(movie_id, prod_co_id),
                               FOREIGN KEY(movie_id)
                                   REFERENCES Movies(movie_id),
                               FOREIGN KEY(prod_co_id)
                                   REFERENCES ProductionCo(prod_co_id)
                            )"""
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Castlist(
                               movie_id INT,
                               actor_id INT,
                               PRIMARY KEY(movie_id, actor_id),
                               FOREIGN KEY(movie_id)
                                   REFERENCES Movies(movie_id),
                               FOREIGN KEY(actor_id)
                                   REFERENCES Actors(actor_id)
                               )
                            """
                            )

    def process_item(self, item, actors_wiki_spider):
        """
        Insert the information from the items into the tables.

        Find the Movie_id for the film in the Movies table or insert the film into the Movies table
        with budget, box_office, and release_date as Null values.

        In the first case, the item is a MovieItem. We start by updating the budget, box_office, and
        release_date in the Movies table.

        In the other cases, we use the fill_tables function to fill the respective tables.

        Args:
            item (Scrapy Item object): This is a CastItem, a DirectorItem, a DistributorItem, a MovieItem,
            or a ProductionCoItem. CastItems have two fields: 'film' and 'actor_name'. DirectorItems
            have 2 fields: 'film' and 'director'. DistributorItems have 2 fields: 'film' and 'distributor'/
            MovieItems have 5 fields: 'film', 'director', 'budget','box_office', and 'release_date'.
            ProductionCoItems have 2 fields: 'film' and 'prod_co'.

            actors_wiki_spider (Scrapy Spider): This is the spider we used to scrape wikipedia
                for movies and their starring cast in the specified time range.

        Returns:
            item
        """

        def inner_fill_tables(cur, movie_id, item_field, item, id_query, primary_insert_query, foreign_insert_query):
            """
            Populate the tables.

            Args:
                cur (pymysql cursor object): This is the cursor created from the pymsql connection object.
                movie_id (str): This is the id of the movie in the Movies table
                item_field (str): This is either 'actor_name', 'director', or 'prod_co'.
                item (scrapy Item): This is either a CastItem, DirectorItem, or ProductionCoItem.
                id_query (str): This is the query used to obtain the id of the value of the item_field
                    in the Actors table, Directors table, or ProductionCo table.
                primary_insert_query (str): This is the query used to insert the value of the item_field
                   into the Actors table, Directors table, or ProductionCo table.
                foreign_insert_query (str): This is the query used to insert the movie_id and the value of
                the id of the value of the item_field into the Castlist table, Filmdirectors table, or
                Filmprodco table as foreign keys.

            Returns:
                None
            """
            field = item.get(item_field)
            cur.execute(id_query, (field,))
            field_id_tup = cur.fetchone()
            if field_id_tup:
                field_id = field_id_tup[0]
            else:
                cur.execute(primary_insert_query, (field,))
                cur.execute(id_query, (field,))
                field_id = cur.fetchone()[0]
            cur.execute(foreign_insert_query, (movie_id, field_id))

        def fill_tables(cur, movie_id, item):
            """
            Fill the tables according to item.

            Args:
                 cur (pymysql cursor object): : This is the cursor created from the pymsql connection object.
                movie_id (str): This is the id of the movie in the Movies table
                item (scrapy Item): This is either a CastItem, DirectorItem, or ProductionCoItem.

            Returns:
                None
            """
            if "actor_name" in item.keys() and item.get("actor_name", None):
                item_field = "actor_name"
                id_query = """SELECT actor_id
                              FROM Actors 
                              WHERE actor = %s
                           """
                primary_insert_query = """INSERT INTO Actors(actor)
                                          VALUES (%s)
                                       """
                foreign_insert_query = """INSERT INTO Castlist(movie_id, actor_id)
                                          VALUES (%s, %s)
                                       """
                inner_fill_tables(cur, movie_id, item_field, item, id_query, primary_insert_query, foreign_insert_query)

            elif "director" in item.keys() and item.get("director", None):
                item_field = "director"
                id_query = """SELECT director_id
                              FROM Directors
                              WHERE director = %s
                           """
                primary_insert_query = """INSERT INTO Directors(director)
                                          VALUES (%s)
                                       """
                foreign_insert_query = """INSERT INTO Filmdirectors(movie_id, director_id)
                                          VALUES (%s, %s)
                                       """
                inner_fill_tables(cur, movie_id, item_field, item, id_query, primary_insert_query, foreign_insert_query)
            elif "distributor" in item.keys() and item.get("distributor", None):
                item_field = "distributor"
                id_query = """SELECT distributor_id
                              FROM Distributors
                              WHERE distributor = %s
                           """
                primary_insert_query = """INSERT INTO Distributors(distributor)
                                          VALUES (%s)
                                       """
                foreign_insert_query = """INSERT INTO Filmdistributors(movie_id, distributor_id)
                                          VALUES (%s, %s)
                                       """
                inner_fill_tables(cur, movie_id, item_field, item, id_query, primary_insert_query, foreign_insert_query)
            elif "prod_co" in item.keys() and item.get("prod_co", None):
                item_field = "prod_co"
                id_query = """SELECT prod_co_id
                              FROM ProductionCo
                              WHERE prod_co = %s
                           """
                primary_insert_query = """INSERT INTO ProductionCo(prod_co)
                                          VALUES (%s)
                                       """
                foreign_insert_query = """INSERT INTO Filmprodco(movie_id, prod_co_id)
                                          VALUES (%s, %s)
                                       """
                inner_fill_tables(cur, movie_id, item_field, item, id_query, primary_insert_query, foreign_insert_query)

        # Get rid of any double quotation marks in any fields of the item.
        for key in item.keys():
            if isinstance(item[key], str):
                item[key].replace('"', '')

        # Get the movie_id, this is used in all cases in what follows.
        film = item.get("film")
        movie_id_query = """SELECT movie_id
                            FROM Movies
                            WHERE movie = %s
                         """
        movie_insert_query = """INSERT INTO Movies(movie)
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
        # Need to handle the release date format using regex to see if
        # it is already in the format YYYY-MM-DD. If it is not in this format
        # use strptime and strftime in datetime module to convert it to this
        # format.
        if "budget" in item.keys():
            movies_update_query = """UPDATE Movies
                                     SET 
                                        budget = %s,
                                        box_office = %s,
                                        release_date = %s
                                     WHERE movie_id = %s
                                  """
            budget = item.get("budget")
            box_office = item.get("box_office")
            # Need to handle the release date format using regex to see if
            # it is already in the format YYYY-MM-DD. If it is not in this format
            # use strptime and strftime in datetime module to convert it to this
            # format.
            release_date = item.get("release_date")
            self.cursor.execute(movies_update_query, (budget, box_office, release_date, movie_id))

        else:
            fill_tables(self.cursor, movie_id, item)
        self.conn.commit()

        return item
