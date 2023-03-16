# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter

import os
import sys

import pymysql
from dotenv import load_dotenv

load_dotenv()


class ActorsWikiPipeline:
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
                        )"""
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Directors(
                        director_id INT AUTO_INCREMENT PRIMARY KEY,
                        director VARCHAR(100) NOT NULL UNIQUE
                        )"""
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS ProductionCo(
                        prod_co_id INT AUTO_INCREMENT PRIMARY KEY,
                        prod_co VARCHAR(200) NOT NULL UNIQUE
                        )"""
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Movies(
                        movie_id INT AUTO_INCREMENT PRIMARY KEY,
                        movie VARCHAR(200) NOT NULL UNIQUE,
                        budget VARCHAR(100) DEFAULT NULL,
                        box_office VARCHAR(100) DEFAULT NULL,
                        release_date DATE DEFAULT NULL
                        )"""
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Filmdirectors(
                        movie_id INT,
                        director_id INT,
                        PRIMARY KEY(movie_id, director_id),
                        FOREIGN KEY(movie_id)
                            REFERENCES Movies(movie_id),
                        FOREIGN KEY(director_id)
                            REFERENCES Directors(director_id)
                            )"""
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
                        )"""
                            )

    def process_item(self, item, actors_wiki_spider):
        """
        Insert the information from the items into the tables.

        Find the Movie_id for the film in the Movies table or insert the film into the Movies table
        with budget, box_office, and release_date as Null values.

        In the first case, the item is a MovieItem. We start by updating the budget, box_office, and
        release_date in the Movies table.

        In the second case, the item is a DirectorItem. In this case we either get the director_id from
        the Directors table or we insert the director into the Directors table and get the director_id.
        Using this, we insert the movie_id and the director_id into the Filmdirectors table.

        In the third case, the item is a ProductionCoItem. In this case we either get the prod_co_id from
        the ProductionCo table or we insert the prod_co into the ProductionCo table and get the prod_co_id.
        Using this, we insert the movie_id and the prod_co_id into the Filmprodco table.

        In the last case, the item is an CastItem. In this case we either get the actor_id from the Actors
        table or we insert the actor_name into the Actors table and get the actor_id. Using this, we insert the
        movie_id and the actor_id into the Castlist table.

        Args:
            item (Scrapy Item object): This is a CastItem, a DirectorItem, a MovieItem, or a
            ProductionCoItem. CastItems have two fields: 'film' and 'actor_name'. DirectorItems
            have 2 fields: 'film' and 'director'. MovieItems have 5 fields: 'film', 'director', 'budget',
            'box_office', and 'release_date'. ProductionCoItems have 2 fields: 'film' and 'prod_co'.

            actors_wiki_spider (Scrapy Spider): This is the spider we used to scrape wikipedia
                for movies and their starring cast in the specified time range.

        Returns:
            item
        """
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
        movie_id = self.cursor.fetchone()
        if not movie_id:
            self.cursor.execute(movie_insert_query, (film,))
            self.cursor.execute(movie_id_query, (film,))
            movie_id = self.cursor.fetchone()

        # In the first case the item is a MovieItem.
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
            release_date = item.get("release_date")
            self.cursor.execute(movies_update_query, (budget, box_office, release_date, movie_id))

        # In the second case the item is a DirectorItem.
        if "director" in item.keys() and item.get("director", None):
            director = item.get("director")
            director_id_query = """SELECT director_id
                                   FROM Directors
                                   WHERE director = %s
                                """
            directors_insert_query = """INSERT INTO Directors(director)
                                        VALUES (%s)
                                     """
            self.cursor.execute(director_id_query, (director,))
            director_id = self.cursor.fetchone()
            if not director_id:
                self.cursor.execute(directors_insert_query, (director,))
                self.cursor.execute(director_id_query, (director,))
                director_id = self.cursor.fetchone()
            filmdirectors_insert_query = """INSERT INTO Filmdirectors(movie_id, director_id)
                                            VALUES (%s, %s)
                                         """
            self.cursor.execute(filmdirectors_insert_query, (movie_id, director_id))

        # In the third case the item is a ProductionCoItem.
        if "prod_co" in item.keys() and item.get("prod_co", None):
            prod_co = item.get("prod_co")
            prod_co_id_query = """SELECT prod_co_id
                                  FROM ProductionCo
                                  WHERE prod_co = %s
                               """
            prod_co_insert_query = """INSERT INTO ProductionCo(prod_co)
                                      VALUES (%s)
                                   """
            self.cursor.execute(prod_co_id_query, (prod_co,))
            prod_co_id = self.cursor.fetchone()
            if not prod_co_id:
                self.cursor.execute(prod_co_insert_query, (prod_co,))
                self.cursor.execute(prod_co_id_query, (prod_co,))
                prod_co_id = self.cursor.fetchone()
            filmprodco_insert_query = """INSERT INTO Filmprodco(movie_id, prod_co_id)
                                         VALUES (%s, %s)
                                      """
            self.cursor.execute(filmprodco_insert_query, (movie_id, prod_co_id))

        # In the last case the item is a CastItem.
        if "actor_name" in item.keys():
            actor_name = item.get("actor_name")
            actor_query = """SELECT actor_id
                             FROM Actors 
                             WHERE actor = %s
                          """
            actor_insert_query = """INSERT INTO Actors(actor)
                                    VALUES (%s)
                                 """
            self.cursor.execute(actor_query, (actor_name,))
            actor_id = self.cursor.fetchone()
            if not actor_id:
                self.cursor.execute(actor_insert_query, (actor_name,))
                self.cursor.execute(actor_query, (actor_name,))
                actor_id = self.cursor.fetchone()
            castlist_insert_query = """INSERT INTO Castlist(movie_id, actor_id)
                                       VALUES (%s, %s)
                                    """
            self.cursor.execute(castlist_insert_query, (movie_id, actor_id))

        self.conn.commit()

        return item
