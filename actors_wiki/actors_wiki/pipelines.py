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
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Movies(
                        movie_id INT AUTO_INCREMENT PRIMARY KEY,
                        movie VARCHAR(200) NOT NULL UNIQUE,
                        budget VARCHAR(100) DEFAULT NULL,
                        box_office VARCHAR(100) DEFAULT NULL
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
        Insert the information from the items into the Actors, Movies, and Castlist tables.

        Find the Movie_id for the film in the Movies table or insert the film into the Movies table
        with budget and box office as Null values.

        In the first case, the item is a MovieItem. We start by updating the budget and box_office
        in the Movies table. Then, we either get the director_id from the Directors table or we
        insert the director into the Directors table and get the director_id. Using this, we insert the
        movie_id and the director_id into the Filmdirectors table.

        Args:
            item (Scrapy Item object): This is a CastItem or a MovieItem. CastItems have two
                fields: 'film' and 'actor_name'. MovieItems have 4 fields: 'film', 'director', 'budget',
                and 'box_office'.
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
        movie_insert_query = """INSERT INTO Movies(movie, budget, box_office)
                                VALUES (%s, %s, %s)
                             """
        self.cursor.execute(movie_id_query, (film,))
        movie_id = self.cursor.fetchone()
        if not movie_id:
            self.cursor.execute(movie_insert_query, (film, "NULL", "NULL"))
            self.cursor.execute(movie_id_query, (film,))
            movie_id = self.cursor.fetchone()

        # In the first case the item is a MovieItem.
        if "director" in item.keys():
            movies_update_query = """UPDATE Movies
                                     SET 
                                         budget = %s,
                                         box_office = %s
                                     WHERE movie_id = %s
                                  """
            budget = item.get("budget")
            box_office = item.get("box_office")
            director = item.get("director")
            self.cursor.execute(movies_update_query, (budget, box_office, movie_id))

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

        # In the second case the item is a CastItem.
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
