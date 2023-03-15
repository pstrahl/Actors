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
                        Actor_id INT AUTO_INCREMENT PRIMARY KEY,
                        Actor VARCHAR(100) NOT NULL UNIQUE
                        )"""
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Movies(
                        Movie_id INT AUTO_INCREMENT PRIMARY KEY,
                        Movie VARCHAR(200) NOT NULL UNIQUE
                        )"""
                            )
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Castlist(
                        Movie_id INT,
                        Actor_id INT,
                        PRIMARY KEY(Movie_id, Actor_id),
                        FOREIGN KEY(Movie_id)
                            REFERENCES Movies(Movie_id)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE,
                        FOREIGN KEY(Actor_id)
                            REFERENCES Actors(Actor_id)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE
                        )"""
                            )
    def process_item(self, item, actors_wiki_spider):
        """
        Insert the information from the items into the Actors, Movies, and Castlist tables.

        Find the Movie_id for the film in the Movies table or insert the film into the Movies table,
        insert the actor_name into the Actors table (if it wasn't already in there), get the
        Actor_id from the Actor's table, and insert the Movie_id and Actor_id into the Castlist table.

        Args:
            item (Scrapy Item object): This is a CastItem, which has two fields: 'film', 'actor_name'.
            actors_wiki_spider (Scrapy Spider): This is the spider we used to scrape wikipedia
                for movies and their starring cast in the specified time range.

        Returns:
            item
        """
        film = item.get("film")
        print("movie:{}".format(film))
        self.cursor.execute("SELECT Movie_id FROM Movies WHERE Movie = %s", (film,))
        movie_id = self.cursor.fetchone()
        print("Movie_id :{}".format(movie_id))
        if not movie_id:
            self.cursor.execute("INSERT INTO Movies(Movie) VALUES (%s)", (film,))
            self.cursor.execute("SELECT Movie_id FROM Movies WHERE Movie = %s", (film,))
            movie_id = self.cursor.fetchone()
            print("Movie_id:{}".format(movie_id))

        actor_name = item.get("actor_name")
        print("actor_name:{}".format(actor_name))
        self.cursor.execute("SELECT Actor_id FROM Actors WHERE Actor = %s", (actor_name,))
        actor_id = self.cursor.fetchone()
        print("actor_id:{}".format(actor_id))
        if not actor_id:
            self.cursor.execute("INSERT INTO Actors(Actor) VALUES (%s)", (actor_name, ))
            self.cursor.execute("SELECT Actor_id FROM Actors WHERE Actor = %s", (actor_name,))
            actor_id = self.cursor.fetchone()
            print("actor_id:{}".format(actor_id))

        self.cursor.execute("INSERT INTO Castlist(Movie_id, Actor_id) VALUES (%s, %s)", (movie_id, actor_id))
        self.conn.commit()

        return item
