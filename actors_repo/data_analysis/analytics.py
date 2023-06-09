import csv
import os

from typing import List, Text
from abc import ABC, abstractmethod

import pymysql
from dotenv import load_dotenv

load_dotenv()


class AnalyticsInterface(ABC):
    """
    This abstract class is used as an interface to query the actorswiki database.

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
        table (List[List[Text]]): the table of the statistics (after calling the query method);
            initially this is set to None
    """
    def __init__(self):
        self.u = os.environ.get('DB_USER')
        self.p = os.environ.get('DB_PSWD')
        self.h = os.environ.get('DB_HOST')
        self.conn = pymysql.connect(user=self.u, password=self.p, host=self.h)
        self.cursor = self.conn.cursor()
        self.cursor.execute("USE actors_wiki")
        self.table = None

    @abstractmethod
    def query(self):
        """
        Query the actors_wiki database, and store the result in the table attribute.
        """
        pass

    @abstractmethod
    def store_csv(self, file_path: Text):
        """
        Store the result in table (after performing query method) in a csv file.
        """
        pass


class ActorsAnalysis(AnalyticsInterface):
    """
    Find the box office max, min, average, standard deviation, and film count for each actor.
    """
    def query(self):
        """
        Store the table containing the box office statistics for each actor in the table attribute.
        """
        actors_query = """
                       With cte AS (
                           SELECT a.actor AS actor,
                               a.actor_id AS actor_id,
                               c.movie_id AS movie_id
                           FROM actors AS a
                           LEFT JOIN castlist AS c
                           ON a.actor_id = c.actor_id
                           )
                       SELECT 
                           actor, 
                           MAX(box_office) AS box_office_max,
                           MIN(box_office) AS box_office_min,
                           CAST(AVG(box_office) AS DECIMAL(12,6)) AS box_office_avg,
                           IF(STDDEV(box_office),                                    
                               CAST(STDDEV(box_office) AS DECIMAL(12, 6)),
                               0
                              ) AS box_office_std,
                           COUNT(movie) AS film_count  
                       FROM cte AS c 
                       LEFT JOIN movies AS m
                       ON c.movie_id = m.movie_id
                       WHERE box_office is not null
                       GROUP BY actor
                       ORDER BY box_office_max DESC, box_office_avg DESC, film_count DESC
                       """
        self.cursor.execute(actors_query)
        self.table = self.cursor.fetchall()

    def store_csv(self, file_path):
        """
        Store the result in the table attribute in a csv file.
        """
        with open(file_path, 'w', encoding='utf-8', newline='') as csv_file:
            actors_writer = csv.writer(csv_file, dialect='excel')
            actors_writer.writerow(["actor",
                                    "box office max (in millions)",
                                    "box office min (in millions)",
                                    "box office avg (in millions)",
                                    "box office std (in millions)",
                                    "film count"
                                    ]
                                   )
            actors_writer.writerows(self.table)
        self.conn.close()


class DirectorsAnalysis(AnalyticsInterface):
    """
    Find the box office max, min, average, standard deviation and film count for each director.
    """
    def query(self):
        """
        Store the table containing the box office statistics for each director in the table attribute.
        """
        directors_query = """
                          With cte AS (
                              SELECT 
                                  d.director AS director,
                                  d.director_id AS director_id,
                                  f.movie_id AS movie_id
                              FROM directors AS d
                              LEFT JOIN filmdirectors AS f
                              ON d.director_id = f.director_id
                              )
                          SELECT 
                              director,
                              MAX(box_office) AS box_office_max,
                              MIN(box_office) AS box_office_min,
                              CAST(AVG(box_office) AS DECIMAL(12,6)) AS box_office_avg,
                              IF(STDDEV(box_office),
                                  CAST(STDDEV(box_office) AS DECIMAL(12, 6)),
                                  0
                                 ) AS box_office_std,
                              COUNT(movie) AS film_count   
                          FROM cte AS c
                          LEFT JOIN movies AS m
                          ON c.movie_id = m.movie_id
                          WHERE box_office is not null
                          GROUP BY director
                          ORDER BY box_office_max DESC, box_office_avg DESC, film_count DESC
                          """
        self.cursor.execute(directors_query)
        self.table = self.cursor.fetchall()

    def store_csv(self, file_path):
        """
        Store the result in the table attribute in a csv file.
        """
        with open(file_path, 'w', encoding='utf-8', newline='') as csv_file:
            directors_writer = csv.writer(csv_file, dialect='excel')
            directors_writer.writerow(["director",
                                       "box office max (in millions)",
                                       "box office min (in millions)",
                                       "box office avg (in millions)",
                                       "box office std (in millions)",
                                       "film count"
                                       ]
                                      )
            directors_writer.writerows(self.table)
        self.conn.close()


class DistributorsAnalysis(AnalyticsInterface):
    """
    Find the box office max, min, average, standard deviation, and film count for each distributor.

    Given that the distribution companies are likely to have distributed more than one film each year,
    we consider these statistics per year, as well as cumulatively (across all years).
    """
    def query(self):
        """
        Store the table containing the box office statistic for each distributor in the table attribute.
        """
        distributors_query = """
                             With cte AS (
                                 SELECT d.distributor AS distributor,
                                     YEAR(m.release_date) AS release_year,
                                     m.box_office AS box_office,
                                     m.movie AS movie
                                 FROM distributors AS d
                                 LEFT JOIN filmdistributors AS f
                                 ON d.distributor_id = f.distributor_id
                                 LEFT JOIN movies AS m
                                 ON f.movie_id = m.movie_id
                                 WHERE box_office IS NOT NULL
                                 )
                             SELECT 
                                 IF(GROUPING(distributor),
                                     'All distributors',
                                     distributor
                                   ) AS distributor,
                                 IF(GROUPING(release_year),
                                     'All years',
                                     release_year
                                   ) AS release_year,
                                 MAX(box_office) AS box_office_max,
                                 MIN(box_office) AS box_office_min,
                                 COUNT(movie) AS film_count,
                                 CAST(AVG(box_office) AS DECIMAL(12, 6)) AS box_office_avg,
                                 IF(STDDEV(box_office),
                                     CAST(STDDEV(box_office) AS DECIMAL(12, 6)),
                                     0
                                   ) AS box_office_std
                             FROM cte AS c
                             GROUP BY
                                 distributor,
                                 release_year
                             WITH ROLLUP
                             """
        self.cursor.execute(distributors_query)
        self.table = self.cursor.fetchall()

    def store_csv(self, file_path):
        """
        Store the result in the table attribute in a csv file.
        """
        with open(file_path, 'w', encoding='utf-8', newline='') as csv_file:
            distributors_writer = csv.writer(csv_file, dialect='excel')
            distributors_writer.writerow(["distributor",
                                          "release year",
                                          "box office max (in millions)",
                                          "box office min (in millions)",
                                          "film count",
                                          "box office avg (in millions)",
                                          "box office std (in millions)"
                                          ]
                                         )
            distributors_writer.writerows(self.table)
        self.conn.close()


class ProductionCoAnalysis(AnalyticsInterface):
    """
    Find the box office max, min, average, standard deviation and film count for each production company.

    Given that each production company is likely to have produced more than one film each year, we consider
    these statistics per year, as well as cumulatively (across all years).

        """
    def query(self):
        """
        Store the table containing the box office statistic for each production company in the table attribute.
        """
        prod_co_query = """
                             With cte AS (
                                 SELECT 
                                     p.prod_co AS prod_co,
                                     YEAR(m.release_date) AS release_year,
                                     m.box_office AS box_office,
                                     m.movie AS movie
                                 FROM productionco AS p
                                 LEFT JOIN filmprodco AS f
                                 ON p.prod_co_id = f.prod_co_id
                                 LEFT JOIN movies AS m
                                 ON f.movie_id = m.movie_id
                                 WHERE box_office IS NOT NULL
                                 )
                             SELECT 
                                 IF(GROUPING(prod_co),
                                     'All production companies',
                                     prod_co
                                   ) AS prod_co,
                                 IF(GROUPING(release_year),
                                     'All years',
                                     release_year
                                   ) AS release_year,
                                 MAX(box_office) AS box_office_max,
                                 MIN(box_office) AS box_office_min,
                                 COUNT(movie) AS film_count,
                                 CAST(AVG(box_office) AS DECIMAL(12, 6)) AS box_office_avg,
                                 IF(STDDEV(box_office),
                                     CAST(STDDEV(box_office) AS DECIMAL(12, 6)),
                                     0
                                   ) AS box_office_std
                             FROM cte AS c
                             GROUP BY
                                 prod_co,
                                 release_year
                             WITH ROLLUP
                             """
        self.cursor.execute(prod_co_query)
        self.table = self.cursor.fetchall()

    def store_csv(self, file_path):
        """
        Store the result in the table attribute in a csv file.
        """
        with open(file_path, 'w', encoding='utf-8', newline='') as csv_file:
            prod_co_writer = csv.writer(csv_file, dialect='excel')
            prod_co_writer.writerow(["production company",
                                     "release year",
                                     "box office max (in millions)",
                                     "box office min (in millions)",
                                     "film count",
                                     "box office avg (in millions)",
                                     "box office std (in millions)"
                                     ]
                                    )
            prod_co_writer.writerows(self.table)
        self.conn.close()


if __name__ == "__main__":
    actors_stats = ActorsAnalysis()
    actors_stats.query()
    actors_stats_csv = os.environ.get('actors_path')
    actors_stats.store_csv(actors_stats_csv)
    directors_stats = DirectorsAnalysis()
    directors_stats.query()
    directors_stats_csv = os.environ.get('directors_path')
    directors_stats.store_csv(directors_stats_csv)
    distributors_stats = DistributorsAnalysis()
    distributors_stats.query()
    distributors_stats_csv = os.environ.get('distributors_path')
    distributors_stats.store_csv(distributors_stats_csv)
    production_co_stats = ProductionCoAnalysis()
    production_co_stats.query()
    production_co_stats_csv = os.environ.get('production_co_path')
    production_co_stats.store_csv(production_co_stats_csv)







