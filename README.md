# Actors
This project uses scrapy and pymysql to scrape Wikipedia for film information, store the data in a MySQL database, and compute some statistics for each actor, director, distributor, and production company.

## Description
To obtain the film information and store this information in the database: <br>
**1.** The scrapy spider actors_wiki_spider scrapes the page en.wikipedia.org/wiki/List_of_American_films_of_20** (where ** is between 03 and 22, both inclusive) for the link to the 
wikipedia page for each film. <br>
**2.** Once on each film's page, the spider scrapes for the film's title, release date, budget, box office, director(s), production companies, distributors,
and starring castlist. <br>
**3.** Several scrapy items are constructed: <br>
  - **MovieItems** contain the fields "film", "budget", "box_office", and "release_date" <br>
  - **CastItems** contain the fields "film" and "actor_name" <br>
  - **DirectorItems** contain the fields "film" and "director" <br>
  - **DistributorItems** contain the fields "film" and "distributor" <br>
  - **ProductionCoItems** contain the fields "film" and "prod_co" <br>

**4.** These items are then passed to the item pipeline, the first step of which is the **DropEmptyPipeline**. If the item_field is None after eliminating strings enclosed in brackets, braces, or parentheses, or other non-text characters, this step drops the item. If this is not the case, this step removes extraneous punctuation symbols from otherwise valid items. <br>
**5.** The second step is the **DatePipeline**. If the item is a MovieItem, this class cleans the release_date field and convert it into the 
numerical YYYY-MM-DD or drops the MovieItem if the release_date is None. Otherwise the item is returned as is. <br>
**6.** The third step is the **MoneyPipeline**, which cleans the budget and box_office fields (if the item is a MovieItem) so that they are represented as decimals with 6 places to the right of the decimal; 
it is understood that these fields are now in terms of millions of dollars. If the item is not a MovieItem, it is returned as is. <br>
**7.** Having cleaned the relevant fields for insertion into a MySQL database, the **DBPipeline** creates a MySQL database titled actors_wiki whose schema can be found in the 
actors_wiki_ER_diagram pdf. <br>

To query the database and store the resulting tables in csv files: <br>
**8.** Having completed the above steps, for each actor and director, over the 20 year timespan, we find the box office maximum, minimum, average, and standard deviation, as well as the film count. This information is stored in two separate tables (one for actors and one for directors). <br>
**9.** Since distribution and production companies tend to distribute or produce more than one film per year, we find the film count, box office maximum, minimum, average and standard deviation for each year as well as across all 20 years. As above, this information is stored in two separate tables (one for distribution companies and one for production companies).

## Usage
**1.** This project requires python 3.10 or greater, scrapy 2.8.0, pymysql 1.0.2, python-dotenv 0.21.0, and itemadapter 0.7.0 to be imported; see the requirements.txt file for more information. <br>
**2.** Prior to running the spider, a .env file should be created in the actors_repo directory. This contains:
  - **DB_USER** (the username for connecting to the database) <br>
  - **DB_PSWD** (the password for connecting to the database) <br>
  - **DB_HOST** (the hostname for connecting to the database) <br>
  - **actors_path** (the filepath for storing the actors' box office statistics)
  - **directors_path** (the filepath for storing the directors' box office statistics)
  - **distributors_path** (the filepath for storing the distributors' box office statistics)
  - **production_co_path** (the filepath for storing the production companies' box office statistics)
  
**3.** To run the spider in scrapy, from the command line navigate to the directory Actors/actors_repo and enter the command 
> scrapy crawl actors_wiki_spider
<br>
After this, the spider will perform the steps as outlined above and populate the MySQL database actors.
As the spider crawls and items are created, the log.txt file stores the logging information at the logging.DEBUG level.

**4.** Having completed the above steps, navigate to the subdirectory data_analysis and run the script analytics.py by entering the command
> python analytics.py
<br>
This will create 4 new csv files in the locations specified in the .env file.

## Future Work
In the future, we would like to add functionality to examine which actors have worked together and make predictions about which actors will work together in the future
by considering directors, distributors, production companies, and other actors they have worked with previously. Another direction is to use this data to make box office
predictions.


