# Actors
This project uses scrapy and pymysql to scrape Wikipedia for film information and store the data in a MySQL database.

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

**4.** These items are then passed to the item pipeline, the first step of which is the **DatePipeline**. If the item is a MovieItem, this class cleans the release_date field and convert it into the 
numerical YYYY-MM-DD or drops the MovieItem if the release_date is None. Otherwise the item is returned as is. <br>
**5.** The second step is the **MoneyPipeline**, which cleans the budget and box_office fields (if the item is a MovieItem) so that they are represented as decimals with 6 places to the right of the decimal; 
it is understood that these fields are now in terms of millions of dollars. If the item is not a MovieItem, it is returned as is. <br>
**6.** Having cleaned the relevant fields for insertion into a MySQL database, the **DBPipeline** creates a MySQL database titled actors whose schema can be found in the 
actors_wiki_ER_diagram pdf.

## Usage
**1.** This project requires scrapy 2.8.0, pymysql 1.0.2, python-dotenv 0.21.0, and itemadapter 0.7.0 to be imported; see the requirements.txt file for more information. <br>
**2.** Prior to running the spider, a .env file should be created in the actors_wiki/actors_wiki directory. This contains:
  - **DB_USER** (the username for connection to the database) <br>
  - **DB_PSWD** (the password for connecting to the database) <br>
  - **DB_HOST** (the hostname for connecting to the database) <br>
  
**3.** To run the spider in scrapy, from the command line navigate to the directory Actors/actors_wiki and enter the command 
> scrapy crawl actors_wiki_spider
<br>
After this, the spider will perform the steps as outlined above and populate the MySQL database actors.

## Future Work
In the future, we would like to add functionality to examine which actors have worked together and make predictions about which actors will work together in the future
by considering directors, distributors, production companies, and other actors they have worked with previously. Another direction is to use this data to make box office
predictions.


