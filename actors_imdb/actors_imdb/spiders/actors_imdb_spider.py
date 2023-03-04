import scrapy
import pandas


class Actors_imdb(scrapy.Spider):
    """
    This class is used to scrape a particular wikipedia page,
    find a table, and save the table in CSV format.
    """
    name = 'actors'
    allowed_domains = ["https://www.imdb.com/"]
    start_urls = ["https://www.imdb.com/title/tt0111161/fullcredits?ref_=tt_cl_sm"]

# consider the following xpaths to get the anchor tags of the actors:
# response.xpath('//table[@class="cast_list"]/tr[@class="odd"]/td[not(@*)]/a/@href').getall()
# response.xpath('//table[@class="cast_list"]/tr[@class="even"]/td[not(@*)]/a/@href').getall()
# and use these xpaths to get the actor names
# response.xpath('//table[@class="cast_list"]/tr[@class="odd"]/td[not(@*)]/a/text()').getall()
# response.xpath('//table[@class="cast_list"]/tr[@class="even"]/td[not(@*)]/a/text()').getall()
# To filter we can consider
# response.xpath('//table[@class="cast_list"]/tr[@class="even"]/td[@class="character"]/text').getall(),
# zip this list with the two above (actor anchor tags and names, both for evens),
# then see if the second entry contains "uncredited" after removing whitespace and newlines.

# Still need to find movie name to add to table, consider how to click on the actor anchor tag,
# and how to select movies from each actor's page.

    def parse(self, response):
        """
        This method scrapes the webpage given in start_urls for the table with
        of finite groups of order up to 30, then it processes the table and
        saves the table in CSV format to a file finite_groups.csv
        """

        table = response.xpath('//table[@class="wikitable"]')            #select the table labeled wikitable
        column_names = table.xpath('tbody//tr')[0]                       #select the first row of the table
        column_list = []
        for item in column_names.xpath('th'):
            try:                                                         #Some column names were stored using
                text_1 = item.xpath('text()').extract()[0].rstrip()      #two tags: <th> and <i>
                text_2 = item.xpath('i/text()').extract()[0].rstrip()    #We want the first item of the list yielded by
                column_list.append('{} {}'.format(text_1, text_2))       #extract(), and the whitespace must be stripped
            except:
                text_1 = item.xpath('text()').extract()[0].rstrip()      #Other column names were stored only using <th>
                column_list.append(text_1)
                                                                         #In both cases we append the column name to our
                                                                         # list of column_names: column_list

        rows_list = []
        for row in table.xpath('tbody//tr')[1:]:                         #select the remaining rows of the table
            row_list = []                                                #this is the list of entries in the row

            for item in ['th/text()', 'td/text()']:                      #Some entries in the row have the <th> tag
                for _ in row.xpath(item).extract():                      #while others have the <td> tag
                    row_list.append(_.rstrip())                          #In both cases we must strip the whitespace
            rows_list.append(row_list)

        with open('finite_groups.csv', 'w') as _f:
            writer=csv.writer(_f)
            writer.writerow(column_list)                                 #write the column list to the first row
            writer.writerows(rows_list)                                  #write the other rows from rows_list
