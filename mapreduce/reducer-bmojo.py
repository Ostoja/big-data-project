#Reducer za spajanje tabela bmojoweek.tsv i bmojoalltsv.
import sys

weekend_dict = {}
movie_dict = {}

for line in sys.stdin:
      line = line.strip()
      
      try:
            
            movie_id, movie_title, domestic_total_gross, release_date, close_date, in_release_days, runtime_mins, rating, genre, distributor, director, producer, production_budget,  widest_release_theaters, actors, writers, cinematographers, composers, year, calendarWeek, date, rank, boxOffice, theatres, grossBoxOffice, longWeekend = line.split('^')
            
            if director != "-1": #bmojoall data
                 movie_dict[movie_id] = (movie_id, movie_title, domestic_total_gross, release_date, close_date, in_release_days, runtime_mins, rating, genre, distributor, director, producer, production_budget,  widest_release_theaters, actors, writers, cinematographers, composers)
             
            else: #bmojoweek data
                 
                 weekend_dict[movie_id + "," + calendarWeek] = (year, calendarWeek, date, rank, boxOffice, theatres, grossBoxOffice, longWeekend)
                 
                 
      except:
            pass

with open('bmojoreduced.tsv', 'a') as singleFile: 
                 print("D")
                 for key in weekend_dict.keys():
                      
                      splitkey = key.split(",")
                      movie_id = splitkey[0]
                      calendarWeek = splitkey[1]
                      
                      movie_id, movie_title, domestic_total_gross, release_date, close_date, in_release_days, runtime_mins, rating, genre, distributor, director, producer, production_budget,  widest_release_theaters, actors, writers, cinematographers, composers= movie_dict[movie_id]
                      year, calendarWeek, date, rank, boxOffice, theatres, grossBoxOffice, longWeekend = weekend_dict[key]
                      newLine = movie_id + "\t" + movie_title + "\t" + domestic_total_gross + "\t" + release_date+ "\t" + close_date+ "\t" + in_release_days+ "\t" + runtime_mins+ "\t" + rating+ "\t" + genre + "\t" + distributor + "\t" + director + "\t" + producer + "\t" + production_budget + "\t" + widest_release_theaters + "\t" + actors + "\t" + writers + "\t" + cinematographers + "\t" + composers + "\t" + year + "\t" + calendarWeek + "\t" + date + "\t" + rank + "\t" + boxOffice + "\t" + theatres + "\t" + grossBoxOffice + "\t" + longWeekend +"\n"
                      
                      singleFile.write(newLine)
                      
