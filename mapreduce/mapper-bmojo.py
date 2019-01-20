#Mapper za spajanje tabela bmojoweek.tsv i bmojoall.tsv.
import sys


for line in sys.stdin:
    try: 
        
        movie_id = "-1"  
        year = "-1"  
        calendarWeek = "-1"  
        date = "-1"  
	rank = "-1"  
        boxOffice = "-1"  
        theatres = "-1"  
        grossBoxOffice = "-1"  
        longWeekend = "-1"

	movie_title = "-1"
        domestic_total_gross = "-1"
        foreign_total_gross = "-1"
        opening_weekend = "-1"
        opening_weekend_limited = "-1"
        opening_weekend_wide = "-1"
        release_date = "-1"
        close_date = "-1"
        in_release_days = "-1"
        runtime_mins = "-1"
        rating = "-1"
        genre = "-1"
        distributor = "-1"
        director = "-1"
        producer = "-1"
        production_budget = "-1"
        widest_release_theaters = "-1"
        actors = "-1"
        writers = "-1"
        cinematographers = "-1"
        composers = "-1"

        line = line.strip()
        if "movie_id" in line:
             continue
        splits = line.split("\t")
        if(len(splits)!=22 and len(splits)!=9):
             print len(splits)
        if len(splits) == 22:  # bmojoall data
            movie_id,movie_title,domestic_total_gross,foreign_total_gross,opening_weekend,opening_weekend_limited,opening_weekend_wide,release_date,close_date,in_release_days,runtime_mins,rating,genre,distributor,director,producer,production_budget,widest_release_theaters,actors,writers,cinematographers,composers = splits
        else:  # bmojoweeend data
            movie_id = splits[0]
            year = splits[1]
            calendarWeek = splits[2]
            date = splits[3]
	    rank = splits[4]
            boxOffice = splits[5]
            theatres = splits[6]
            grossBoxOffice = splits[7]
            longWeekend = splits[8]
       
        print '%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s' % (movie_id, movie_title, domestic_total_gross, release_date, close_date, in_release_days, runtime_mins, rating, genre, distributor, director, producer, production_budget, widest_release_theaters, actors, writers, cinematographers, composers, year, calendarWeek, date, rank, boxOffice, theatres, grossBoxOffice, longWeekend)
    except:  
        pass
