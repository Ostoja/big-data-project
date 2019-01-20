#Iz dataset-a title.basics.tsv sa IMDb-a filtrira filmove koji su izasli 2000. i posle.
import sys


for line in sys.stdin:
    try: 
        
        tconst = "-1"  
        titleType = "-1"  
        primaryTitle = "-1" 
        originalTitle = "-1"  
	isAdult = "-1" 
        startYear = "-1"  
        endYear = "-1"  
        runtimeMinutes = "-1"  
        genres = "-1"

       
        line = line.strip()
        if "tconst" in line:
             continue
        splits = line.split("\t")
        if len(splits) == 9 and splits[1]=="movie" and int(splits[5])>=2000: 
            print splits
            tconst = splits[0]
            titleType = splits[1]
            primaryTitle = splits[2]
            originalTitle = splits[3]
	    isAdult = splits[4]
            startYear = splits[5]
            endYear = splits[6]
            runtimeMinutes = splits[7]
            genres = splits[8]
        with open('movie2000.tsv', 'a') as singleFile:
                 if tconst != "-1":
                       newLine = tconst+ "\t" + titleType+ "\t" + primaryTitle+ "\t" + originalTitle + "\t" + isAdult+ "\t" + startYear+ "\t" + endYear+ "\t" + runtimeMinutes+ "\t" + genres +"\n"
                       singleFile.write(newLine)
         
        #print '%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s^%s' % (tconst, ordering, nconst, category, job, characters, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
    except:  
        pass
