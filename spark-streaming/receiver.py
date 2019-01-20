#Prima podatke koje salje sender.py preko netcat-a i vrsi kategorizaciju primljenih podataka.

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("receiver.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
    sc = SparkContext(appName="NatalitySmokerCounter")
	
    sc.setLogLevel("ERROR")
	
    ssc = StreamingContext(sc, 2)
	
    ssc.checkpoint("checkpoint")

    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)
    
    def mapFunc(line):
        line = str(line)
        splits = line.split(",")
        for i in range(0, len(splits)):
             if "c" in splits[i] or ""==splits[i]:
                  return (i, i)
             splits[i] = int(splits[i])
        if splits[0]==0 and splits[1]==0 and splits[2]==0 and splits[3]==0:
             return ("nonsmoker", 1)
        if splits[0]>50 and splits[1]==0 and splits[2]==0 and splits[3]==0:
             return ("heavy smoker that quit smoking", 1)
        if splits[0]>0 and splits[1]==0 and splits[2]==0 and splits[3]==0:
             return ("quit smoking", 1)
        if splits[0]>50 and splits[1]>50 and splits[2]>50 and splits[3]>50:
             return ("heavy smoker", 1)
        if splits[0]>50 and splits[1]<5 and splits[2]<5 and splits[3]<5:
             return ("heavy smoker that reduced number of smoked cigarettes", 1)
        if splits[0]<5 and splits[1]<5 and splits[2]<5 and splits[3]<5 and splits[0]!=0 and splits[1]!=0 and splits[2]!=0 and splits[3]!=0:
             return ("occasional smoker", 1)
        if splits[0]==0 and splits[1]>50 and splits[2]>50 and splits[3]>50:
             return ("from nonsmoker to heavy smoker during pregnancy", 1)
        if splits[0]==0 and (splits[1]>0 or splits[2]>0 or splits[3]>0):
             return ("started smoking when she became pregnant", 1)
        if splits[1]>0 or splits[2]>0 or splits[3]>0:
             return ("smoked during the pregnancy, uncategorized before", 1)
        
        return ("uncategorized", 1)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    running_counts = lines.flatMap(lambda line: line.split(" "))\
                          .map(mapFunc)\
                          .updateStateByKey(updateFunc)

    running_counts.pprint()

    ssc.start()
    ssc.awaitTermination()
