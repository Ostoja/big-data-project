```
python sender.py | nc -l -p 1111

$SPARK_HOME/bin/submit ./receiver.py localhost 1111
```
