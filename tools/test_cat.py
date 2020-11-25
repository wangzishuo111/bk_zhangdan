import cat
import time

 
#cat.init("cat.mysql")
cat.init("kd.hbase")

t = cat.Transaction("Trans", "t3")

#cat.log_event("Event", "E1")
time.sleep(0.3)
t.complete()

 
