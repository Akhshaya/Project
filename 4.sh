hadoop fs -rm -r /H1b_final/4op
hadoop jar top.jar Top5Employeer /H1b_final/* /H1b_final/4op
hadoop fs -cat /H1b_final/4op/p*
