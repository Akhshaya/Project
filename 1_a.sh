hadoop fs -rm -r /H1b_final/1aop
hadoop jar data.jar DataEngineerJob /H1b_final/* /H1b_final/1aop
hadoop fs -cat /H1b_final/1aop/p*
