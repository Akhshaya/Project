hadoop fs -rm -r /H1b_final/2aop
hadoop jar work.jar WorkLocation /H1b_final/* /H1b_final/2aop
hadoop fs -cat /H1b_final/2aop/p*
