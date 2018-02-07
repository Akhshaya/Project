hadoop fs -rm -r /H1b_final/7op
hadoop jar count.jar NumberOfApplications /H1b_final/* /H1b_final/7op
hadoop fs -cat /H1b_final/7op/p*
