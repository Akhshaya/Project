A =  LOAD  '/user/hive/warehouse/h1b_final/*'  USING PigStorage('\t')  AS  (s_no, case_status:chararray, employer_name, soc_name:chararray, job_title:chararray, full_time_position, prevailing_wage, year, worksite, longitute, latitute);

--dump A;

A_f = filter A by case_status=='CERTIFIED' and job_title=='DATA SCIENTIST';

--dump A_f;

A_g = GROUP A_f by $3;

A_c = foreach A_g generate $0, COUNT(A_f.$4);

--dump A_c;

A_ord = ORDER A_c BY $1 DESC;
  
A_lmt = LIMIT A_ord 1;
  
Dump A_lmt;

