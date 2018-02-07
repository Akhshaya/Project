bag1 = load '/user/hive/warehouse/h1b_final/*'  using  PigStorage('\t')  AS (s_no, case_status:chararray, employer_name, soc_name:chararray, job_title:chararray, full_time_position, prevailing_wage, year, worksite, longitute, latitute);

--dump bag1;

bag1_g = GROUP bag1 by $2;

bag1_c = foreach bag1_g generate $0, COUNT(bag1.$1);

--dump bag1_c;

bag1_f = filter bag1 by case_status=='CERTIFIED';

--dump bag1_f;

bag1_g1 = GROUP bag1_f by $2;

bag1_c1 = foreach bag1_g1 generate $0, COUNT(bag1_f.$1);

--dump bag1_c1;

bag1_f1 = filter bag1 by case_status=='CERTIFIED-WITHDRAWN';

--dump bag1_f1;

bag1_g2 = GROUP bag1_f1 by $2;

bag1_c2 = foreach bag1_g2 generate $0, COUNT(bag1_f1.$1);

--dump bag1_c2;

bag1_j = join bag1_c by $0, bag1_c1 by $0, bag1_c2 by $0;

--dump bag1_j;

bag1_success = foreach bag1_j generate $0, (float)($3+$5)/$1*100 ,$1;

--dump bag1_success;

bag1_final = filter bag1_success by $1>70 and $2>=1000;

dump bag1_final;














