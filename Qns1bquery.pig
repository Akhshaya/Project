A = load '/user/hive/warehouse/h1b_final/*'  using  PigStorage('\t')  AS (s_no, case_status:chararray, employer_name, soc_name:chararray, job_title:chararray, full_time_position, prevailing_wage, year, worksite, longitute, latitute);

--dump A;

A_f1 = filter A by year=='2011';

--dump A_f1;

A_g1 = group A_f1 by $4;

--dump A_g1;

A_gp1 = foreach A_g1 generate group, COUNT(A_f1.$1);

--dump A_gp1;
 
A_f2 = filter A by year=='2012';

--dump A_f2;

A_g2 = group A_f2 by $4;
  
A_gp2 = foreach A_g2 generate group, COUNT(A_f2.$1);

A_f3 = filter A by year=='2013';

--dump A_f3;

A_g3 = group A_f3 by $4;
  
A_gp3 = foreach A_g3 generate group, COUNT(A_f3.$1);

A_f4 = filter A by year=='2014';

--dump A_f4;

A_g4 = group A_f4 by $4;
  
A_gp4 = foreach A_g4 generate group, COUNT(A_f4.$1);

A_f5 = filter A by year=='2015';

--dump A_f5;

A_g5 = group A_f5 by $4;
  
A_gp5 = foreach A_g5 generate group, COUNT(A_f5.$1);

A_f6 = filter A by year=='2016';

--dump A_f6;

A_g6 = group A_f6 by $4;
  
A_gp6 = foreach A_g6 generate group, COUNT(A_f6.$1);


A_j = join A_gp1 by $0 ,A_gp2 by $0 ,A_gp3 by $0 ,A_gp4 by $0 ,A_gp5 by $0 ,A_gp6 by $0;

--dump A_j;

A_j1 = foreach A_j generate $0 , $1 , $3 ,$5 ,$7 ,$9 ,$11;

A_growth = foreach A_j1 generate $0 , (float)(($2-$1)/$1)*100, (float)(($3-$2)/$2)*100,(float)(($4-$3)/$3)*100, (float)(($5-$4)/$4)*100, (float)(($6-$5)/$5)*100;

avg_growth = foreach A_growth generate $0 , ($1+$2+$3+$4+$5)/5;

A_lmt = limit (order avg_growth by $1 desc) 5;

dump A_lmt;








