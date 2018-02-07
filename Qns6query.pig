A = load '/user/hive/warehouse/h1b_final/*'  using  PigStorage('\t')  AS (s_no, case_status:chararray, employer_name, soc_name:chararray, job_title:chararray, full_time_position, prevailing_wage, year, worksite, longitute, latitute);

--dump A;

A1 = foreach A generate year,case_status;

--dump A1;

A1_g = group A1 by year;

--dump A1_g;

A1_t = foreach A1_g generate group, COUNT(A1.$1);

--dump A1_t;

A1_g1 = group A1 by (year,case_status);

--dump A1_g1;

A1_c1 = foreach A1_g1 generate group, group.year, COUNT(A1);

--dump A1_c1;

A1_j = join A1_c1 by $1, A1_t by $0;

--dump A1_j;

A1_f = foreach A1_j generate FLATTEN($0), (float)($2*100)/$4,$2;

dump A1_f;

--A_op = store A1_f into '/home/hduser/query6pig';
