
proc datasets lib=work kill nolist memtype=data;
quit;

libname PV 'E:\Drive\Local Disk F\Prof Vikram'; run;
libname PV2 'E:\Drive\Local Disk F\Prof Vikram\Bob results'; run;
libname Final 'E:\Drive\Local Disk F\Morningstar\Data received from Girjinder\Final datasets'; run;

***Import xlsx;

PROC IMPORT OUT= PV.india_funds
            DATAFILE= "E:\Drive\Local Disk F\Prof Vikram\india_funds.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;


PROC IMPORT OUT= PV.Details
            DATAFILE= "E:\Drive\Local Disk F\Prof Vikram\Data.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Details";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;


PROC IMPORT OUT= PV.Fund_Returns
            DATAFILE= "E:\Drive\Local Disk F\Prof Vikram\Data.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Fund_Returns";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;


PROC IMPORT OUT= PV.Benchmark_Returns
            DATAFILE= "E:\Drive\Local Disk F\Prof Vikram\Data.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Benchmark_Returns";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

**Fama-french factors;

PROC IMPORT OUT= PV.FF
            DATAFILE= "E:\Drive\Local Disk F\Prof Vikram\FourFactors_India.csv" 
            DBMS=CSV REPLACE;
RUN;


*converting Fund_Returns from wide to long form;

proc transpose data=PV.Fund_Returns out=PV.Fund_Returns1;
  by Name SecId FundId notsorted;
  var _all_;
run;

data PV.Fund_Returns1;
	set PV.Fund_Returns1;
	Ret=input(col1,12.);
	Date=input(_LABEL_,MMDDYY10.);
	format Date date9. ;
	if _label_ in ('Name','SecId','FundId') then delete;
	drop _name_ _label_ ;
run;
*471360 obs;


*Many characters dont get converted, so had to assign col1 column;
data PV.Fund_Returns1;
	set PV.Fund_Returns1;
	if missing(ret)=1 then ret=col1;
run;

data PV.Fund_Returns1;
	set PV.Fund_Returns1;
	if missing(ret)=1 then delete;
	drop col1;
run;

*converting Benchmark_Returns from wide to long form;

proc transpose data=PV.Benchmark_Returns out=PV.Benchmark_Returns1;
  by Name SecId notsorted;
  var _all_;
run;

data PV.Benchmark_Returns1;
	set PV.Benchmark_Returns1;
	Date=input(_LABEL_,MMDDYY10.);
	format Date date9.;
	if _label_ in ('Name','SecId') then delete;
	drop _name_ _label_;
run;


***Start and end dates of the fund data received;

proc sql;
	create table PV.funds as
	select distinct SecID, FundId, min(date) as FirstDate, max(Date) as LastDate
	from PV.Fund_Returns1
	group by SecID, FundId;
quit;

data PV.funds;
	set PV.funds;
	format FirstDate date9. LastDate date9.;
run;


**Merging data I sent with received;
proc sql;
	create table PV.verification as
	select distinct a.*,b.*
	from PV.India_funds as a left join PV.Funds as b
	on a.FundId=b.FundId;
quit;

data PV.test;
	set PV.verification;
	if missing(FirstDate)=1;
run;
*data for 112 funds out of 448 missing before and after removing missing values;

data PV.test;
	set PV.verification;
	drop SecId;
run;

proc sort data=PV.test nodupkey; by fundid firstdate lastdate; run;

proc sort data=PV.verification nodupkey; by fundid; run;

data PV.verification;
	set PV.verification;
	drop SecId;
run;


**********************************************************************
                             Analysis
*********************************************************************;

*India SecIds and benchmarkIds;

data PV.IndiaMFs;
	set PV.Details;
	if missing(Primary_Prospectus_Benchmark_Id)=0;
	keep SecId FundId Morningstar_Category Primary_Prospectus_Benchmark_Id;
run;

**large cap funds;
data PV2.IndiaMFs_large;
	set PV.IndiaMFs;
	if Morningstar_Category='India Fund Large-Cap';
run;
**small and mid cap funds;
data PV2.IndiaMFs_smid;
	set PV.IndiaMFs;
	if Morningstar_Category='India Fund Small/Mid-Cap';
run;

**merging large and smid cap funds with returns data and then with benchmark returns data;

*(1)Large-cap funds;
proc sql;
	create table PV2.IndiaMFs_large as
	select distinct a.*,b.date, (b.Ret/100) as ret
	from PV2.IndiaMFs_large as a left join PV.fund_returns1 as b
	on a.SecId=b.SecId;
quit;

*176 out of 498 Large cap funds do not have historical returns data;

data PV2.IndiaMFs_large;
	set PV2.IndiaMFs_large;
	if missing(date)=1 then delete;
	drop mkt_ret mkt_ret1;
run;

*Calculating monthly returns;

proc sql;
	create table PV2.IndiaMFs_large as
	select distinct a.*,b.col1 as mkt_ret
	from PV2.IndiaMFs_large as a left join PV.benchmark_returns1 as b
	on a.Primary_Prospectus_Benchmark_Id=b.SecId and month(a.Date)=month(b.Date) and year(a.Date)=year(b.Date);
quit;
data PV2.IndiaMFs_large;
	set PV2.IndiaMFs_large;
	mkt_ret1=mkt_ret/100;
	drop mkt_ret;
run;

*(2) Small & mid-cap funds;

**small and mid cap funds;
data PV2.IndiaMFs_smid;
	set PV.IndiaMFs;
	if Morningstar_Category='India Fund Small/Mid-Cap';
run;

**merging smid and smid cap funds with returns data and then with benchmark returns data;

*(1)smid-cap funds;
proc sql;
	create table PV2.IndiaMFs_smid as
	select distinct a.*,b.date, (b.Ret/100) as ret
	from PV2.IndiaMFs_smid as a left join PV.fund_returns1 as b
	on a.SecId=b.SecId;
quit;

*176 out of 498 smid cap funds do not have historical returns data;

data PV2.IndiaMFs_smid;
	set PV2.IndiaMFs_smid;
	if missing(date)=1 then delete;
	drop mkt_ret mkt_ret1;
run;

*Calculating monthly returns;

proc sql;
	create table PV2.IndiaMFs_smid as
	select distinct a.*,b.col1 as mkt_ret
	from PV2.IndiaMFs_smid as a left join PV.benchmark_returns1 as b
	on a.Primary_Prospectus_Benchmark_Id=b.SecId and month(a.Date)=month(b.Date) and year(a.Date)=year(b.Date);
quit;
data PV2.IndiaMFs_smid;
	set PV2.IndiaMFs_smid;
	mkt_ret1=mkt_ret/100;
	drop mkt_ret;
run;

***Merging funds data with FF factors to run regressions;

*(1)largecap;

proc sql;
	create table PV2.largecap as
	select distinct a.*,b.*
	from PV2.IndiaMFs_large as a left join PV.ff as b
	on year(a.date)=b.year and month(a.date)=b.month
	order by b.year,b.month,a.SecId;
quit;

data PV2.largecap;
	set PV2.largecap;
	Exfundret=Ret-rf;
	Exmktret=mkt_ret1-rf;
	if missing(mkt_ret1)=1 or missing(SMB)=1 then delete;
	if year<2002 then delete;
run;

proc sort data=PV2.largecap; by secid year month ; run;


**loadings;
proc reg data=PV2.largecap noprint tableout outest=PV2.largecap_loadings; 
/*  by secid;*/
  model ExFundRet = Exmktret SMB HML WML ;
quit;


*(2)small & mid cap;

proc sql;
	create table PV2.smidcap as
	select distinct a.*,b.*
	from PV2.IndiaMFs_smid as a left join PV.ff as b
	on year(a.date)=b.year and month(a.date)=b.month
	order by b.year,b.month,a.SecId;
quit;

data PV2.smidcap;
	set PV2.smidcap;
	Exfundret=Ret-rf;
	Exmktret=mkt_ret1-rf;
	if missing(mkt_ret1)=1 or missing(SMB)=1 then delete;
	if year<2002 then delete;
run;

proc sort data=PV2.smidcap; by secid year month ; run;


**loadings;
proc reg data=PV2.smidcap noprint tableout outest=PV2.smidcap_loadings; 
/*  by secid;*/
  model ExFundRet = Exmktret SMB HML WML ;
quit;


*****************************************************
***Fund persistence;
****************************************************;


**(1)Large cap;
**Annual returns;
data PV2.IndiaMFs_large;
	set PV2.IndiaMFs_large;
	year=year(date);
run;

proc sql;
	create table PV2.Annual_largecap as
	select distinct SecId,year,sum(ret) as ret
	from PV2.IndiaMFs_large
	group by SecId,year;
quit;

**Adding next year returns;
proc sql;
	create table PV2.Annual_largecap as
	select distinct a.*,b.ret as ret1
	from PV2.Annual_largecap as a left join PV2.Annual_largecap as b
	on a.secid=b.secid and a.year=b.year-1;
quit;
proc sql;
	create table PV2.Annual_largecap as
	select distinct a.*,b.ret as ret2
	from PV2.Annual_largecap as a left join PV2.Annual_largecap as b
	on a.secid=b.secid and a.year=b.year-2;
quit;
proc sql;
	create table PV2.Annual_largecap as
	select distinct a.*,b.ret as ret3
	from PV2.Annual_largecap as a left join PV2.Annual_largecap as b
	on a.secid=b.secid and a.year=b.year-3;
quit;
proc sql;
	create table PV2.Annual_largecap as
	select distinct a.*,b.ret as ret4
	from PV2.Annual_largecap as a left join PV2.Annual_largecap as b
	on a.secid=b.secid and a.year=b.year-4;
quit;
proc sql;
	create table PV2.Annual_largecap as
	select distinct a.*,b.ret as ret5
	from PV2.Annual_largecap as a left join PV2.Annual_largecap as b
	on a.secid=b.secid and a.year=b.year-5;
quit;

proc sql;
	create table PV2.Annual_largecap as
	select distinct a.*,b.ret as ret_1
	from PV2.Annual_largecap as a left join PV2.Annual_largecap as b
	on a.secid=b.secid and a.year=b.year+1;
quit;
proc sql;
	create table PV2.Annual_largecap as
	select distinct a.*,b.ret as ret_2
	from PV2.Annual_largecap as a left join PV2.Annual_largecap as b
	on a.secid=b.secid and a.year=b.year+2;
quit;
proc sql;
	create table PV2.Annual_largecap as
	select distinct a.*,b.ret as ret_3
	from PV2.Annual_largecap as a left join PV2.Annual_largecap as b
	on a.secid=b.secid and a.year=b.year+3;
quit;
proc sql;
	create table PV2.Annual_largecap as
	select distinct a.*,b.ret as ret_4
	from PV2.Annual_largecap as a left join PV2.Annual_largecap as b
	on a.secid=b.secid and a.year=b.year+4;
quit;
proc sql;
	create table PV2.Annual_largecap as
	select distinct a.*,b.ret as ret_5
	from PV2.Annual_largecap as a left join PV2.Annual_largecap as b
	on a.secid=b.secid and a.year=b.year+5;
quit;



data PV2.Annual_largecap;
	set PV2.Annual_largecap;
	if year<2002 or year>2015 then delete;
	if missing(ret1)=1 or missing(ret2)=1 or missing(ret3)=1 or missing(ret4)=1 or missing(ret5)=1 or
	missing(ret_1)=1 or missing(ret_2)=1 or missing(ret_3)=1 or missing(ret_4)=1 then delete;
	ret_past5yr=sum(ret,ret_1,ret_2,ret_3,ret_4);
	ret_fut5yr=sum(ret1,ret2,ret3,ret4,ret5);
	*if missing(ret1)=1 or missing(ret2)=1 or missing(ret3)=1 or missing(ret_1)=1 or missing(ret_2)=1 then delete; 
	*(for three years);
run;


**dividing into quartiles;

proc sort data=PV2.Annual_largecap; by year ret_past5yr; run;

proc rank data=PV2.Annual_largecap groups=4 out=PV2.Annual_largecap;
  by year;
  var ret_past5yr;
  ranks Rank_ret;
run;

data PV2.Annual_largecap;
  set PV2.Annual_largecap;
  Rank_ret=Rank_ret+1;
run;


**checking the proportion of funds posting greater returns than current median return in next term;

proc sql;
	create table PV2.Annual_largecap1 as
	select distinct year, Rank_ret, median(ret_past5yr) as Median_ret
	from PV2.Annual_largecap
	group by year, Rank_ret;
quit;

proc sql;
	create table PV2.Annual_largecap as
	select distinct a.*,b.*
	from PV2.Annual_largecap as a left join PV2.Annual_largecap1 as b
	on a.rank_ret=b.rank_ret and a.year=b.year;
quit;

data PV2.Annual_largecap;
  set PV2.Annual_largecap;
  if ret_fut5yr>Median_ret then dummy=1;
  else dummy=0;
run;

proc sql;
	create table PV2.Annual_largecap1 as
	select distinct year, Rank_ret,(sum(dummy)/count(distinct secid))*100 as success_rate
	from PV2.Annual_largecap
	group by year, Rank_ret;
quit;

proc sql;
	create table PV2.Annual_largecap1 as
	select distinct a.*,b.success_rate as success_rate_4
	from PV2.Annual_largecap1 as a left join PV2.Annual_largecap1 as b
	on a.rank_ret+3=b.rank_ret and a.year=b.year;
quit;

data PV2.Annual_largecap1;
  set PV2.Annual_largecap1;
  if Rank_ret=1;
  diff=success_rate-success_rate_4;
  drop success ncount;
run;

proc sql;
	create table PV2.Annual_largecap1 as
	select distinct mean(diff) as average, min(diff) as min, max(diff) as max
	from PV2.Annual_largecap1;
quit;



**************************************
**(2) Small/Mid-cap;

**Annual returns;
data PV2.IndiaMFs_smid;
	set PV2.IndiaMFs_smid;
	year=year(date);
run;

proc sql;
	create table PV2.Annual_smidcap as
	select distinct SecId,year,sum(ret) as ret
	from PV2.IndiaMFs_smid
	group by SecId,year;
quit;

**Adding next year returns;
proc sql;
	create table PV2.Annual_smidcap as
	select distinct a.*,b.ret as ret1
	from PV2.Annual_smidcap as a left join PV2.Annual_smidcap as b
	on a.secid=b.secid and a.year=b.year-1;
quit;
proc sql;
	create table PV2.Annual_smidcap as
	select distinct a.*,b.ret as ret2
	from PV2.Annual_smidcap as a left join PV2.Annual_smidcap as b
	on a.secid=b.secid and a.year=b.year-2;
quit;
proc sql;
	create table PV2.Annual_smidcap as
	select distinct a.*,b.ret as ret3
	from PV2.Annual_smidcap as a left join PV2.Annual_smidcap as b
	on a.secid=b.secid and a.year=b.year-3;
quit;
proc sql;
	create table PV2.Annual_smidcap as
	select distinct a.*,b.ret as ret4
	from PV2.Annual_smidcap as a left join PV2.Annual_smidcap as b
	on a.secid=b.secid and a.year=b.year-4;
quit;
proc sql;
	create table PV2.Annual_smidcap as
	select distinct a.*,b.ret as ret5
	from PV2.Annual_smidcap as a left join PV2.Annual_smidcap as b
	on a.secid=b.secid and a.year=b.year-5;
quit;

proc sql;
	create table PV2.Annual_smidcap as
	select distinct a.*,b.ret as ret_1
	from PV2.Annual_smidcap as a left join PV2.Annual_smidcap as b
	on a.secid=b.secid and a.year=b.year+1;
quit;
proc sql;
	create table PV2.Annual_smidcap as
	select distinct a.*,b.ret as ret_2
	from PV2.Annual_smidcap as a left join PV2.Annual_smidcap as b
	on a.secid=b.secid and a.year=b.year+2;
quit;
proc sql;
	create table PV2.Annual_smidcap as
	select distinct a.*,b.ret as ret_3
	from PV2.Annual_smidcap as a left join PV2.Annual_smidcap as b
	on a.secid=b.secid and a.year=b.year+3;
quit;
proc sql;
	create table PV2.Annual_smidcap as
	select distinct a.*,b.ret as ret_4
	from PV2.Annual_smidcap as a left join PV2.Annual_smidcap as b
	on a.secid=b.secid and a.year=b.year+4;
quit;
proc sql;
	create table PV2.Annual_smidcap as
	select distinct a.*,b.ret as ret_5
	from PV2.Annual_smidcap as a left join PV2.Annual_smidcap as b
	on a.secid=b.secid and a.year=b.year+5;
quit;



data PV2.Annual_smidcap;
	set PV2.Annual_smidcap;
	if year<2002 or year>2015 then delete;
	if missing(ret1)=1 or missing(ret2)=1 or missing(ret3)=1 or missing(ret4)=1 or missing(ret5)=1 or
	missing(ret_1)=1 or missing(ret_2)=1 or missing(ret_3)=1 or missing(ret_4)=1 then delete;
	ret_past5yr=sum(ret,ret_1,ret_2,ret_3,ret_4);
	ret_fut5yr=sum(ret1,ret2,ret3,ret4,ret5);
	*if missing(ret1)=1 or missing(ret2)=1 or missing(ret3)=1 or missing(ret_1)=1 or missing(ret_2)=1 then delete; 
	*(for three years);
run;


**dividing into quartiles;

proc sort data=PV2.Annual_smidcap; by year ret_past5yr; run;

proc rank data=PV2.Annual_smidcap groups=4 out=PV2.Annual_smidcap;
  by year;
  var ret_past5yr;
  ranks Rank_ret;
run;

data PV2.Annual_smidcap;
  set PV2.Annual_smidcap;
  Rank_ret=Rank_ret+1;
run;


**checking the proportion of funds posting greater returns than current median return in next term;

proc sql;
	create table PV2.Annual_smidcap1 as
	select distinct year, Rank_ret, median(ret_past5yr) as Median_ret
	from PV2.Annual_smidcap
	group by year, Rank_ret;
quit;

proc sql;
	create table PV2.Annual_smidcap as
	select distinct a.*,b.*
	from PV2.Annual_smidcap as a left join PV2.Annual_smidcap1 as b
	on a.rank_ret=b.rank_ret and a.year=b.year;
quit;

data PV2.Annual_smidcap;
  set PV2.Annual_smidcap;
  if ret_fut5yr>Median_ret then dummy=1;
  else dummy=0;
run;

proc sql;
	create table PV2.Annual_smidcap1 as
	select distinct year, Rank_ret,(sum(dummy)/count(distinct secid))*100 as success_rate
	from PV2.Annual_smidcap
	group by year, Rank_ret;
quit;

proc sql;
	create table PV2.Annual_smidcap1 as
	select distinct a.*,b.success_rate as success_rate_4
	from PV2.Annual_smidcap1 as a left join PV2.Annual_smidcap1 as b
	on a.rank_ret+3=b.rank_ret and a.year=b.year;
quit;

data PV2.Annual_smidcap1;
  set PV2.Annual_smidcap1;
  if Rank_ret=1;
  diff=success_rate-success_rate_4;
  drop success ncount;
run;

proc sql;
	create table PV2.Annual_smidcap1 as
	select distinct mean(diff) as average, min(diff) as min, max(diff) as max
	from PV2.Annual_smidcap1;
quit;

**combining large, small &mid cap companies;
data PV2.largesmid;
  set PV2.largecap Pv2.smidcap;
run;




















