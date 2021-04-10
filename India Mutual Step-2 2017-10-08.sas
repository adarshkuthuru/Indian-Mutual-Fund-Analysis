


proc datasets lib=work kill nolist memtype=data;
quit;


*(B) Point 4: Return Analysis: Rolling 5 year period;



options symbolgen;
%macro doit;
%do i=2002 %to 2012;

*(1)Largecap funds;
proc sql;
	create table largecap_annual as
	select distinct secid, year, sum(Ret) as SFundRet, mean(SMB) as SSMB, mean(HML) as SHML, mean(WML) as SWML,
	sum(Exfundret) as SExfundret, sum(Exmktret) as SExmktret, sum(rf) as RF_annual
	from PV2.largecap
	group by year,secid
	having count(distinct month)=12;
quit;

data largecap_annual;
  set largecap_annual;
  if year>=&i and year<=&i+4;
run;

proc sql;
	create table largecap_bob33 as
	select distinct count(distinct SecId) as nfunds,mean(SExfundret) as Mean_ret, std(SExfundret) as Std_ret, median(SExfundret) as Median_ret,
	min(SExfundret) as Min_ret,max(SExfundret) as Max_ret, mean(SExfundret)/std(SExfundret) as Sharpe_ratio,1 as id
	from largecap_annual;
quit;

**top and bottom quartile returns;
proc sort data=largecap_annual; by year secid;run;

proc rank data=largecap_annual groups=4 out=largecap_annual1;
  by year;
  var SExfundret;
  ranks Rank_ret;
run;

data largecap_annual1;
  set largecap_annual1;
  if Rank_ret=0 or Rank_ret=3;
run;

proc sql;
	create table largecap_bob331 as
	select distinct Rank_ret,mean(SExfundret) as Mean_ret
	from largecap_annual1
	group by Rank_ret;
quit;
 
proc transpose data=largecap_bob331 out=largecap_bob331 prefix=Rank;
/*    by year;*/
    id Rank_ret;
    var Mean_ret;
run;


data largecap_bob331;
	set largecap_bob331;
	Top_minus_bottom_quartile_Exret=Rank3-Rank0;
	id=1;
	rename Rank3=Top_quartile;
	rename Rank0=bottom_quartile;
	drop _NAME_;
run;

proc sql;
	create table largecap_bob33 as
	select distinct a.*,b.*
	from largecap_bob33 as a, largecap_bob331 as b
	where a.id=b.id;

	drop table largecap_bob331;
run;

**Regressions;
proc reg data=largecap_annual noprint tableout outest=largecap_Alpha;
  model SExfundret = SExmktret; *CAPM;
  model SExfundret = SExmktret SSMB SHML SWML; *4-factor;
quit;

data largecap_Alpha;
  set largecap_Alpha;
  where _Type_ in ('PARMS','T');
  keep _model_ _Type_ Intercept;
  if _Type_='PARMS' then Intercept=Intercept;
  rename Intercept=Alpha;
  rename _Type_ =Stat;
run;

data largecap_Alpha;
  set largecap_Alpha;
  nrow=_N_;
run;

proc transpose data=largecap_Alpha out=largecap_Alpha;
    id nrow;
    var Alpha;
run;
data largecap_Alpha;
  set largecap_Alpha;
  id=1;
  rename _1=CAPM_Alpha;
  rename _2=Tstat_CAPM_Alpha;
  rename _3=Four_factor_Alpha;
  rename _4=Tstat_Four_factor_Alpha;
  drop _NAME_ _LABEL_;
run;

proc sql;
	create table largecap_bob33 as
	select distinct a.*,b.*
	from largecap_bob33 as a, largecap_Alpha as b
	where a.id=b.id;
run;


proc sql;
	create table largecap_bob331 as
	select distinct secid, count(distinct year) as nfunds,mean(SExfundret) as Mean_ret, std(SExfundret) as Std_ret, mean(SExfundret)/std(SExfundret) as Sharpe_ratio,1 as id
	from largecap_annual
	group by secid;

	create table largecap_bob331 as
	select distinct a.*,b.Sharpe_ratio as Sharpe_ratio_benchmark
	from largecap_bob331 as a, largecap_bob33 as b
	where a.id=b.id;
quit;

data largecap_bob331;
  set largecap_bob331;
  if Sharpe_ratio>=Sharpe_ratio_benchmark;
run;

proc sql;
	create table largecap_bob331 as
	select distinct count(distinct SecId) as ncount,1 as id
	from largecap_bob331;

	create table largecap_bob33 as
	select distinct a.*,(b.ncount/a.nfunds)*100 as Precent_funds_beating_sharpe
	from largecap_bob33 as a, largecap_bob331 as b
	where a.id=b.id;

	drop table largecap_Alpha, largecap_annual, largecap_annual1,largecap_bob331;
quit;

data largecap_bob33;
	set largecap_bob33;
	year=&i+4;
	drop _NAME_;
run;

**Appending to the final dataset;
proc append data=largecap_bob33 base=test1; run;


%end;
%mend doit;
%doit

*(2)Small & Mid cap funds;
options symbolgen;
%macro doit;
%do i=2002 %to 2012;

proc sql;
	create table smidcap_annual as
	select distinct secid, year, sum(Ret) as SFundRet, mean(SMB) as SSMB, mean(HML) as SHML, mean(WML) as SWML,
	sum(Exfundret) as SExfundret, sum(Exmktret) as SExmktret, sum(rf) as RF_annual
	from PV2.smidcap
	group by year,secid
	having count(distinct month)=12;
quit;

data smidcap_annual;
  set smidcap_annual;
  if year>=&i and year<=&i+4;
run;

proc sql;
	create table smidcap_bob33 as
	select distinct count(distinct SecId) as nfunds,mean(SExfundret) as Mean_ret, std(SExfundret) as Std_ret, median(SExfundret) as Median_ret,
	min(SExfundret) as Min_ret,max(SExfundret) as Max_ret, mean(SExfundret)/std(SExfundret) as Sharpe_ratio,1 as id
	from smidcap_annual;
quit;

**top and bottom quartile returns;
proc sort data=smidcap_annual; by year secid;run;

proc rank data=smidcap_annual groups=4 out=smidcap_annual1;
  by year;
  var SExfundret;
  ranks Rank_ret;
run;

data smidcap_annual1;
  set smidcap_annual1;
  if Rank_ret=0 or Rank_ret=3;
run;

proc sql;
	create table smidcap_bob331 as
	select distinct Rank_ret,mean(SExfundret) as Mean_ret
	from smidcap_annual1
	group by Rank_ret;
quit;
 
proc transpose data=smidcap_bob331 out=smidcap_bob331 prefix=Rank;
/*    by year;*/
    id Rank_ret;
    var Mean_ret;
run;


data smidcap_bob331;
	set smidcap_bob331;
	Top_minus_bottom_quartile_Exret=Rank3-Rank0;
	id=1;
	rename Rank3=Top_quartile;
	rename Rank0=bottom_quartile;
	drop _NAME_;
run;

proc sql;
	create table smidcap_bob33 as
	select distinct a.*,b.*
	from smidcap_bob33 as a, smidcap_bob331 as b
	where a.id=b.id;

	drop table smidcap_bob331;
run;

**Regressions;
proc reg data=smidcap_annual noprint tableout outest=smidcap_Alpha;
  model SExfundret = SExmktret; *CAPM;
  model SExfundret = SExmktret SSMB SHML SWML; *4-factor;
quit;

data smidcap_Alpha;
  set smidcap_Alpha;
  where _Type_ in ('PARMS','T');
  keep _model_ _Type_ Intercept;
  if _Type_='PARMS' then Intercept=Intercept;
  rename Intercept=Alpha;
  rename _Type_ =Stat;
run;

data smidcap_Alpha;
  set smidcap_Alpha;
  nrow=_N_;
run;

proc transpose data=smidcap_Alpha out=smidcap_Alpha;
    id nrow;
    var Alpha;
run;
data smidcap_Alpha;
  set smidcap_Alpha;
  id=1;
  rename _1=CAPM_Alpha;
  rename _2=Tstat_CAPM_Alpha;
  rename _3=Four_factor_Alpha;
  rename _4=Tstat_Four_factor_Alpha;
  drop _NAME_ _LABEL_;
run;

proc sql;
	create table smidcap_bob33 as
	select distinct a.*,b.*
	from smidcap_bob33 as a, smidcap_Alpha as b
	where a.id=b.id;
run;


proc sql;
	create table smidcap_bob331 as
	select distinct secid, count(distinct year) as nfunds,mean(SExfundret) as Mean_ret, std(SExfundret) as Std_ret, mean(SExfundret)/std(SExfundret) as Sharpe_ratio,1 as id
	from smidcap_annual
	group by secid;

	create table smidcap_bob331 as
	select distinct a.*,b.Sharpe_ratio as Sharpe_ratio_benchmark
	from smidcap_bob331 as a, smidcap_bob33 as b
	where a.id=b.id;
quit;

data smidcap_bob331;
  set smidcap_bob331;
  if Sharpe_ratio>=Sharpe_ratio_benchmark;
run;

proc sql;
	create table smidcap_bob331 as
	select distinct count(distinct SecId) as ncount,1 as id
	from smidcap_bob331;

	create table smidcap_bob33 as
	select distinct a.*,(b.ncount/a.nfunds)*100 as Precent_funds_beating_sharpe
	from smidcap_bob33 as a, smidcap_bob331 as b
	where a.id=b.id;

	drop table smidcap_Alpha, smidcap_annual, smidcap_annual1,smidcap_bob331;
quit;


data smidcap_bob33;
	set smidcap_bob33;
	year=&i+4;
	drop _NAME_;
run;

**Appending to the final dataset;
proc append data=smidcap_bob33 base=test2; run;


%end;
%mend doit;
%doit


options symbolgen;
%macro doit;
%do i=2002 %to 2012;
*(3)Combo of Largecap, Small & Mid cap funds;
proc sql;
	create table largesmidcap_annual as
	select distinct secid, year, sum(Ret) as SFundRet, mean(SMB) as SSMB, mean(HML) as SHML, mean(WML) as SWML,
	sum(Exfundret) as SExfundret, sum(Exmktret) as SExmktret, sum(rf) as RF_annual
	from PV2.largesmid
	group by year,secid
	having count(distinct month)=12;
quit;

data largesmidcap_annual;
  set largesmidcap_annual;
  if year>=&i and year<=&i+4;
run;

proc sql;
	create table largesmidcap_bob33 as
	select distinct count(distinct SecId) as nfunds,mean(SExfundret) as Mean_ret, std(SExfundret) as Std_ret, median(SExfundret) as Median_ret,
	min(SExfundret) as Min_ret,max(SExfundret) as Max_ret, mean(SExfundret)/std(SExfundret) as Sharpe_ratio,1 as id
	from largesmidcap_annual;
quit;

**top and bottom quartile returns;
proc sort data=largesmidcap_annual; by year secid;run;

proc rank data=largesmidcap_annual groups=4 out=largesmidcap_annual1;
  by year;
  var SExfundret;
  ranks Rank_ret;
run;

data largesmidcap_annual1;
  set largesmidcap_annual1;
  if Rank_ret=0 or Rank_ret=3;
run;

proc sql;
	create table largesmidcap_bob331 as
	select distinct Rank_ret,mean(SExfundret) as Mean_ret
	from largesmidcap_annual1
	group by Rank_ret;
quit;
 
proc transpose data=largesmidcap_bob331 out=largesmidcap_bob331 prefix=Rank;
/*    by year;*/
    id Rank_ret;
    var Mean_ret;
run;


data largesmidcap_bob331;
	set largesmidcap_bob331;
	Top_minus_bottom_quartile_Exret=Rank3-Rank0;
	id=1;
	rename Rank3=Top_quartile;
	rename Rank0=bottom_quartile;
	drop _NAME_;
run;

proc sql;
	create table largesmidcap_bob33 as
	select distinct a.*,b.*
	from largesmidcap_bob33 as a, largesmidcap_bob331 as b
	where a.id=b.id;

	drop table largesmidcap_bob331;
run;

**Regressions;
proc reg data=largesmidcap_annual noprint tableout outest=largesmidcap_Alpha;
  model SExfundret = SExmktret; *CAPM;
  model SExfundret = SExmktret SSMB SHML SWML; *4-factor;
quit;

data largesmidcap_Alpha;
  set largesmidcap_Alpha;
  where _Type_ in ('PARMS','T');
  keep _model_ _Type_ Intercept;
  if _Type_='PARMS' then Intercept=Intercept;
  rename Intercept=Alpha;
  rename _Type_ =Stat;
run;

data largesmidcap_Alpha;
  set largesmidcap_Alpha;
  nrow=_N_;
run;

proc transpose data=largesmidcap_Alpha out=largesmidcap_Alpha;
    id nrow;
    var Alpha;
run;
data largesmidcap_Alpha;
  set largesmidcap_Alpha;
  id=1;
  rename _1=CAPM_Alpha;
  rename _2=Tstat_CAPM_Alpha;
  rename _3=Four_factor_Alpha;
  rename _4=Tstat_Four_factor_Alpha;
  drop _NAME_ _LABEL_;
run;

proc sql;
	create table largesmidcap_bob33 as
	select distinct a.*,b.*
	from largesmidcap_bob33 as a, largesmidcap_Alpha as b
	where a.id=b.id;
run;


proc sql;
	create table largesmidcap_bob331 as
	select distinct secid, count(distinct year) as nfunds,mean(SExfundret) as Mean_ret, std(SExfundret) as Std_ret, mean(SExfundret)/std(SExfundret) as Sharpe_ratio,1 as id
	from largesmidcap_annual
	group by secid;

	create table largesmidcap_bob331 as
	select distinct a.*,b.Sharpe_ratio as Sharpe_ratio_benchmark
	from largesmidcap_bob331 as a, largesmidcap_bob33 as b
	where a.id=b.id;
quit;

data largesmidcap_bob331;
  set largesmidcap_bob331;
  if Sharpe_ratio>=Sharpe_ratio_benchmark;
run;

proc sql;
	create table largesmidcap_bob331 as
	select distinct count(distinct SecId) as ncount,1 as id
	from largesmidcap_bob331;

	create table largesmidcap_bob33 as
	select distinct a.*,(b.ncount/a.nfunds)*100 as Precent_funds_beating_sharpe
	from largesmidcap_bob33 as a, largesmidcap_bob331 as b
	where a.id=b.id;

	drop table largesmidcap_Alpha, largesmidcap_annual, largesmidcap_annual1,largesmidcap_bob331;
quit;


data largesmidcap_bob33;
	set largesmidcap_bob33;
	year=&i+4;
	drop _NAME_;
run;

**Appending to the final dataset;
proc append data=largesmidcap_bob33 base=test3; run;


%end;
%mend doit;
%doit


proc append base=test4 data=test1;
run;
proc append base=test4 data=test2;
run;
proc append base=test4 data=test3;
run;
