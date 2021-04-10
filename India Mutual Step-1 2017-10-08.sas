proc datasets lib=work kill nolist memtype=data;
quit;

libname PV 'E:\Drive\Local Disk F\Prof Vikram'; run;
libname PV2 'E:\Drive\Local Disk F\Prof Vikram\Bob results'; run;
libname Final 'E:\Drive\Local Disk F\Morningstar\Data received from Girjinder\Final datasets'; run;

*****************************************************
*************stats told by Bob Jones;
****************************************************;

*(A) Point 3(3): Returns;

*(1)Largecap funds;
data PV2.largecap;
	set PV2.largecap;
	year=year(date);
run;

proc sql;
	create table largecap_annual as
	select distinct Fundid, secid, year, sum(Ret) as SFundRet, mean(SMB) as SSMB, mean(HML) as SHML, mean(WML) as SWML,
	sum(Exfundret) as SExfundret, sum(Exmktret) as SExmktret, sum(rf) as RF_annual
	from PV2.largecap
	group by year,secid
	having count(distinct month)=12;
quit;

proc sort data=largecap_annual; by year SFundRet;run;

data largecap_annual;
	set largecap_annual;
	if SFundRet=0 then delete;
run;

proc sql;
	create table largecap_bob33 as
	select distinct year, count(distinct FundId) as nfunds, count(distinct SecId) as nSecids,mean(SFundRet) as Mean_ret, median(SFundRet) as Median_ret,
	min(SFundRet) as Min_ret,max(SFundRet) as Max_ret
	from largecap_annual
	group by year;
quit;

**top and bottom quartile returns;
proc sort data=largecap_annual; by year secid;run;

proc rank data=largecap_annual groups=4 out=largecap_annual1;
  by year;
  var SFundRet;
  ranks Rank_ret;
run;

data largecap_annual1;
  set largecap_annual1;
  if Rank_ret=0 or Rank_ret=3;
run;

proc sql;
	create table largecap_bob331 as
	select distinct year, Rank_ret,mean(SFundRet) as Mean_ret
	from largecap_annual1
	group by year, Rank_ret;
quit;

proc sort data=largecap_bob331; by year rank_ret;run;
 
proc transpose data=largecap_bob331 out=largecap_bob331 prefix=Rank;
    by year;
    id Rank_ret;
    var Mean_ret;
run;

**merge both;
proc sql;
	create table largecap_bob33 as
	select distinct a.*,b.*
	from largecap_bob33 as a left join largecap_bob331 as b
	on a.year=b.year;
quit;


data largecap_bob33;
	set largecap_bob33;
	drop _NAME_;
run;


*(2)Small & Mid cap funds;
data PV2.smidcap;
	set PV2.smidcap;
	year=year(date);
run;

proc sql;
	create table smidcap_annual as
	select distinct Fundid, secid, year, sum(Ret) as SFundRet, mean(SMB) as SSMB, mean(HML) as SHML, mean(WML) as SWML,
	sum(Exfundret) as SExfundret, sum(Exmktret) as SExmktret, sum(rf) as RF_annual
	from PV2.smidcap
	group by year,secid
	having count(distinct month)=12;
quit;

proc sort data=smidcap_annual; by year SFundRet;run;

data smidcap_annual;
	set smidcap_annual;
	if SFundRet=0 then delete;
run;

proc sql;
	create table smidcap_bob33 as
	select distinct year, count(distinct FundId) as nfunds, count(distinct SecId) as nSecids,mean(SFundRet) as Mean_ret, median(SFundRet) as Median_ret,
	min(SFundRet) as Min_ret,max(SFundRet) as Max_ret
	from smidcap_annual
	group by year;
quit;

**top and bottom quartile returns;
proc sort data=smidcap_annual; by year secid;run;

proc rank data=smidcap_annual groups=4 out=smidcap_annual1;
  by year;
  var SFundRet;
  ranks Rank_ret;
run;

data smidcap_annual1;
  set smidcap_annual1;
  if Rank_ret=0 or Rank_ret=3;
run;

proc sql;
	create table smidcap_bob331 as
	select distinct year, Rank_ret,mean(SFundRet) as Mean_ret
	from smidcap_annual1
	group by year, Rank_ret;
quit;

proc sort data=smidcap_bob331; by year rank_ret;run;
 
proc transpose data=smidcap_bob331 out=smidcap_bob331 prefix=Rank;
    by year;
    id Rank_ret;
    var Mean_ret;
run;

**merge both;
proc sql;
	create table smidcap_bob33 as
	select distinct a.*,b.*
	from smidcap_bob33 as a left join smidcap_bob331 as b
	on a.year=b.year;
quit;


data smidcap_bob33;
	set smidcap_bob33;
	drop _NAME_;
run;


*(3)Combo of Largecap, Small & Mid cap funds;
data PV2.largesmid;
	set PV2.largesmid;
	year=year(date);
run;

proc sql;
	create table largesmid_annual as
	select distinct Fundid, secid, year, sum(Ret) as SFundRet, mean(SMB) as SSMB, mean(HML) as SHML, mean(WML) as SWML,
	sum(Exfundret) as SExfundret, sum(Exmktret) as SExmktret, sum(rf) as RF_annual
	from PV2.largesmid
	group by year,secid
	having count(distinct month)=12;
quit;

proc sort data=largesmid_annual; by year SFundRet;run;

data largesmid_annual;
	set largesmid_annual;
	if SFundRet=0 then delete;
run;

proc sql;
	create table largesmid_bob33 as
	select distinct year, count(distinct FundId) as nfunds, count(distinct SecId) as nSecids,mean(SFundRet) as Mean_ret, median(SFundRet) as Median_ret,
	min(SFundRet) as Min_ret,max(SFundRet) as Max_ret
	from largesmid_annual
	group by year;
quit;

**top and bottom quartile returns;
proc sort data=largesmid_annual; by year secid;run;

proc rank data=largesmid_annual groups=4 out=largesmid_annual1;
  by year;
  var SFundRet;
  ranks Rank_ret;
run;

data largesmid_annual1;
  set largesmid_annual1;
  if Rank_ret=0 or Rank_ret=3;
run;

proc sql;
	create table largesmid_bob331 as
	select distinct year, Rank_ret,mean(SFundRet) as Mean_ret
	from largesmid_annual1
	group by year, Rank_ret;
quit;

proc sort data=largesmid_bob331; by year rank_ret;run;
 
proc transpose data=largesmid_bob331 out=largesmid_bob331 prefix=Rank;
    by year;
    id Rank_ret;
    var Mean_ret;
run;

**merge both;
proc sql;
	create table largesmid_bob33 as
	select distinct a.*,b.*
	from largesmid_bob33 as a left join largesmid_bob331 as b
	on a.year=b.year;
quit;


data largesmid_bob33;
	set largesmid_bob33;
	drop _NAME_;
run;

data test;
	set largecap_bob33 smidcap_bob33 largesmid_bob33;
run;




*(B) Point 4: Return Analysis;

*(1)Largecap funds;
proc sql;
	create table largecap_annual as
	select distinct secid, year, sum(Ret) as SFundRet, mean(SMB) as SSMB, mean(HML) as SHML, mean(WML) as SWML,
	sum(Exfundret) as SExfundret, sum(Exmktret) as SExmktret, sum(rf) as RF_annual
	from PV2.largecap
	group by year,secid
	having count(distinct month)=12;
quit;

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
  drop id;
run;

*(2)Small & Mid cap funds;
proc sql;
	create table smidcap_annual as
	select distinct secid, year, sum(FundRet) as SFundRet, mean(SMB) as SSMB, mean(HML) as SHML, mean(WML) as SWML,
	sum(Exfundret) as SExfundret, sum(Exmktret) as SExmktret, sum(rf) as RF_annual
	from PV2.smidcap
	group by year,secid
	having count(distinct month)=12;
quit;

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
  drop id;
run;

*(3)Combo of Largecap, Small & Mid cap funds;
proc sql;
	create table largesmid_annual as
	select distinct secid, year, sum(Ret) as SFundRet, mean(SMB) as SSMB, mean(HML) as SHML, mean(WML) as SWML,
	sum(Exfundret) as SExfundret, sum(Exmktret) as SExmktret, sum(rf) as RF_annual
	from PV2.largesmid
	group by year,secid
	having count(distinct month)=12;
quit;

proc sql;
	create table largesmid_bob33 as
	select distinct count(distinct SecId) as nfunds,mean(SExfundret) as Mean_ret, std(SExfundret) as Std_ret, median(SExfundret) as Median_ret,
	min(SExfundret) as Min_ret,max(SExfundret) as Max_ret, mean(SExfundret)/std(SExfundret) as Sharpe_ratio,1 as id
	from largesmid_annual;
quit;

**top and bottom quartile returns;
proc sort data=largesmid_annual; by year secid;run;

proc rank data=largesmid_annual groups=4 out=largesmid_annual1;
  by year;
  var SExfundret;
  ranks Rank_ret;
run;

data largesmid_annual1;
  set largesmid_annual1;
  if Rank_ret=0 or Rank_ret=3;
run;

proc sql;
	create table largesmid_bob331 as
	select distinct Rank_ret,mean(SExfundret) as Mean_ret
	from largesmid_annual1
	group by Rank_ret;
quit;
 
proc transpose data=largesmid_bob331 out=largesmid_bob331 prefix=Rank;
/*    by year;*/
    id Rank_ret;
    var Mean_ret;
run;


data largesmid_bob331;
	set largesmid_bob331;
	Top_minus_bottom_quartile_Exret=Rank3-Rank0;
	id=1;
	rename Rank3=Top_quartile;
	rename Rank0=bottom_quartile;
	drop _NAME_;
run;

proc sql;
	create table largesmid_bob33 as
	select distinct a.*,b.*
	from largesmid_bob33 as a, largesmid_bob331 as b
	where a.id=b.id;

	drop table largesmid_bob331;
run;

**Regressions;
proc reg data=largesmid_annual noprint tableout outest=largesmid_Alpha;
  model SExfundret = SExmktret; *CAPM;
  model SExfundret = SExmktret SSMB SHML SWML; *4-factor;
quit;

data largesmid_Alpha;
  set largesmid_Alpha;
  where _Type_ in ('PARMS','T');
  keep _model_ _Type_ Intercept;
  if _Type_='PARMS' then Intercept=Intercept;
  rename Intercept=Alpha;
  rename _Type_ =Stat;
run;

data largesmid_Alpha;
  set largesmid_Alpha;
  nrow=_N_;
run;

proc transpose data=largesmid_Alpha out=largesmid_Alpha;
    id nrow;
    var Alpha;
run;
data largesmid_Alpha;
  set largesmid_Alpha;
  id=1;
  rename _1=CAPM_Alpha;
  rename _2=Tstat_CAPM_Alpha;
  rename _3=Four_factor_Alpha;
  rename _4=Tstat_Four_factor_Alpha;
  drop _NAME_ _LABEL_;
run;

proc sql;
	create table largesmid_bob33 as
	select distinct a.*,b.*
	from largesmid_bob33 as a, largesmid_Alpha as b
	where a.id=b.id;
run;


proc sql;
	create table largesmid_bob331 as
	select distinct secid, count(distinct year) as nfunds,mean(SExfundret) as Mean_ret, std(SExfundret) as Std_ret, mean(SExfundret)/std(SExfundret) as Sharpe_ratio,1 as id
	from largesmid_annual
	group by secid;

	create table largesmid_bob331 as
	select distinct a.*,b.Sharpe_ratio as Sharpe_ratio_benchmark
	from largesmid_bob331 as a, largesmid_bob33 as b
	where a.id=b.id;
quit;

data largesmid_bob331;
  set largesmid_bob331;
  if Sharpe_ratio>=Sharpe_ratio_benchmark;
run;

proc sql;
	create table largesmid_bob331 as
	select distinct count(distinct SecId) as ncount,1 as id
	from largesmid_bob331;

	create table largesmid_bob33 as
	select distinct a.*,(b.ncount/a.nfunds)*100 as Precent_funds_beating_sharpe
	from largesmid_bob33 as a, largesmid_bob331 as b
	where a.id=b.id;

	drop table largesmid_Alpha, largesmid_annual, largesmid_annual1,largesmid_bob331,test;
quit;

data largesmid_bob33;
  set largesmid_bob33;
  drop id;
run;


proc append base=test data=largecap_bob33;
run;
proc append base=test data=smidcap_bob33;
run;
proc append base=test data=largesmid_bob33;
run;
