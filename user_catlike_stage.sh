#!/bin/bash
source ~/.bashrc
hivepath=`date -d "-1 hour" +%Y/%m/%d/%H`
start_date=`date -d "-338 hour" +%Y%m%d%H`
end_date=`date -d "-1 hour" +%Y%m%d%H`


sql="
use log;
set hive.cli.print.header=false;
CREATE TEMPORARY TABLE tmp_common_event (userid string,catid string, sourceType string);
CREATE TEMPORARY TABLE common_event (userid string,catid string, sourceType string);
CREATE TEMPORARY TABLE push_event (uid string,cid string, recomCount int, allRecomCount int, clickCount int, allClickCount int);
CREATE TEMPORARY TABLE click_dislike (userid string,catid int,tfidf double);
CREATE TEMPORARY TABLE news_social (userid string,catid string,callcount int);
CREATE TEMPORARY TABLE default_call (userid string,catid string,catcount int,callcount int,islike string);
CREATE TEMPORARY TABLE default_call_number (userid string,catid string,weight int);
CREATE TEMPORARY TABLE default_call_score (userid string,catid string,weight double);
CREATE TEMPORARY TABLE categoryIds (userid string, categoryIds string);
CREATE TEMPORARY TABLE clickIds (userid string, cid string);


INSERT INTO categoryIds
    select userid, from_json(b.push_item,'map<string,string>')['categoryIds'] as categoryIds from push_news a
        Lateral view explode(json_array(a.data)) b as push_item
        where pt>='${start_date}'
        and pt<='$end_date'
        and topic='topic_recommendation_event'
        and from_json(b.push_item,'map<string,string>')['messageType']='NEWS';

insert INTO clickIds
    select userid, d.cid as cid from topic_common_event
        lateral view explode(split(from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['categoryIds'], ',')) d as cid
        where pt>='${start_date}'
        and pt<='$end_date'
        AND from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['sourceType'] !=''
        AND from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['newsType'] == 'NEWS'
        AND from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['categoryIds'] !='';

INSERT INTO push_event
    select recommendation.uid as uid, recommendation.cid as cid, recommendation.cCount as recomCount, recommendation.allCount as allRecomCount,
           click.cCount as clickCount, click.allCount as allClickCount  from
    (select xx.userid as uid, xx.cid as cid, xx.cCount as cCount, xy.allCount as allCount from
    (select userid, e.cid as cid, count(1) as cCount  from categoryIds
    lateral view explode(split(categoryIds, ',')) e as cid
    group by userid, cid) xx
    JOIN
    (select userid, count(1) as allCount from categoryIds
        lateral view explode(split(categoryIds, ',')) e as cid
         group by userid
    ) xy ON (xx.userid = xy.userid)) recommendation
    JOIN
    (select dd.userid as uid, dd.cid as cid, dd.cCount as cCount, de.allCount as allCount from
    (select userid, cid as cid, count(1) as cCount from clickIds 
        group by userid, cid) dd
    JOIN
    (select userid, count(1) as allCount from clickIds
        group by userid) de
     ON (dd.userid = de.userid)) click
     ON (recommendation.uid = click.uid and recommendation.cid = click.cid);

INSERT INTO news_social
SELECT a.userid as userid,
        cast(b.catId AS int) AS catId,
        sum(CASE logtype WHEN 'INTERACT' THEN 2 WHEN 'SHARE' THEN 4 WHEN 'COLLECT' THEN 4 END) AS catCount
FROM log.topic_news_social a LATERAL VIEW explode(json_array(get_json_object(data,'$.categoryIds'))) b AS catId
WHERE pt>='${start_date}' AND pt<='${end_date}'
	AND logtype IN ('INTERACT','SHARE','COLLECT')
        AND get_json_object(a.data,'$.categoryIds[0]') IS NOT NULL
GROUP BY userid,catId;

insert into common_event 
select userid, catId, sourceType from(
select click_t.userid as userid, click_t.catId as catId, click_t.sourceType as sourceType from (
select userid, newsid, catId, sourceType from (
select userid,
        from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['newsId'] as newsId,
        from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['categoryIds'] as catids,
        from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['sourceType'] as sourceType
from log.topic_common_event
where 
	pt>='${start_date}'
	AND pt<='$end_date'
	AND from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['newsType']=='NEWS'
	AND from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['categoryIds'] !=''
	AND from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['sourceType'] !=''
	) as c LATERAL VIEW explode(split(catids,',')) b AS catId
) click_t 
inner join
(
select userid, newsId, catId, durations from (
select userid, 
	from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['newsId'] as newsId,
	from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['categoryIds'] as catids,
	from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['duration'] as durations
	from log.topic_common_event 
where 
	pt>='${start_date}'
	AND pt<='$end_date'
	AND from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['duration'] !='' 
	AND from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['categoryIds'] !='' 
	AND from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['newsType'] == 'NEWS'
	AND from_json(from_json(data,'map<string,string>')['param'],'map<string,string>')['duration'] >= 2000
	) as e LATERAL VIEW explode(split(catids,',')) f AS catId
) duration_t  
on click_t.userid = duration_t.userid
and click_t.newsid = duration_t.newsId
and click_t.catId = duration_t.catId
) temp
where temp.catId rlike '^[0-9]+$';

insert into default_call
     SELECT dc.userid,
             dc.catId,
             dc.catCount,
             dd.userCount,
             'dislike' as islike
      FROM
        (SELECT userid,
                catId,
                sum(catCount) AS catCount
         FROM
           (
                SELECT 'default' AS userid,
                             catId,
                             sum(callcount) AS catCount
            FROM news_social
            GROUP BY catId
            UNION ALL SELECT 'default' as userid,
                        cast(catId AS INT) as catId,
                        count(1) as catCount
                FROM common_event
                group by catId
         ) AS all_user_act_table
         GROUP BY userid,
                  catId) dc
      JOIN
        (
                SELECT 'default' as userid,count(1) as userCount FROM common_event
        ) dd ON (dc.userid = dd.userid);


insert into default_call_score
      select a.userid as userid, a.catid as catid, (a.catcount/b.allcount) as weight from
     (select userid, catid, catcount
     from default_call) a
     join (
      select userid, sum(catcount) as allcount from default_call
      group by userid) b
      on (b.userid = 'default');        


insert into default_call_number
      select b.userid as userid, a.catid as catid, (100 * a.weight) as weight from
     (select d.userid as userid,  count(1) as tmp from 
      (select userid from common_event
      union all
      select userid from news_social
      )d group by d.userid) b
      join
     (select userid, catid, weight
     from default_call_score) a on (a.userid = 'default'); 
       

insert into click_dislike
  SELECT dx.userid,
          dx.catid,
          case 
          	when islike='clicklike' then
          		(dx.weight*dy.idf)
         	when islike='dislike' then dx.weight
	  end
         AS tfidf
   FROM
     (
	select userid,catid,(catcount/callcount) as weight,islike from default_call
      UNION ALL 

      SELECT f.userid as userid,f.catId as catId, case when g.clickCount is not null 
                                             then (f.weight*g.clickCount*g.allRecomCount/(g.recomCount*g.allClickCount)) 
                                             when g.clickCount is null
                                             then f.weight*0.5  
                                             end AS weight, f.islike as islike from
      (SELECT c.userid as userid,c.catId as catId,(c.catCount/d.userCount) AS weight,'clicklike' as islike
      FROM
         (select userid, catid,
                sum(catCount) AS catCount
         FROM
           (
	    SELECT userid,
                             catId,
                             sum(callcount) AS catCount
	    FROM news_social
            GROUP BY userid,
                     catId
	    UNION ALL
            select userid, catid, weight as catCount from
            default_call_number

            UNION ALL 
	    SELECT z.userid, z.catId, sum(z.catCount) as catCount
            FROM
            (SELECT userid, catId,
                   case
                     when sourceType='HOT'
                     then
                         count(1)*0.6
                     else
                         count(1)
                     end
                     AS catCount

	    FROM common_event
            GROUP By userid,catid, sourceType) z
	    GROUP By userid,catid
	   ) user_act_table
         GROUP BY userid,
                  catId) c
          
      JOIN
        (SELECT userid,
                sum(userCount) AS userCount
         FROM
        (
            SELECT userid,
                             sum(callcount) AS userCount
	    FROM news_social
            GROUP BY userid
	    UNION ALL
            select userid, sum(weight) as userCount from
            default_call_number
            group by userid
            UNION ALL 
	    SELECT userid as userid,count(1) AS userCount
	    FROM common_event
	    GROUP BY userid
	   ) user_act_table
         GROUP BY userid
	) d ON (c.userid = d.userid)) f
	LEFT JOIN
	(select uid, cid, recomCount, allRecomCount, clickCount, allClickCount from push_event) g
	ON (f.userid = g.uid and f.catId = g.cid)
 )AS dx
   left outer JOIN
     (
	select catid,((callcount-catcount)/callcount) as idf from default_call
) AS dy 
	ON dx.catid=dy.catid;

CREATE TEMPORARY TABLE click (json String);
insert into click 
SELECT merge_cat_like(ttx.userid,ttx.json,CAST(ttx.seq as int),0) from (
SELECT tw.userid as userid,cat_like(tw.userid,tw.catId,tw.weight,0) as json,0 as seq from (
	select tx.userid,tx.catid,case when choose.weight is null then tx.weight else tx.weight+choose.weight end as weight from(
		select cd.userid,cd.catid,cd.tfidf/tmp.total as weight from click_dislike as cd join (
			select userid,sum(tfidf) as total from click_dislike group by userid
		) as tmp on tmp.userid=cd.userid
	) as tx
	left outer join 
	(
	select split(cl.tmpdata,'-')[0] as userid,ct.catid,ct.weight from (
			select get_latest_one(userid,cast(createtime as bigint),data) as tmpdata from log.choose_like where  pt BETWEEN '$start_date' AND '$end_date' group by userid
	) as cl 
		LATERAL VIEW array_udtf(get_json_object(split(cl.tmpdata,'-')[1],'$.fondIds')) ct 
	) as choose
		on tx.userid=choose.userid and tx.catid=choose.catid
	left outer join
	(
	select userid,catid,sum(discount) as discount from profiles.dislike group by userid,catid
	)as dis
		on dis.userid=tx.userid and dis.catid=tx.catid
	WHERE dis.discount is NULL
) as tw  GROUP BY tw.userid
UNION ALL
SELECT userid,json,seq FROM profiles.user_trait
) as ttx 
GROUP BY ttx.userid;

select cat_like(cd.userid,CAST(cd.catid as int),CAST(cd.weight as double),15) from(
select click.userid as userid,click.catid as catid,click.weight as weight from (
                select json_udtf(json) from click
        ) as click left outer join (select userid,catid,sum(discount) as discount from  profiles.dislike group by userid,catid) as dis on dis.userid=click.userid and dis.catid=click.catid where dis.discount is NULL
) cd group by cd.userid
"
echo $sql
#hive -e "$sql" >/home/hadoop/caishi/local/task/hivesql/bak/user_catLike.json
hive -e "$sql" >/home/hadoop/data/hivedata/user_catLike.json
sed -i "/WARN/d" /home/hadoop/data/hivedata/user_catLike.json
/root/software/mongodb/bin/mongoimport --host "user_profile/10.4.1.25:27017,10.4.1.6:27017,10.4.1.23:27017"  --username userprofile  --password userprofile9icaishi  --authenticationDatabase  user_profiles  --upsert --collection usercatLike --db user_profiles --file /home/hadoop/data/hivedata/user_catLike.json
hadoop fs -rm -r /hivedata/profiles/user_catLike.json
hadoop fs -copyFromLocal /home/hadoop/data/hivedata/user_catLike.json /hivedata/profiles/
