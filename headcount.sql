alter view [deeque].[headcount] as

with drp_codes as
(
     select drp_code
       from
           (
            values
                  ('ЧисАд')
                 ,('ЧисИ')
                 ,('ЧисПр')
                 ,('ЧисКр')
           )t(drp_code)
),

src_kpi as
(
     select 'kpi' src
          , dt
          , guid
          , level_1 L1
          , sum(val) val
       from
           (
                select k.dt
                     , k.guid
                     , k.val
                     , case when j.h_type='Численность'
                            then k.level_1
                            else 'ЧисИ'
                             end level_1
                  from
                      (
                       select data_date_dt dt
                            , load_date_dt
                            , guid
                            , level_1
                            , cast(value as float) val
                            , row_number()over(partition by guid, level_1, data_date_dt order by load_date_dt desc) rn
                         from kpi.kpi
                         join drp_codes on kpi.level_1=drp_codes.drp_code
                        where source_table='headcount'
                          and data_date_dt>='2022-01-01'
                          and cast(load_date_dt as date)=cast(getdate()-1 as date)
                      )k
            cross join
                      (
                       values
                             ('Численность')
                            ,('Итоговая')
                      )j(h_type)
                 where k.rn=1 
           )kpi
  union all
     select 'hr'
          , hr.date
          , hr.guid
          , hr.drp_code L1
          , sum(cast(hr.rub as float))
       from ads.hr.msfohr hr
       join drp_codes c on hr.drp_code=c.drp_code
      where hr.sc_dim='Fact'
        and hr.date>='2022-01-01'
   group by hr.date
          , hr.guid
          , hr.drp_code           
),

all_values as
(
     select src
          , dt
          , guid
          , L1
          , val
          , sum(val*case when src='kpi' then -1 else 1 end)over(partition by guid, L1, dt) diff
          , count(src)over(partition by dt, guid, L1) cnt
       from src_kpi
),

all_deviations as
(
     select distinct guid
          , L1
          , dt
          , 1 err
       from all_values
      where
           (
                cnt=1 
            and src='kpi'
           )
         or
           (
                cnt=2
            and abs(diff)>0.1
           )
),

veracity as
(
     select c.drp_code fld
          , 1-coalesce(s.err,0) val
          , g.guid
       from
           (
            select distinct guid
              from src_kpi
           )g
 cross join drp_codes c
  left join
           (
            select distinct guid
                 , L1
                 , err
              from all_deviations
           )s on s.guid=g.guid
             and s.L1=c.drp_code
),

zup_msfo_detailed as
(
     select pv.dt
          , pv.guid
          , pv.ЭтоГруппа
          , pv.msfo
          , case when pv.guid='94e'
                 then sum
                         (
                          case when msfo is not null
                                and zup is not null
                                and ЭтоГруппа=0
                               then pv.zup
                               else 0
                                end
                         )over(partition by dt)
                 else pv.zup
                  end zup
        from
            (
                  select 'zup' src
                       , oa.dt
                       , oa.uid guid
                       , o.ЭтоГруппа 
                       , sum(oa.v) val
                    from
                        (
                            select hs.Дата dt
                                 , hs.Организация_УИД guid
                                 , organizations.Родитель_УИД puid
                                 , hs.КоличествоСтавок val
                              from db_ods.hr.hr_staff hs
                         left join db_ods.fund.organizations on organizations.Ссылка_УИД=hs.Организация_УИД
                             where hs.ДатаУтверждения<=hs.Дата                       
                        )z
             outer apply
                        (
                         values
                               (
                                 z.guid
                               , z.dt
                               , z.val
                               )
                              ,(
                                 z.puid
                               , z.dt
                               , z.val
                               )
                        )oa
                           (
                             uid
                           , dt
                           , v
                           )         
               left join db_ods.fund.organizations o on oa.uid=o.Ссылка_УИД
                group by oa.uid
                       , oa.dt
                       , o.ЭтоГруппа  
               union all
                  select 'msfo'
                       , oa.dt
                       , oa.uid guid
                       , o.ЭтоГруппа
                       , sum(oa.v)
                    from
                        ( 
                            select hr.organization_guid guid
                                 , hr.date dt
                                 , h.Родитель_УИД puid
                                 , hr.val
                              from ads.hr.msfohr hr
                         left join db_ods.fund.organizations h on hr.organization_guid=h.Ссылка_УИД
                             where hr.sc='Fact'
                               and hr.showing_guid='asdadasd-1234123-asdadsdadreegf'
                        )msfo
             outer apply 
                        (
                         values
                               (
                                 msfo.guid
                               , msfo.dt
                               , msfo.val
                               )
                              ,(
                                 msfo.puid
                               , msfo.dt
                               , msfo.val
                               )
                        )oa
                           (
                             uid
                           , dt
                           , v
                           ) 
               left join db_ods.fund.organizations o on oa.uid=o.Ссылка_УИД   
                group by oa.uid
                       , oa.dt
                       , o.ЭтоГруппа   
            )zup_msfo
       pivot
            (
                sum(val)
             for src in 
                       (
                         zup
                       , msfo                  
                       )
            )pv
       where pv.guid<>'arc12312-12311325-31254' ---archive
         and pv.dt=eomonth
                          (
                            getdate()
                          , case when getdate()>=
                                                 (
                                                  select date
                                                    from dbo.pc
                                                   where eomonth(getdate())=eomonth(dt)
                                                     and wd=11
                                                 )
                                 then -1
                                 else -2
                                  end
                          )
),

precalc as
(
     select guid
          , venue
          , case when guid='94e'
                 then 2
                 else ЭтоГруппа
                  end ЭтоГруппа
          , zup zup_as_is
          , msfo msfo_as_is
          , case when guid='94e'
                 then zup-
                          (
                           select sum(coalesce(zup,0))
                             from zup_msfo_detailed
                            where guid in 
                                         ( 
                                           '123asfga'
                                         , 'etigady'
                                         )
                          )
                 else zup
                  end zup
          , msfo
       from zup_msfo_detailed
),

zup_msfo_final as
(
     select *
          , sum(case when zup is not null and msfo is not null then zup else 0 end)over(partition by ЭтоГруппа) zup_hier
          , sum(case when zup is not null and msfo is not null then msfo else 0 end)over(partition by ЭтоГруппа) msfo_hier
       from precalc 
),

metrics as
(
     select 'hier_accordance' mtrc
          , 'zup' fld
          , case when count(distinct zup_hier)=1
                 then 1
                 else 0
                  end val
          , null uid
       from zup_msfo_final
      where ЭтоГруппа is not null
  union all
     select 'hier_accordance' mtrc
          , 'msfo' fld
          , case when count(distinct msfo_hier)=1
                 then 1
                 else 0
                  end val
          , null uid
       from zup_msfo_final
      where ЭтоГруппа is not null
  union all
     select 'concomitance'
          , 'zup-msfo'
          , case when abs(zup-msfo)>2
                 then 0
                 else 1
                  end
          , guid
       from precalc
      where zup is not null
        and msfo is not null
  union all
     select 'unalterability'
          , fld
          , val
          , guid
       from veracity
  union all
     select 'timeliness'
          , ''
          , case when cast(getdate() as date)=
                                              (
                                               select date
                                                 from dbo.pc
                                                where eomonth(getdate())=eomonth(dt)
                                                  and wd=case when uid<>'94e' 
                                                              then 9
                                                              else 11
                                                               end
                                              )
                 then
                      case when max(hr.date)>=eomonth(getdate(),-1)
                           then 1
                           else 0
                            end
                 else
                     (
                      select val
                        from
                            (
                             select uid
                                  , val
                                  , row_number()over(partition by guid order by load_dt desc) rn
                               from deeque.history
                              where obj_id=9123
                                and mtrc='timeliness'
                                and guid=hr.guid
                            )x
                       where x.guid=hr.guid
                         and x.rn=1
                     )
                  end
          , guid
       from ads.hr.msfohr hr
       join drp_codes c on hr.drp_code=c.drp_code
      where hr.scenario='Fact'
        and hr.date>='2023-01-01'
   group by hr.guid
)
     select cast(getdate()-1 as date) dt
          , mtrc_tp
          , fld
          , val
          , 9123 object_id
          , guid
       from metrics   
