#!/usr/bin/env python
# coding: utf-8

# In[1]:

import subprocess
import sys


import psycopg2

import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler

import statsmodels.api as sm

from sklearn.linear_model import LogisticRegression

import joblib

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)


# In[2]:


conn = psycopg2.connect(
                host="dwh-production.db.eigensonne.de",
                port="5432",
                database="eigensonne_dwh",
                user="amareid",
                password="ey4cMnS6Chni29G2H2iL",
                sslmode="require")


# In[3]:




query = """with base1 as (
select l.id
    , va.prospect_id
    , case when va.email_template_id in (9187) then 'Personal introduction Sales Rep'
    when va.email_template_id in (9695) then 'Personal introduction Sales Rep Solarmiete'
    when va.email_template_id in (67) then 'LOST - Wrong Phone'
    when va.email_template_id = 63 then 'LOST - Not Reachable NO DISCOUNT'
    when va.email_template_id in (65) then 'LOST - Alles Gute'
    when va.email_template_id in (9263) then 'LOST - Images Rejected'
    when va.email_template_id in (11121) then 'LOST - Outside Service Area'
    when va.email_template_id in (9707) then 'Solarmiete Sales Rep cant reach you 2'
    when va.email_template_id in (9191) then 'Sales Rep cant reach you 2'
    when va.email_template_id in (9561) then 'Solarmiete We need to talk 3'
    when va.email_template_id in (8876) then 'We need to talk 3'
    when va.email_template_id in (85) then 'ORDER - Order confirmation'
    when va.email_template_id in (99) then 'Confirm FSC & We Need Pics'
    when va.email_template_id in (295) then 'Photo reminder before FSC'
    when va.email_template_id in (8890) then 'Photo Upload Request'
    when va.email_template_id in (9667) then 'Solarmiete Photo Upload Request'
    when va.email_template_id in (9109) then 'Photo Upload Reminder 1'
    when va.email_template_id in (9671) then 'Solarmiete Photo Upload Reminder 2'
    when va.email_template_id in (9113) then 'Photo Upload Reminder 2'
    when va.email_template_id in (9669) then 'Solarmiete Photo Upload Reminder 1'
    when va.email_template_id in (9711) then 'Solarmiete Sales cant reach you for OP2'
    when va.email_template_id in (9197) then 'Sales cant reach you for OP2'
    when va.email_template_id in (8888) then 'Confirm FQC'
    when va.email_template_id in (10545) then 'Waiting for pictures before Whatsapp'
    when va.email_template_id in (9199) then 'Sales cant reach you for OP3'
    when va.email_template_id in (9709) then 'Solarmiete Sales cant reach you for OP1'
    when va.email_template_id in (9195) then 'Sales cant reach you for OP1'
    when va.email_template_id in (9703) then 'Solarmiete Sales Rep cant reach you 3'
    when va.email_template_id in (9193) then 'Sales Rep cant reach you 3'
    when va.email_template_id in (9117) then 'Better Photos Requested Reminder'
    when va.email_template_id in (9675) then 'Solarmiete Better Photos Requested Reminder'
    when va.email_template_id in (9115) then 'Photo Upload Reminder 3'
    when va.email_template_id in (9673) then 'Solarmiete Photo Upload Reminder 3'
    when va.email_template_id in (87) then 'ORDER - MaStR Info'
    when va.email_template_id in (8874) then 'We need to talk 2'
    when va.email_template_id in (9559) then 'Solarmiete We need to talk 2'
    when va.email_template_id in (9123) then 'Confirm Offer Presentation Appointment'
    when va.email_template_id in (107) then 'Offer Created - Confirm C2C'
    when va.email_template_id in (9189) then 'Sales Rep cant reach you 1'
    when va.email_template_id in (9705) then 'Solarmiete Sales Rep cant reach you 1'
    when va.email_template_id in (8872,20) then 'We need to talk 1'
    when va.email_template_id in (9557) then 'Solarmiete We need to talk 1'
    when va.email_template_id in (8868) then 'Welcome Email (Website)'
    when va.email_template_id in (235,8870) then 'Welcome Email (Partners)'
    when va.email_template_id in (9555) then 'Solarmiete Welcome Email (Partners)'
    when va.email_template_id in (9251) then 'LOST - Pictures not sufficient'
    when va.email_template_id in (9213) then 'Solarmiete Welcome Email (Website)'
    when va.email_template_id in (11217) then 'LOST - clean-up'
    when va.email_template_id in (9183) then 'Opp-Conversion E-Mail'
    else va.details
    end as email_sent
, c.url
 , case when va.type = 6 then 1 else 0
    end as has_sent
 , case when va.type = 11 then 1 else 0
    end as has_opened
, case when va.type = 1 and c.url not like '%fotoupload%' and c.url not like '%Foto_Anleitung%pdf'then 1 else 0
    end as has_clicked
from eigensonne_dwh.salesforce_production.leads as l
left join eigensonne_dwh.pardot_production.visitor_activities as va on va.prospect_id::text=l.prospect_id_c
left join eigensonne_dwh.pardot_production.email_clicks as c on c.prospect_id::text = l.prospect_id_c
                                                        and c.email_template_id=va.email_template_id
                                                        and va.type = 1
                                                        and va.list_email_id=c.list_email_id
where l.prospect_id_c is not null and va.prospect_id is not null
and va.type in (1,6,11)
group by 1,2,3,4
, va.type
order by l.id),

base2 as (select b1.id,
b1.prospect_id,
b1.email_sent,
b3.url,
b1.has_sent,
b2.has_opened,
b3.has_clicked
from base1 as b1
left join base1 as b2 on b1.prospect_id=b2.prospect_id
                    and b1.id=b2.id
                    and b1.email_sent=b2.email_sent
                    and b2.has_opened =1
left join base1 as b3 on b1.prospect_id=b3.prospect_id
                    and b1.id=b3.id
                    and b1.email_sent=b3.email_sent
                    and b3.has_clicked=1
where b1.has_sent = 1
group by 1,2,3,4,5,6,7
order by id),

base3 as (
    with leads as (
    select id,
           created_date,
           received_at,
           lead_source,
           utm_source_c,
           utm_medium_c,
           utm_content_c,
           utm_campaign_c,
           google_adid_c,
           google_adgroupid_c,
           google_campaignid_c,
           split_part(split_part(google_clientid_c, 'GA', 2), '.', 3) || '.' ||
           split_part(split_part(google_clientid_c, 'GA', 2), '.', 4) as google_client_id,
           lead_cost_c
    from eigensonne_dwh.salesforce_production.leads
    where (utm_source_c is null or utm_source_c not in
                                   ('Maritza Shannon',
                                    'Kolten Walters',
                                    'Averi Key',
                                    'PEuMFCmchfpA',
                                    'NrAyTvleEfuH',
                                    'JjqsgbcDtnAa',
                                    'VkSxtCTDROKe',
                                    'hYrKIxDsvVMP',
                                    'mZSpOWdqGwkB',
                                    'nXhedGBAZyFS',
                                    'Jasper Chambers',
                                    'jEzOtUmguCXc',
                                    'vUYCzBiAeqfo',
                                    'MFUQbAfNulKW',
                                    'LPCVbWhcvyXi',
                                    'aBrTgqCJvIKH',
                                    'qjaAZwmSctIH',
                                    'GMXdsFDIRVHx',
                                    'QbsHVwLZeaTR',
                                    'laOugswGrVpT',
                                    'kxzQGSBRhpDK',
                                    'NBiHtgwqxKuD',
                                    'RtdJXUxDsjny',
                                    'nKmDrRUZMGYw',
                                    'SeBTcJbzaWdH',
                                    'ZFxNJiMqeogz',
                                    'fPQjWrzkVGAB',
                                    'pVrONLzkElgG',
                                    'taDSnEBcvigp',
                                    'sfPRgSNjaieC',
                                    'batQDXIfiNne',
                                    'hbciKpBIMlPm',
                                    'pwsbtQYkjJxu',
                                    'seEukjBdbAZl',
                                    'Jayda Snyder',
                                    'FxucSGkRXyOA',
                                    'uoFSlvzdpIjw')
        and verify_pictures_date_time_c is null
        and loss_date_time_c is null
        and created_date::date >= '2020-10-01'
        and status in ('New', 'Waiting For Pictures'))
),

  ga_data as (
    select first_value(blendo_exported_at) over
        (partition by ga_dimension11
            , ga_datehourminute order by blendo_exported_at desc) as latest_export
         , blendo_exported_at
         , ga_dimension11                                         as client_id
         , to_timestamp(ga_datehourminute, 'YYYYMMDDHH24MI')      as start_at_cet
         , case
               when ga_devicecategory = '(not set)'
                   then null
               else ga_devicecategory end                         as device_category
         , split_part(case
                          when
                              ga_sourcemedium = '(not set)'
                              then null
                          else
                              ga_sourcemedium end, '/ ', 2)       as traffic_medium
         , split_part(case
                          when
                              ga_sourcemedium = '(not set)'
                              then null
                          else
                              ga_sourcemedium end, ' /', 1)       as traffic_source
         , case
               when ga_campaign = '(not set)'
                   then null
               else ga_campaign end                               as traffic_campaign
         , case
               when ga_adcontent = '(not set)'
                   then null
               else ga_adcontent end                              as traffic_adcontent
         , case
               when ga_keyword = '(not set)'
                   then null
               else ga_keyword end                                as traffic_keyword
         , case
               when ga_adwordsadgroupid = '(not set)'
                   then null
               else ga_adwordsadgroupid end                       as ad_group_id
         , ga_sessions                                            as count_session
         , ga_sessionduration                                     as session_duration
         , ga_pageviews                                           as count_page_views
         , ga_pageviewspersession                                 as count_session_page_views
         , case
               when ga_sessions
                   > 0 then 1
               else 0 end                                         as is_session 
         , case
               when ga_exits
                   > 0 then 1
               else 0 end                                         as is_exited 
    from eigensonne_dwh.google_analytics.user_sessions_report
      where to_timestamp(ga_datehourminute, 'YYYYMMDDHH24MI')::date >= '2020-10-01'
),

     fb_ad_group_enriched as (
         select ga.latest_export,
                ga.blendo_exported_at,
                ga.client_id,
                ga.start_at_cet,
                ga.device_category,
                ga.traffic_medium,
                ga.traffic_source,
                ga.traffic_campaign,
                ga.traffic_adcontent,
                ga.traffic_keyword,
                case
                    when ga.ad_group_id is null and
                         fb.ga_dimension9 is not null
                        then fb.ga_dimension9
                    else ga.ad_group_id
                    end as ad_group_id,
                ga.count_session,
                ga.session_duration,
                ga.count_page_views,
                ga.count_session_page_views,
                ga.is_session,
                ga.is_exited
         from ga_data ga
                  left join eigensonne_dwh.google_analytics.user_sessions_fb_report as fb
                            on ga.client_id = fb.ga_dimension11 and
                               ga.start_at_cet = to_timestamp(fb.ga_datehourminute, 'YYYYMMDDHH24MI') and
                               ga.traffic_source = split_part(fb.ga_sourcemedium, ' /', 1)
     ),
     
     latest_export as (
         select client_id || '_' ||
                sum(is_session)
                over (partition by client_id
                    order by traffic_adcontent
                        , start_at_cet
                    rows unbounded preceding) as client_session_id
              , *
         from fb_ad_group_enriched
         where latest_export = blendo_exported_at
     ),

     session_start as (
         select *
              , first_value(start_at_cet)
                over (partition by client_session_id order by start_at_cet) as session_start_at_cet
              , first_value(traffic_medium)
                over (partition by client_session_id order by start_at_cet) as session_first_traffic_medium
              , first_value(traffic_source)
                over (partition by client_session_id order by start_at_cet) as session_first_traffic_source
              , first_value(traffic_campaign)
                over (partition by client_session_id order by start_at_cet) as session_first_traffic_campaign
              , first_value(traffic_adcontent)
                over (partition by client_session_id order by start_at_cet) as session_first_traffic_adcontent
              , first_value(traffic_keyword)
                over (partition by client_session_id order by start_at_cet) as session_first_traffic_keyword
              , first_value(ad_group_id)
                over (partition by client_session_id order by start_at_cet) as session_first_ad_group_id
         from latest_export),

     web_sessions as (
         select client_session_id
              , client_id
              , session_start_at_cet
              , device_category
              , session_first_traffic_medium
              , session_first_traffic_source
              , session_first_traffic_campaign
              , session_first_traffic_adcontent
              , session_first_traffic_keyword
              , session_first_ad_group_id
              , case
                    when leads.google_client_id is not null and
                         session_start_at_cet <= (leads.created_date at time zone 'Europe/Berlin')
                        then 1
                    else 0 end               as is_before_conversion
              , sum(count_session)           as multiple_sessions
              , sum(session_duration::float) as session_duration
              , sum(count_page_views)        as session_page_views
         from session_start
                  left join leads on client_id = leads.google_client_id
         group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
     ),

     web_sessions_last_session as (
         select client_id
              , last_value(session_start_at_cet)
                over (partition by client_id order by is_before_conversion, session_start_at_cet rows between unbounded preceding and unbounded following) last_session_start_at_cet
              , last_value(device_category)
                over (partition by client_id order by is_before_conversion,session_start_at_cet rows between unbounded preceding and unbounded following) last_session_device_category
              , last_value(session_first_traffic_medium)
                over (partition by client_id order by is_before_conversion,session_start_at_cet rows between unbounded preceding and unbounded following) last_session_medium
              , last_value(session_first_traffic_source)
                over (partition by client_id order by is_before_conversion,session_start_at_cet rows between unbounded preceding and unbounded following) last_session_source
              , last_value(session_first_traffic_campaign)
                over (partition by client_id order by is_before_conversion,session_start_at_cet rows between unbounded preceding and unbounded following) last_session_campaign
              , last_value(session_first_traffic_adcontent)
                over (partition by client_id order by is_before_conversion,session_start_at_cet rows between unbounded preceding and unbounded following) last_session_adcontent
              , last_value(session_first_traffic_keyword)
                over (partition by client_id order by is_before_conversion,session_start_at_cet rows between unbounded preceding and unbounded following) last_session_keyword
              , last_value(session_first_ad_group_id)
                over (partition by client_id order by is_before_conversion,session_start_at_cet rows between unbounded preceding and unbounded following) last_session_ad_group_id
              , last_value(session_duration)
                over (partition by client_id order by is_before_conversion,session_start_at_cet rows between unbounded preceding and unbounded following) last_session_duration
              , last_value(session_page_views)
                over (partition by client_id order by is_before_conversion,session_start_at_cet rows between unbounded preceding and unbounded following) last_session_page_views
              , sum(session_duration) over (partition by client_id )                                                                 sum_all_sessions_duration
              , avg(session_duration) over (partition by client_id )                                                                 avg_all_sessions_duration
              , avg(session_page_views) over (partition by client_id )                                                               avg_all_sessions_page_views
              , sum(session_page_views) over (partition by client_id )                                                               sum_all_sessions_page_views
              , sum(multiple_sessions) over (partition by client_id )                                                                sum_total_sessions
         from web_sessions
     ),

     web_sessions_aggregated as (
         select client_id
              , last_session_start_at_cet
              , last_session_device_category
              , last_session_medium
              , last_session_source
              , last_session_campaign
              , last_session_adcontent
              , last_session_keyword
              , last_session_ad_group_id
              , last_session_duration
              , last_session_page_views
              , sum_all_sessions_duration
              , avg_all_sessions_duration
              , avg_all_sessions_page_views
              , sum_all_sessions_page_views
              , sum_total_sessions
         from web_sessions_last_session
         group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
     ),

     web_data as (
         select l.id                                                       as lead_id
              , l.lead_source                                              as lead_source
              , l.created_date                                             as lead_created_at
              , w.client_id                                                as google_client_id
             , case
                    when l.lead_source in
                         ('KP', 'MVF', 'Sonnen', 'Wattfox', 'Wattfox-Exclusive', 'Wattfox-Premium', 'FIBAV', 'Lition', 'Hausfrage')
                        then 'affiliate'
                    when l.lead_source in ('Recommendation', 'Solytic')
                        then 'referral'
                    when l.lead_source in ('Facebook')
                      then 'paidsocial' 
                    when l.lead_source in ('EWE Direct Mailing') then 'direct mailing' --EWE Direct Mailing Campaign added
                    when w.client_id is null and l.lead_source not in
                                                 ('KP', 'MVF', 'Sonnen', 'Wattfox', 'Wattfox-Exclusive',
                                                  'Wattfox-Premium', 'FIBAV', 'Lition', 'Hausfrage')
                        then l.utm_medium_c
                    when w.client_id is not null and l.lead_source not in
                                                     ('KP', 'MVF', 'Sonnen', 'Wattfox', 'Wattfox-Exclusive',
                                                      'Wattfox-Premium', 'FIBAV', 'Lition', 'Hausfrage')
                        then w.last_session_medium
             end                                                          as medium
              , case
                    when l.lead_source in
                         ('Wattfox', 'Sonnen', 'KP', 'MVF', 'Recommendation', 'Facebook', 'FIBAV', 'Lition', 'Hausfrage')
                        then lower(l.lead_source)
                    when l.lead_source in ('Wattfox-Exclusive', 'Wattfox-Premium')
                        then 'wattfox'
                    when l.lead_source in ('EWE Direct Mailing') then 'ewe'  
                    when w.client_id is null and
                         l.lead_source not in ('Wattfox', 'Sonnen', 'KP', 'MVF', 'Recommendation',
                                               'Facebook', 'FIBAV', 'Lition', 'Wattfox-Exclusive',
                                               'Wattfox-Premium', 'Hausfrage')
                        then utm_source_c
                    when w.client_id is not null and
                         l.lead_source not in ('Wattfox', 'Sonnen', 'KP', 'MVF', 'Recommendation',
                                               'Facebook', 'FIBAV', 'Lition', 'Wattfox-Exclusive',
                                               'Wattfox-Premium', 'Hausfrage')
                        then w.last_session_source
             end                                                          as source
              , case
                    when l.lead_source in ('Wattfox')
                        then 'wattfox-standard'
                    when l.lead_source in ('Wattfox-Exclusive', 'Wattfox-Premium')
                        then lower(l.lead_source)
                    when l.lead_source in ('KP') and l.created_date::date >='2020-10-01'
                        then 'kp-standard'
                   when l.lead_source in ('EWE Direct Mailing') then 'dima-ewe-brandenburg-august20' 
                  when l.lead_source in ('Hausfrage') then 'hausfrage-standard' 
                    when w.client_id is null and
                         l.lead_source not in ('Wattfox', 'Wattfox-Exclusive', 'Wattfox-Premium')
                        then l.utm_campaign_c
                    when w.client_id is not null and
                         l.lead_source not in ('Wattfox', 'Wattfox-Exclusive', 'Wattfox-Premium')
                        then w.last_session_campaign
             end                                                           as campaign
              , case
                    when w.client_id is null
                        then l.utm_content_c
                    else w.last_session_adcontent
             end                       as ad_content
              , case
                    when w.client_id is null
                        then l.google_adgroupid_c
                    else w.last_session_ad_group_id
             end                       as ad_group_id
              , case
                    when w.client_id is null
                        then l.google_campaignid_c
                    else ag.campaign_id
              end                                                          as campaign_id
              , w.last_session_keyword                                     as keyword
              , l.lead_cost_c
              , last_session_start_at_cet
              , last_session_device_category
              , last_session_duration
              , last_session_page_views
              , sum_all_sessions_duration
              , avg_all_sessions_duration
              , avg_all_sessions_page_views
              , sum_all_sessions_page_views
              , sum_total_sessions
              , case
                    when l.id is not null then 1
                    else 0
             end                                                           as is_converted_to_lead
         from leads as l
                  full outer join web_sessions_aggregated as w on l.google_client_id = w.client_id
                  left join eigensonne_dwh.adwords4.ad_groups as ag on ag.id = w.last_session_ad_group_id
     ),

     google_ad_groups as (
         select 'google'                                                                                                             as source,
                case
                    when c.adwords_customer_id = '1909631833'
                        then 'display'
                    when c.adwords_customer_id = '1370478568'
                        then 'cpc'
                    else null end                                                                                                    as medium,
                c.name                                                                                                               as campaign_name,
                ag.name                                                                                                              as ad_group_name,
                last_value(ag.id)
                over (partition by c.name, ag.name order by ag.received_at rows between unbounded preceding and unbounded following) as ad_group_id,
                last_value(c.id)
                over (partition by c.name, ag.name order by c.received_at rows between unbounded preceding and unbounded following)  as campaign_id
         from eigensonne_dwh.adwords4.ad_groups ag
                  left join eigensonne_dwh.adwords4.campaigns as c on ag.campaign_id = c.id
         where c.id is not null
     ),
     google_ad_groups_unique as (
         select source::text, medium::text, campaign_name, ad_group_name, ad_group_id, campaign_id
         from google_ad_groups
         group by 1, 2, 3, 4, 5, 6
     ),
     enriched as (
         select web.lead_id,
                web.lead_source,
                web.lead_created_at,
                web.google_client_id,
                coalesce(web.medium, ag_names.medium)                                                                                                                  as medium,
                coalesce(web.source, ag_names.source)                                                                                                                  as source,
                coalesce(web.campaign, ag_names.campaign_name)                                                                                                         as campaign,
              case when web.source like 'taboola' then split_part(substring(web.ad_content,12,100),'-',1)
                    when web.source like 'outbrain' then substring(web.ad_content,36,100)
                    else coalesce(web.ad_content, ag_names.ad_group_name)              end                                                                            as ad_content,
                coalesce(web.campaign_id, ag_ids.campaign_id, tc.id::text, oc.id::text)                                                                               as campaign_id,
                case when web.source like 'taboola' then substring(web.ad_content,1,10)
                    when web.source like 'outbrain' then substring(web.ad_content,1,34)
                    else coalesce(web.ad_group_id, ag_ids.ad_group_id)             end                                                                                 as ad_group_id,
                case
                    when web.campaign is null or web.ad_content is null then null
                    else first_value(web.campaign_id)
                         over (partition by web.campaign, web.ad_content order by web.campaign_id rows between unbounded preceding and unbounded following) end        as for_missing_campaign_id,
                case
                    when web.campaign is null or web.ad_content is null then null
                    else
                                first_value(web.ad_group_id)
                                over (partition by web.campaign, web.ad_content order by web.campaign_id rows between unbounded preceding and unbounded following) end as for_missing_ad_group_id,
                keyword,
                lead_cost_c,
                last_session_start_at_cet,
                last_session_device_category,
                last_session_duration,
                last_session_page_views,
                sum_all_sessions_duration,
                avg_all_sessions_duration,
                avg_all_sessions_page_views,
                sum_all_sessions_page_views,
                sum_total_sessions,
                is_converted_to_lead
         from web_data as web
                  left join google_ad_groups_unique ag_ids
                            on (web.campaign = ag_ids.campaign_name and web.ad_content = ag_ids.ad_group_name)
                  left join google_ad_groups_unique ag_names
                            on (web.campaign_id = ag_names.campaign_id and web.ad_group_id = ag_names.ad_group_id)
                  left join eigensonne_dwh.taboola.campaigns as tc on web.campaign=tc.name
                  left join eigensonne_dwh.outbrain.campaigns as oc on web.campaign=oc.name
     )

select lead_id,
       lead_source,
       lead_created_at,
       google_client_id,
       case
          when source like 'facebook' then 'paidsocial'
          else medium
          end as medium,
       source,
       campaign,
       ad_content,
       coalesce(campaign_id, for_missing_campaign_id) as campaign_id,
       coalesce(ad_group_id, for_missing_ad_group_id) as ad_group_id,
       keyword,
       lead_cost_c,
       last_session_start_at_cet,
       last_session_device_category,
       last_session_duration,
       last_session_page_views,
       sum_all_sessions_duration,
       avg_all_sessions_duration,
       avg_all_sessions_page_views,
       sum_all_sessions_page_views,
       sum_total_sessions,
       is_converted_to_lead
from enriched
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
),

     base4 as (
select l.id
     , l.lead_source
     , case when  ga_sf_lead_acquisition_dimension.medium is null then l.utm_medium_c
        else ga_sf_lead_acquisition_dimension.medium
            end as lead_medium
    , case when h.bundesland like 'Saarland' then 'Rheinland-Pfalz' else h.bundesland end as bundesland
     , l.record_type_name_c
    , case when pardot1.has_opened is null then 0 else pardot1.has_opened end as has_opened
    , case when pardot2.has_clicked is null then 0 else pardot2.has_clicked  end as has_clicked_non_photo_url
    , case when l.waiting_for_pictures_date_time_c is not null
        then (l.waiting_for_pictures_date_time_c::date - l.created_date::date)
       when l.waiting_for_pictures_date_time_c is null and photos_uploaded_at_c is not null
        then  (l.photos_uploaded_at_c::date - l.created_date::date)
        when l.waiting_for_pictures_date_time_c is null and photos_uploaded_at_c is null and l.verify_pictures_date_time_c is not null
        then  (l.verify_pictures_date_time_c::date - l.created_date::date)
    else (loss_date_time_c::date - l.created_date::date)
    end as time_in_queue
, case when l.waiting_for_pictures_date_time_c > l.loss_date_time_c or
                l.verify_pictures_date_time_c > l.loss_date_time_c then 1 else 0
                    end as reccords_opened_again
from eigensonne_dwh.salesforce_production.leads as l
    FULL OUTER JOIN 
       base3
        AS ga_sf_lead_acquisition_dimension ON (l."id") = (ga_sf_lead_acquisition_dimension."lead_id")
left join eigensonne_dwh.public.historization_marketing_territories as h on h.zip = l.postal_code
left join base2 as pardot1 on pardot1.id = l.id
                            and pardot1.has_opened = 1
left join base2 as pardot2 on pardot2.id = l.id
                            and pardot2.has_clicked = 1
where verify_pictures_date_time_c is null
        and loss_date_time_c is null
        and created_date::date >= '2020-10-01'
        and status in ('New', 'Waiting For Pictures')
group by l.id
     , l.lead_source
     , ga_sf_lead_acquisition_dimension.medium
    , l.utm_medium_c
    , h.bundesland
    , l.record_type_name_c
    , pardot1.has_opened
    , pardot2.has_clicked
    , l.verify_pictures_date_time_c
    , l.waiting_for_pictures_date_time_c
    , l.created_date
    , photos_uploaded_at_c
    , loss_date_time_c)

select id
     , case
         when lead_source is null then 'Others'
            else lead_source
             end as lead_source
     , case
         when lead_medium is null then 'Others'
            else lead_medium
             end as lead_medium
    , case
         when bundesland is null then 'Unknown/Others'
            else bundesland
             end as bundesland
     , record_type_name_c
    , has_opened
    , has_clicked_non_photo_url
, time_in_queue
    from base4
    where reccords_opened_again = 0
and time_in_queue >=0
 """
    


# In[4]:


cr = conn.cursor()
result = cr.execute(query)
cols = []

for col in cr.description:
    cols.append(col[0])
    
final_list=[]
for val in cr.fetchall():
    final_list.append(dict(zip(cols,val)))
    
cr.close()
base=pd.DataFrame(final_list)


# In[5]:


data = base.copy(deep=True)


# In[6]:




dummy1 = pd.get_dummies(data[['record_type_name_c','lead_source',
                           'lead_medium','bundesland'
                             ]], drop_first=False)

data = pd.concat([data, dummy1], axis=1)


# In[7]:


data = data.drop(['lead_source', 'lead_medium',	'bundesland',
                             'record_type_name_c'], axis = 1)


# In[8]:



X = data.drop(['id'
              ], axis=1)

scaler = StandardScaler()


X[['time_in_queue']] = scaler.fit_transform(
    X[['time_in_queue']])


# In[9]:


clf_from_joblib = joblib.load('trained_model.pkl') 
list_columns_model=list(clf_from_joblib.conf_int().reset_index().iloc[1:]['index'])

X_list=list(X.columns) 

main_list = list(np.setdiff1d(list_columns_model,X_list))

for col in main_list:
    X[col] = 0

main_list2 = list(np.setdiff1d(X_list,list_columns_model))

for col in main_list2:
    X=X.drop(col,axis=1)


# In[10]:


X_data = sm.add_constant(X)
result= clf_from_joblib.predict(X_data)


# In[11]:



score = pd.DataFrame(result)
score= score.rename(columns={ 0 : 'Lead_Score'})

score['Lead_Score'] = score.Lead_Score.map( lambda x: round(x*100))


# In[12]:


Final= base.merge(score[['Lead_Score']], how='left',left_index=True,right_index=True).reset_index(drop=True)
#Final.head(500)


# In[13]:


Final.head()


# In[34]:


todaysdate=pd.to_datetime('today')
todaysdate2=pd.to_datetime('today').strftime("%m-%d-%Y, %H:%M:%S")
Final1=Final[['id','Lead_Score']]
Final1['datetime']=todaysdate
Final1.head()


# In[35]:


excelfilename="/Users/amareid/leadscoring_output/"+todaysdate2+".xlsx"


# In[36]:


Final1.to_excel(excelfilename)


# In[ ]:




