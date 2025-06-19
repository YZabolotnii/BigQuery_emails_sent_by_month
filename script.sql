WITH date_and_mess_count AS (
    SELECT DISTINCT
    date(
        extract(YEAR FROM date_add(s.date, interval es.sent_date DAY)),
        extract(MONTH FROM date_add(s.date, interval es.sent_date DAY)),
        1
    ) as sent_month
    , es.id_account
    , count(es.id_message) over(partition by date_trunc(date_add(s.date, interval es.sent_date DAY), MONTH)) as message_cnt
    , count(es.id_message) over(partition by es.id_account, date_trunc(date_add(s.date, interval es.sent_date DAY), MONTH)) as message_cnt_month
    , min(date_add(s.date, interval es.sent_date DAY)) over(partition by es.id_account) as first_sent_date
    , max(date_add(s.date, interval es.sent_date DAY)) over(partition by es.id_account) as last_sent_date
  FROM `data-analytics-mate.DA.email_sent` es
  JOIN `data-analytics-mate.DA.account` ac
  ON ac.id = es.id_account
  JOIN `data-analytics-mate.DA.account_session` acs
  ON ac.id = acs.account_id
  JOIN `data-analytics-mate.DA.session` s
  ON s.ga_session_id = acs.ga_session_id
)
SELECT
  sent_month
  , id_account
  , message_cnt_month / message_cnt * 100 as sent_msg_percent_from_this_month
  , first_sent_date
  , last_sent_date
FROM date_and_mess_count
ORDER BY 1 DESC