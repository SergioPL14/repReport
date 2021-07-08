set @startDate := '2021-06-01';

set @endDate := '2021-06-31';

select ifnull(concat('sdate: ', @startDate), 'bluff1') as sdate, ifnull(concat('edate: ', @endDate), 'bluff2') as edate;

