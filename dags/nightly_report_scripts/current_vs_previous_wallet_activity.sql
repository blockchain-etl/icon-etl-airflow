with t1 as(
select
	to_address,
	count(distinct(from_address)) as total_active_wallets_pp
from
previous_period
where
from_address like 'hx%'
group by
to_address ),
t2 as (
select
	to_address,
	count(distinct(from_address)) as total_active_wallets_cp
from
current_period
where
from_address like 'hx%'
group by
to_address
)
select t1.to_address,
name,
t1.total_active_wallets_pp,
t2.total_active_wallets_cp,
t2.total_active_wallets_cp - t1.total_active_wallets_pp as delta
from t1
left join (
	select
		*
	from
		(
		select
			address, exchange as name
		from
			exchange_wallets
	union all
		select
			address, name
		from
			score_addresses ) as interest_address) as ia on
	to_address = ia.address
join t2 on t1.to_address = t2.to_address
order by delta