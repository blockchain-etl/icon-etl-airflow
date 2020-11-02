--- average tx volume per destination
with t1 as(
select
	to_address,
	count(*) as total_tx,
	count(distinct(from_address)) as total_active_wallets
from
current_period
group by
to_address )
select
	to_address,
	name,
	total_tx,
	total_active_wallets,
	total_tx / total_active_wallets as average_tx_per_wallet
from
	t1
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
order by
	total_active_wallets desc;