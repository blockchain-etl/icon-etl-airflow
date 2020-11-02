--- average tx volume per active wallet
with t1 as(
select
	count(*) as total_tx,
	count(distinct(from_address)) as total_active_wallets
from
current_period)
select
	*,
	total_tx / total_active_wallets as average_tx_per_wallet
from
	t1;
