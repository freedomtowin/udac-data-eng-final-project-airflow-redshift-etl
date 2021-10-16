drop table public.staging_prices;
CREATE TABLE if not exists public.staging_prices (
timestamp VARCHAR(100) NOT NULL, 
symbol VARCHAR(100), 
"open" NUMERIC,
high NUMERIC,
low NUMERIC,
close NUMERIC,
volume NUMERIC
);

drop table public.staging_blocks;
CREATE TABLE if not exists public.staging_blocks (
timestamp	VARCHAR(400) NOT NULL,
number	BIGINT NOT NULL,
hash	VARCHAR(8000) NOT NULL,
parent_hash	VARCHAR(8000),
nonce	VARCHAR(8000) NOT NULL,
sha3_uncles	VARCHAR(8000),
logs_bloom	VARCHAR(8000),
transactions_root	VARCHAR(8000),
state_root	VARCHAR(8000),
receipts_root	VARCHAR(8000),
miner	VARCHAR(8000),
difficulty	BIGINT, 
total_difficulty VARCHAR(8000),
size	NUMERIC,
extra_data	VARCHAR(8000),
gas_limit	NUMERIC,
gas_used	NUMERIC,
transaction_count	NUMERIC
);

drop table public.staging_transactions;
CREATE TABLE if not exists public.staging_transactions (
hash	VARCHAR(8000) NOT NULL,
nonce	INTEGER NOT NULL,
transaction_index	BIGINT NOT NULL,
from_address	VARCHAR(8000) NOT NULL,
to_address	VARCHAR(8000),
value	VARCHAR(8000),
gas	NUMERIC,
gas_price	NUMERIC,
input	VARCHAR(26000),
receipt_cumulative_gas_used	INTEGER,
receipt_gas_used	INTEGER,
receipt_contract_address	VARCHAR(8000),
receipt_root	VARCHAR(8000), 
receipt_status	INTEGER, 
block_timestamp	VARCHAR(400) NOT NULL,
block_number	INTEGER NOT NULL,
block_hash	VARCHAR(8000) NOT NULL
);


drop table block_transaction;
CREATE TABLE if not exists public.block_transaction (
transaction_hash	VARCHAR(512) NOT NULL,
transaction_index	INTEGER NOT NULL,
sender_address	VARCHAR(512)  NOT NULL,
receiver_address	VARCHAR(512),
miner_address	VARCHAR(512),
wei_value_x9 NUMERIC,
price NUMERIC,
price_timestamp	TIMESTAMP,
block_timestamp	TIMESTAMP  NOT NULL,
block_number	BIGINT  NOT NULL,
block_hash	VARCHAR(512)  NOT NULL
);

CREATE TABLE if not exists public.transactions (
hash	VARCHAR(8000) NOT NULL,
sender_trans_cnt	INTEGER NOT NULL,
transaction_index	BIGINT NOT NULL,
sender_address	VARCHAR(512) NOT NULL,
reciever_address	VARCHAR(512),
wei_value_x9	NUMERIC,
gas	NUMERIC,
gas_price	NUMERIC,
receipt_cumulative_gas_used	INTEGER,
receipt_gas_used	INTEGER,
block_timestamp	VARCHAR(512) NOT NULL,
block_number	INTEGER NOT NULL,
block_hash	VARCHAR(512) NOT NULL,
CONSTRAINT transaction_hash_pkey PRIMARY KEY (hash)
);

drop table block;
CREATE TABLE if not exists public.block (
timestamp	TIMESTAMP NOT NULL,
block_number	BIGINT NOT NULL,
hash	VARCHAR(512) NOT NULL,
parent_hash	VARCHAR(512),
miner	VARCHAR(512),
difficulty	NUMERIC,
total_difficulty_x9	NUMERIC,
size_bytes	NUMERIC,
gas_limit	NUMERIC,
gas_used	NUMERIC,
transaction_count	NUMERIC,
CONSTRAINT block_hash_pkey PRIMARY KEY (hash)
);

drop table price;
CREATE TABLE if not exists public.price (
timestamp	TIMESTAMP NOT NULL,
"open" NUMERIC,
high NUMERIC,
low NUMERIC,
close NUMERIC,
volume NUMERIC
);
