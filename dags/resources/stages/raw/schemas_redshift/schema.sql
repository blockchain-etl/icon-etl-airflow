CREATE SCHEMA IF NOT EXISTS icon;

DROP TABLE IF EXISTS icon.blocks;

CREATE TABLE icon.blocks (
  number            BIGINT         NOT NULL,     -- The block number
  hash              VARCHAR(65535) NOT NULL,     -- Hash of the block
  parent_hash       VARCHAR(65535) NOT NULL,     -- Hash of the parent block
  merkle_root_hash  VARCHAR(65535) NOT NULL,     -- The root of the merkle trie of the block
  timestamp         BIGINT         NOT NULL,     -- The unix timestamp for when the block was collated
  transaction_count BIGINT         NOT NULL,     -- The number of transactions in the block
  version           VARCHAR(65535) NOT NULL,     -- The version of the block
  signature         VARCHAR(65535) NOT NULL,     -- The block's signature
  next_leader       VARCHAR(65535) NOT NULL,     -- The next leader
  PRIMARY KEY (number)
)
DISTKEY (number)
SORTKEY (timestamp);

--

DROP TABLE IF EXISTS icon.logs;

CREATE TABLE icon.logs (
  log_index         BIGINT         NOT NULL, -- Integer of the log index position in the block
  transaction_hash  VARCHAR(65535) NOT NULL, -- Hash of the transactions this log was created from
  transaction_index BIGINT         NOT NULL, -- Integer of the transactions index position log was created from
  block_hash        VARCHAR(65535) NOT NULL, -- Hash of the block where this log was in
  block_number      BIGINT         NOT NULL, -- The block number where this log was in
  address           VARCHAR(65535) NOT NULL, -- Address from which this log originated
  data              VARCHAR(65535) NOT NULL, -- Contains one or more non-indexed arguments of the log
  indexed           VARCHAR(65535) NOT NULL, -- Indexed log arguments
  PRIMARY KEY (block_number, log_index)
)
DISTKEY (block_number)
SORTKEY (log_index);

--

DROP TABLE IF EXISTS icon.receipts;

CREATE TABLE icon.receipts (
  transaction_hash     VARCHAR(65535) NOT NULL,     -- Hash of the transaction
  transaction_index    BIGINT         NOT NULL,     -- Integer of the transactions index position in the block
  block_hash           VARCHAR(65535) NOT NULL,     -- Hash of the block where this transaction was in
  block_number         BIGINT         NOT NULL,     -- Block number where this transaction was in
  cumulative_step_used BIGINT         NOT NULL,     -- The total amount of step used when this transaction was executed in the block
  step_used            BIGINT         NOT NULL,     -- The amount of step used by this specific transaction alone
  step_price           BIGINT         NOT NULL,     -- The step price
  score_address        VARCHAR(65535) DEFAULT NULL, -- The contract address created, if the transaction was a contract creation, otherwise null
  status               BIGINT         DEFAULT NULL, -- Either 1 (success) or 0 (failure)
  PRIMARY KEY (transaction_hash)
)
DISTKEY (block_number)
SORTKEY (transaction_index);

--

DROP TABLE IF EXISTS icon.transactions;

CREATE TABLE icon.transactions (
  version           VARCHAR(65535) NOT NULL,
  hash              VARCHAR(65535) NOT NULL,     -- Hash of the transaction
  nonce             BIGINT         NOT NULL,     -- The number of transactions made by the sender prior to this one
  block_hash        VARCHAR(65535) NOT NULL,     -- Hash of the block where this transaction was in
  block_number      BIGINT         NOT NULL,     -- Block number where this transaction was in
  transaction_index BIGINT         NOT NULL,     -- Integer of the transactions index position in the block
  from_address      VARCHAR(65535) NOT NULL,     -- Address of the sender
  to_address        VARCHAR(65535) DEFAULT NULL, -- Address of the receiver. null when its a contract creation transaction
  value             NUMERIC(38, 0) NOT NULL,     -- Value transferred
  gas_limit         BIGINT         NOT NULL,     -- Gas provided by the sender
  timestamp         BIGINT         NOT NULL,
  nid               VARCHAR(65535) NOT NULL,
  fee               NUMERIC(38,0)  DEFAULT NULL,
  signature         VARCHAR(65535) NOT NULL,
  data_type         VARCHAR(65535) NOT NULL,
  data              VARCHAR(65535) NOT NULL,     -- The data sent along with the transaction
  PRIMARY KEY (hash)
)
DISTKEY (block_number)
SORTKEY (block_number);