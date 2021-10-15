class SqlQueries:

    block_transaction_table_insert = ("""
        insert into block_transaction (transaction_hash, transaction_index, sender_address,receiver_address, miner_address, wei_value_x9,
        block_timestamp, block_number, block_hash)
        SELECT
            t.hash,
            t.transaction_index,
            t.sender_address,
            t.receiver_address,
            b.miner miner_address,
            case when length(t.value)<=9 and length(value)>0 then cast(t.value as NUMERIC)/1000000
            when length(t.value)>9 then cast(SUBSTRING(t.value, 0, length(t.value)-8)/10 as NUMERIC) 
            else null end wei_value_x9,
            TO_TIMESTAMP(t.block_timestamp, 'YYYY-MM-DD HH24:MI:ss+00:00') block_timestamp,
            t.block_number,
            t.block_hash
        from ( select
            hash,
            nonce sender_trans_cnt,
            transaction_index,
            from_address sender_address,
            to_address receiver_address,
            REGEXP_REPLACE(value, '[^0-9]+','') value,
            gas,
            gas_price,
            receipt_cumulative_gas_used,
            receipt_gas_used,
            block_timestamp,
            block_number,
            block_hash from public.staging_transactions) t
        LEFT JOIN (select "timestamp", number, hash, miner 
                   from staging_blocks) b
                ON t.block_timestamp = b.timestamp
                AND t.block_number = b.number
                AND t.block_hash = b.hash
    """)

    transaction_table_insert = ("""
        insert into transactions (hash, sender_trans_cnt, transaction_index, sender_address,reciever_address, wei_value_x9, gas,
        gas_price, receipt_cumulative_gas_used, receipt_gas_used, block_timestamp, block_number, block_hash)
        select 
        hash,
        sender_trans_cnt,
        transaction_index,
        sender_address,
        reciever_address,
        case when length(value)<=9 and length(value)>0 then cast(value as NUMERIC)/1000000
        when length(value)>9 then cast(SUBSTRING(value, 0, length(value)-8)/10 as NUMERIC) 
        else null end wei_value_x9,
        gas,
        gas_price,
        receipt_cumulative_gas_used,
        receipt_gas_used,
        TO_TIMESTAMP("block_timestamp", 'YYYY-MM-DD HH24:MI:ss+00:00') block_timestamp,
        block_number,
        block_hash
        from ( select
        hash,
        nonce sender_trans_cnt,
        transaction_index,
        from_address sender_address,
        to_address reciever_address,
        REGEXP_REPLACE(value, '[^0-9]+','') value,
        gas,
        gas_price,
        receipt_cumulative_gas_used,
        receipt_gas_used,
        block_timestamp,
        block_number,
        block_hash from public.staging_transactions)
    """)

    block_table_insert = ("""
        insert into block (timestamp,block_number,hash,parent_hash, miner,difficulty,total_difficulty_x9,size_bytes,gas_limit,gas_used,transaction_count)
        select TO_TIMESTAMP("timestamp", 'YYYY-MM-DD HH24:MI:ss+00:00') as timestamp,
        number,
        hash,
        parent_hash,
        miner,
        difficulty,
        CAST(SUBSTRING(total_difficulty, 0, length(total_difficulty)-9) as BIGINT) total_difficulty_x9,
        size,
        gas_limit,
        gas_used,
        transaction_count
        from staging_blocks;
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    