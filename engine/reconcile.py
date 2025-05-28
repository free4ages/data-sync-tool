from datetime import UTC, datetime, date, timedelta
import math
from typing import List, Literal, Tuple, Dict, Union
from adapters.base import Adapter
from core.config import HASH_MD5_HASH, MD5_SUM_HASH, PipelineConfig, ReconciliationConfig, SinkConfig, SourceConfig, StateConfig
from core.query import BlockHashMeta, BlockNameMeta, Join, Query, Field, Filter, Table
from utils.utils_fn import add_tz, find_interval_factor, get_value


Status = Dict[Tuple[datetime, datetime, int], str]


def build_filters_from_config(config):
    # Build filters
    filters = []
    if config.filters:
        for filter_cfg in config.filters or []:
            filters.append(Filter(
                column=filter_cfg.column,
                operator=filter_cfg.operator,
                value=filter_cfg.value
            ))
    return filters

def build_joins_from_config(config):
    # Build joins
    joins = []
    if config.joins:
        for join_cfg in config.joins or []:
            joins.append(Join(
                table=join_cfg.table,
                alias=join_cfg.alias,
                on=join_cfg.on,
                type=join_cfg.type
            ))
    return joins

def build_table_from_config(config):
    # Build From
    table = Table(
        table = config.table.table,
        schema = config.table.dbschema,
        alias = config.table.alias
    )
    return table


def build_data_range_query(r_config: ReconciliationConfig, src_config: Union[SourceConfig, SinkConfig, StateConfig]):
    # get min/max of partition field from source
    partition_column = r_config.source_state_pfield.partition_column

    # select fields
    select = [
        Field(expr=f"min({partition_column})", alias='start', type='column'),
        Field(expr=f"max({partition_column})", alias='end', type='column')
    ]
    return Query(
        select = select,
        table= build_table_from_config(src_config),
        joins= build_joins_from_config(src_config),
        filters= build_filters_from_config(src_config)
    )

def partition_generator(start, end, initial_partition_interval, partition_column_type):
    # import pdb;pdb.set_trace()
    # considers [start,end] as inclusive
    if partition_column_type == "datetime":
        initial_partition_interval = int(initial_partition_interval or 365*24*60*60)
        start_timestamp = math.floor(start.timestamp())
        end_timestamp = math.ceil(end.timestamp())
        cur = start_timestamp
        while cur<end_timestamp:
            cur1 = ((cur+initial_partition_interval)//initial_partition_interval)*initial_partition_interval
            s1 = datetime.fromtimestamp(cur, start.tzinfo)
            e1 = datetime.fromtimestamp(min(cur1, end_timestamp), end.tzinfo)
            yield (s1, e1)
            cur = cur1
        # for x in range(start_timestamp, end_timestamp+1, initial_partition_interval):
        #     # convert back to datetime
        #     s1 = datetime.fromtimestamp(x, start.tzinfo)
        #     e1 = datetime.fromtimestamp(min(x+initial_partition_interval-1, end_timestamp), end.tzinfo)
        #     yield (s1, e1)
    elif partition_column_type == "int":
        initial_partition_interval = initial_partition_interval or 200000
        cur = start
        while cur<end:
            cur1 = ((cur+initial_partition_interval)//initial_partition_interval)*initial_partition_interval
            yield(cur, min(cur1, end))
            cur= cur1
        # for x in range(start, end+1, initial_partition_interval):
        #     s1 = x
        #     e1 = min(x+initial_partition_interval-1, end)
        #     yield (s1, e1)

def get_data_range(
        sourcestate: Adapter,
        sinkstate: Adapter,
        r_config:ReconciliationConfig,
        start=None,
        end=None
    ):
    # import pdb;pdb.set_trace()
    user_start = get_value(r_config.start) if start is None else start
    user_end = get_value(r_config.end) if end is None else end

    sstart = user_start
    send = user_end

    if not (user_start and user_end):
        if r_config.strategy in (MD5_SUM_HASH, HASH_MD5_HASH):
            query = build_data_range_query(r_config, sourcestate.adapter_config)
            result = sourcestate.fetch_one(query, op_name="range_search")
            sstart = result["start"] if result else None
            send = result["end"] if result else None

            query = build_data_range_query(r_config, sinkstate.adapter_config)
            result = sinkstate.fetch_one(query, op_name="range_search")
            skstart = result["start"] if result else None
            skend = result["end"] if result else None

            sstart = min(sstart,skstart) if (sstart and skstart) else (sstart or skstart)
            send = max(send,skend) if (send and skend) else (send or skend)

            if send:
                # add buffer of 1 for exclusive range
                send = send+timedelta(seconds=1) if r_config.partition_column_type == "datetime" else send+1
    
    sstart = max(user_start, sstart) if user_start else sstart
    send = min(user_end, send) if user_end else send

    return sstart, send

class Block:
    def __init__(self, start: Union[datetime,int,str], end: Union[datetime,int,str], level: int, num_rows: int, hash: str):
        self.start = start
        self.end = end
        self.level = level
        self.num_rows = num_rows
        self.hash = hash

# Build Query object based on config and partition type

def build_block_hash_query(
    start: Union[datetime,int,str], 
    end: Union[datetime,int,str], 
    level: int,
    intervals: List[int],
    config: Union[StateConfig, SourceConfig, SinkConfig],
    r_config: ReconciliationConfig,
    target: Literal["source", "sink"]
) -> Query:
    # import pdb;pdb.set_trace()
    partition_column_type = r_config.partition_column_type
    if target=="source":
        hash_column = r_config.source_state_pfield.hash_column
        partition_column = r_config.source_state_pfield.partition_column
        order_column = r_config.source_state_pfield.order_column
    else:
        hash_column = r_config.sink_state_pfield.hash_column
        partition_column = r_config.sink_state_pfield.partition_column
        order_column = r_config.sink_state_pfield.order_column
    block_hash_field = Field(
        expr=f"{partition_column}", 
        alias="blockhash", 
        metadata=BlockHashMeta(
            order_column=order_column,
            partition_column = partition_column,
            hash_column = hash_column,
            strategy = r_config.strategy,
            fields = [Field(expr=x.column) for x in config.fields],
            partition_column_type=partition_column_type
        ), 
        type="blockhash"
    )

    block_name_field = Field(expr=f"blockname", alias="blockname", metadata=BlockNameMeta(
        level=level,
        intervals=intervals,
        partition_column_type=partition_column_type,
        strategy=r_config.strategy,
        partition_column=partition_column   
    ), type="blockname")

    select = [
        Field(expr='COUNT(1)', alias='row_count', type='column'),
        block_hash_field,
        block_name_field
    ]
    grp_field = Field(expr="blockname", type="column") 

    filters = []
    if partition_column_type=="datetime":
        filters += [
             Filter(column=partition_column, operator='>=', value=start.strftime("%Y-%m-%d %H:%M:%S")), 
             Filter(column=partition_column, operator='<', value=end.strftime("%Y-%m-%d %H:%M:%S"))
        ]
    elif partition_column_type == "int":
        filters += [
             Filter(column=partition_column, operator='>=', value=start), 
             Filter(column=partition_column, operator='<', value=end)
        ]
    elif partition_column_type == "str":
        filters += [
             Filter(column=partition_column, operator='>=', value=start), 
             Filter(column=partition_column, operator='<', value=end)
        ]
    filters += build_filters_from_config(config)
    query = Query(
        select=select,
        table=build_table_from_config(config),
        joins=build_joins_from_config(config) if hasattr(config, 'joins') else [],
        filters=filters,
        group_by=[grp_field]
    )
    return query

def to_blocks(
    blocks_data,
    r_config: ReconciliationConfig, 
    start, 
    end,  
    level, 
    intervals
):
    # import pdb;pdb.set_trace()
    blocks = []
    for block_data in blocks_data:
        blockname, blockhash, row_count = block_data["blockname"], block_data["blockhash"], block_data["row_count"]
        parts = [int(x) for x in blockname.split('-')]
        block_start = 0
        for index, part_num in enumerate(parts):
            block_start += part_num*intervals[index]
        block_end = block_start+intervals[level-1]
        if r_config.partition_column_type == "datetime":
            # Convert timestamps to datetime objects with the same timezone awareness as start/end
            if getattr(start, 'tzinfo', None) is not None:
                # If start is timezone-aware, create timezone-aware datetimes
                block_start_dt = datetime.fromtimestamp(block_start, start.tzinfo)
                block_end_dt = datetime.fromtimestamp(block_end, end.tzinfo)
            else:
                # If start is timezone-naive, create timezone-naive datetimes
                block_start_dt = datetime.fromtimestamp(block_start)
                block_end_dt = datetime.fromtimestamp(block_end)
            
            block_start = max(block_start_dt, start)
            block_end = min(block_end_dt, end)
        elif r_config.partition_column_type == "int":
            block_start = max(block_start, start)
            block_end = min(block_end,end)
        block = Block(
            start=block_start, 
            end=block_end, 
            level=level,
            num_rows=row_count, 
            hash=blockhash
        )
        blocks.append(block)
    return blocks



def calculate_block_status(src_blocks: List[Block], snk_blocks: List[Block]) -> Tuple[List[Block], Status]:
    status: Status = {}
    all_keys = {(c.start, c.end, c.level) for c in src_blocks} | {(c.start, c.end, c.level) for c in snk_blocks}
    src_map = {(c.start, c.end, c.level): c for c in src_blocks}
    snk_map = {(c.start, c.end, c.level): c for c in snk_blocks}
    result = []

    for key in sorted(all_keys):
        sc = src_map.get(key)
        kc = snk_map.get(key)
        sel = sc or kc
        if sc and kc:
            sel = sc if sc.num_rows> kc.num_rows else kc
        if sc and kc and sc.num_rows == kc.num_rows and sc.hash == kc.hash:
            status[key] = 'N'
        elif sc and kc:
            status[key] = 'M'
        elif sc and not kc:
            status[key] = 'A'
        else:
            status[key] = 'D'
        result.append(sel)

    return result, status           

# Merge adjacent blocks with the same status ('M' or 'A') to optimize fewer queries
def merge_adjacent(blocks: List[Block], statuses: List[str], max_block_size: int) -> Tuple[List[Block], List[str]]:
    merged_blocks, merged_statuses = [], []

    for c, st in zip(blocks, statuses):
        if st in ('M', 'A') and merged_blocks and merged_statuses[-1] == st and merged_blocks[-1].num_rows+c.num_rows <= max_block_size:
            prev = merged_blocks[-1]
            prev.end = max(prev.end, c.end)
            prev.num_rows += c.num_rows
        else:
            merged_blocks.append(c)
            merged_statuses.append(st)

    return merged_blocks, merged_statuses

# # Main recursive calculation function
def calculate_blocks(
    sourcestate: Adapter, 
    sinkstate: Adapter, 
    start: Union[datetime,int,str], 
    end: Union[datetime,int,str], 
    level: int ,
    r_config: ReconciliationConfig,
    intervals: List[int], 
    max_block_size: int,
    max_level =100,
    force_update=False
) -> Tuple[List[Block], List[str]]:
    # import pdb;pdb.set_trace()
    source_query = build_block_hash_query(
        start, 
        end, 
        level,
        intervals,
        sourcestate.adapter_config,
        r_config,
        "source"
    )
    sink_query = build_block_hash_query(
        start, 
        end, 
        level,
        intervals,
        sinkstate.adapter_config,
        r_config,
        "sink"
    )

    src_rows = sourcestate.fetch(source_query)
    snk_rows = sinkstate.fetch(sink_query)

    # import pdb;pdb.set_trace()
    s_blocks = to_blocks(src_rows, r_config, start, end, level, intervals)
    t_blocks = to_blocks(snk_rows, r_config, start, end, level, intervals)

    blocks, status_map = calculate_block_status(s_blocks, t_blocks)

    final_blocks, statuses = [], []
    for c in blocks:
        key = (c.start, c.end, c.level)
        st = status_map[key]

        if st in ('M', 'A') and c.num_rows > max_block_size and level< max_level:
            # intervals[level] = math.floor(intervals[-1]/interval_reduction_factor)
            deeper_blocks, deeper_statuses = calculate_blocks(
                sourcestate, 
                sinkstate, 
                c.start, 
                c.end, 
                level+1,
                r_config,
                intervals, 
                max_block_size,
                max_level,
                force_update=force_update
            )
            final_blocks.extend(deeper_blocks)
            statuses.extend(deeper_statuses)
        else:
            final_blocks.append(c)
            statuses.append(st)
    return final_blocks, statuses

    # merged_blocks, merged_statuses = merge_adjacent(final_blocks, statuses)
    # return merged_blocks, merged_statuses


def build_blocks(
    sourcestate: Adapter, 
    sinkstate: Adapter, 
    start: Union[datetime,int,str], 
    end: Union[datetime,int,str], 
    r_config: ReconciliationConfig,
    max_block_size: int,
    intervals=[],
    force_update=False
    
)->Tuple[List[Block], List[str]]:
    # import pdb;pdb.set_trace()
    partition_column_type = r_config.partition_column_type
    max_level = len(intervals)
    if partition_column_type == "datetime":
        start, end = add_tz(start), add_tz(end)
    # @TODO implement parallelism for fast chunk calculation
    # import pdb;pdb.set_trace()
    all_blocks, all_statuses = [], []
    # import pdb;pdb.set_trace()
    for s, e in partition_generator(start, end, intervals[0], partition_column_type):
        blocks, statuses = calculate_blocks(
            sourcestate, 
            sinkstate, 
            s, 
            e, 
            1,
            r_config,
            intervals,
            max_block_size,
            max_level,
            force_update=force_update
        )
        all_blocks.extend(blocks)
        all_statuses.extend(statuses)
    return merge_adjacent(all_blocks, all_statuses, max_block_size)


def prepare_data_blocks(
    pipeline: 'Pipeline',
    r_config: ReconciliationConfig,
    initial_partition_interval=None,
    max_block_size: int=None,
    interval_reduction_factor=None,
    start: Union[int, str, datetime]=None,
    end: Union[int, str, datetime]=None,
    force_update=False
)-> Tuple[List[Block], List[str]]:
    source, sink = pipeline.source, pipeline.sink
    sourcestate, sinkstate = pipeline.sourcestate, pipeline.sinkstate
    start, end = get_data_range(sourcestate, sinkstate, r_config, start=start, end=end)

    max_block_size = max_block_size or r_config.batch_size
    # sink_max_block_size = sink_max_block_size or sink.adapter_config.batch_size
    initial_partition_interval = initial_partition_interval or r_config.initial_partition_interval
    intervals = []
    interval = initial_partition_interval
    while interval>max_block_size:
        intervals.append(interval)
        interval = interval//interval_reduction_factor
    intervals.append(interval)

    blocks, status = build_blocks(sourcestate, sinkstate, start, end, r_config, max_block_size, intervals, force_update=force_update)
    return blocks, status
