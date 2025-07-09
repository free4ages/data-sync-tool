from typing import List
from typing import Union
from core.config import  ReconciliationConfig, SinkConfig, SourceConfig, StateConfig
from core.query import BlockHashMeta, BlockNameMeta, Join, Query, Field, Filter, RowHashMeta, Table



def build_filters_from_config(config: Union[SourceConfig, SinkConfig, StateConfig]) -> List[Filter]:
    # Build filters
    filters: list[Filter] = []
    if config.filters:
        for filter_cfg in config.filters or []:
            filters.append(Filter(
                column=filter_cfg.column,
                operator=filter_cfg.operator,
                value=filter_cfg.value
            ))
    return filters

def build_joins_from_config(config) -> List[Join]:
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

def build_table_from_config(config) -> Table:
    # Build From
    table = Table(
        table = config.table.table,
        schema = config.table.dbschema,
        alias = config.table.alias
    )
    return table