from engine.reconcile import prepare_data_blocks
from datetime import datetime
from typing import Union
import signal
from typing import List
from core.config import DatastoreConfig, ExternalStoreConfig, PipelineConfig, ReconciliationConfig
from core.db_factory import get_external_adapter
from utils.config_loader import load_config
from core.db_factory import get_adapter
from core.query import Query, Field, Filter
from engine.transform import apply_transform
from engine.enrich import apply_enrichment
from utils.utils_fn import get_value

def _handle_shutdown(signum, frame):
    raise SystemExit(f"Shutdown signal {signum}")

signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# def build_data_partitions(p_conf, src, snk):
#     r_conf = p_conf.reconciliation
#     if r_conf.method=='hash_check':
#         # get the start, end , initial_partition_interval and build initial partition intervals
#         partition_column = r_conf.partition_column
#         partition_column_type = r_conf.partition_column_type
#         initial_partition_interval=r_conf.initial_partition_interval
#         provided_start = getattr(r_conf, 'start', None)
#         provided_end = getattr(r_conf, 'end', None)

#         if not provided_start or not provided_end:
#             query = Query(
#                 select=[
#                     Field(expr=f"min({partition_column})", alias='start', type='column'),
#                     Field(expr=f"max({partition_column})", alias='end', type='hash')
#                 ],
#             )


        




    

# def run_pipeline(config_path: str):
#     cfg = load_config(config_path)
#     datastores = cfg.datastores
#     for pl in cfg.pipelines:
#         # prepare adapters
#         src_cfg = pl.source.datastore
#         snk_cfg = pl.sink.datastore
#         src = get_adapter(src_cfg, datastores, 'source'); src.connect()
#         snk = get_adapter(snk_cfg, datastores, 'sink');   snk.connect()

#         # optional source state/sink state adapters
#         if getattr(pl,'sourcestate', None):
#             s_state = pl.sourcestate
#             if getattr(s_state, 'use_source'):
#                 ss = src
#             else:
#                 ss = get_adapter(pl.sourcestate.datastore, datastores, 'state'); ss.connect()
#         if getattr(pl,'sinkstate', None):
#             s_state = pl.sinkstate
#             if getattr(s_state,'use_sink'):
#                 ds = snk
#             else:
#                 ds = get_adapter(pl.sinkstate.datastore, datastores, 'state'); ss.connect()

#         # fetch rows (hash_check or updated_at)
#         if pl.reconciliation.method=='hash_check':
#             # build and execute checksum queries via QueryBuilder (abstracted)
#             data_partitions = build_data_partitions(config)
#         else:
#             # updated_at: fetch all rows via source.fetch()
#             rows = src.fetch()

#         # process in batches
#         batch_size = getattr(pl.source,'batch_size', 100)
#         for i in range(0, len(rows), batch_size):
#             batch = rows[i:i+batch_size]
#             for row in batch:
#                 rec = apply_transform(row, pl.transform)
#                 rec = apply_enrichment(rec, getattr(pl, 'enrichment',[]), pl.get('redis',{}))
#                 snk.insert_or_update(pl['sink']['schema'] + '.' + pl['sink']['table'], rec)
#             # update state if needed

#         # teardown
#         src.close(); snk.close()
#         if pl.sourcestate: ss.close()
#         if pl.sinkstate: ds.close()


class Pipeline:

    def __init__(self, config: PipelineConfig, datastores: List[DatastoreConfig]=[], externalstores: List[ExternalStoreConfig]=[]):
        self.config = config
        self.source = get_adapter(config.source.datastore, datastores, config=config.source, role='source')
        self.source.connect()

        self.sink = get_adapter(config.sink.datastore, datastores, config=config.sink, role='sink')
        self.sink.connect()

        self.sourcestate = None
        if config.sourcestate:
            self.sourcestate = get_adapter(config.sourcestate.datastore, datastores, config=config.sourcestate, role='sourcestate')
            self.sourcestate.connect()

        self.sinkstate = None
        if config.sinkstate :
            self.sinkstate = get_adapter(config.sinkstate.datastore, datastores, config=config.sinkstate, role='sinkstate')
            self.sinkstate.connect()

        # self.external_stores = {}
        # if config.enrichment:
        #     for e in config.enrichment:
        #         if e.externalstore and e.externalstore not in self.external_stores:
        #             self.external_stores[e.externalstore] = get_external_adapter(e.externalstore, externalstores)

    
    def run(self, 
        recon_name='default',
        initial_partition_interval=None,
        max_block_size: int= 1000,
        interval_reduction_factor=10,
        start: Union[int, str, datetime, None]=None,
        end: Union[int, str, datetime, None]=None,
        force_update=False
    ):
        rconfig: ReconciliationConfig | None = next((p for p in self.config.reconciliation if p.name == recon_name), None)
        if not rconfig:
            raise ValueError(f"Reconciliation config with name {recon_name} not found")
        blocks, status = prepare_data_blocks(self, rconfig, initial_partition_interval, max_block_size, interval_reduction_factor, start, end, force_update)

        load(self, rconfig, blocks, status)

