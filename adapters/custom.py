from typing import Dict, List
from adapters.base import Adapter
from core.query import Query

class CustomAdapter(Adapter):

    def connect(self): 
        pass

    def fetch(self, query: Query) -> List[Dict]:
        pass

    def execute(self, sql: str, params=None):
        pass

    def insert_or_update(self, table: str, row: dict):
        pass

    def close(self):
        pass

    def fetch_one(self, query:Query) -> Dict:
        pass
