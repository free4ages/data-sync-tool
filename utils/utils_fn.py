from datetime import datetime
import importlib

import pytz

def get_value(value, *args, **kwargs):
    if callable(value):
        return value(*args, **kwargs)
    else:
        return value
    
def generate_alias(v):
    return v.replace('.', '__') if '.' in v else v

def generate_column_from_alias(v):
    return v.replace('__', '.') if '__' in v else v


def load_class_from_path(path: str):
    """
    Load a class from a string path like 'synctool.adapters.PostgresAdapter'
    """
    module_path, _, class_name = path.rpartition('.')
    if not module_path or not class_name:
        raise ValueError(f"Invalid path: {path}")
    
    try:
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        return cls
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Could not load class '{class_name}' from module '{module_path}': {e}")
    

def find_interval_factor(c, reduction_factor):
    if c <= 1:
        return 1

    # Use binary search
    low = 1
    high = 1000  # Start with a reasonable upper bound

    while low < high:
        mid = (low + high) // 2
        power = (mid * (mid + 1)) // 2
        val = reduction_factor ** power

        if val >= c:
            high = mid
        else:
            low = mid + 1

    return low

def add_tz(dt: datetime, tz= pytz.UTC):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    else:
        return dt
