import queues_sdk

from queues_sdk import Queues
from queues_sdk.watermarks import default_watermarks





def test_version_exported():
    assert queues_sdk.__version__ == "0.1.2"





def test_default_watermarks_shape():
    memory, disk = default_watermarks()
    assert 0 <= int(memory) <= 100 and 0 <= int(disk) <= 100





def test_custom_thresholds():
    q = Queues(memory_max=97, disk_max=98, spill_batch=512, maxsize=1024)
    assert int(q.pressure_memory_max) == 97 and int(q.pressure_disk_max) == 98
    assert int(q.pressure_spill_batch) == 512 and int(q.db.maxlen) == 1024
    assert int(q.almightydb_service_logger_store.maxsize) == 1024*4
