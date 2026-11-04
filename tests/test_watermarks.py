import queues

from queues import Queues
from queues.watermarks import default_watermarks





def test_version_exported():
    # 版本单一来源 = pyproject.toml，发版漏改直接红。
    import pathlib, re
    pyproject = pathlib.Path(__file__).resolve().parent.parent / "pyproject.toml"
    expected = re.search(r'^version = "(.+)"', pyproject.read_text(encoding="utf-8"), re.MULTILINE).group(1)
    assert queues.__version__ == expected





def test_default_watermarks_shape():
    memory, disk = default_watermarks()
    assert 0 <= int(memory) <= 100 and 0 <= int(disk) <= 100





def test_custom_thresholds():
    q = Queues(memory_max=97, disk_max=98, spill_batch=512, maxsize=1024)
    assert int(q.pressure_memory_max) == 97 and int(q.pressure_disk_max) == 98
    assert int(q.pressure_spill_batch) == 512 and int(q.db.maxlen) == 1024
    assert int(q.almightydb_service_logger_store.maxsize) == 1024*4
