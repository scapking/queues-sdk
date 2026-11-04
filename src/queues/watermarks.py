import os





def default_watermarks() -> tuple[int, int]:
    try:
        memory_total = int(0); memory_available = int(0)
        with open("/proc/meminfo", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemTotal:"): memory_total = int(line.split()[1])
                elif line.startswith("MemAvailable:"): memory_available = int(line.split()[1])
        usage = os.statvfs("/")
        disk_percent = int(100 - usage.f_bavail * 100 / max(int(usage.f_blocks), 1))
        memory_percent = int(100 - memory_available * 100 / max(memory_total, 1))
        return (int(memory_percent), int(disk_percent))

    except OSError: return (int(0), int(0))
