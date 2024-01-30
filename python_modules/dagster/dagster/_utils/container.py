import logging
import os
from enum import Enum
from typing import Optional, TypedDict

import pendulum


def cpu_usage_path_cgroup_v1():
    """Path to the cgroup file containing the CPU time in nanoseconds.

    We use cgroup files instead of the psutil library because psutil uses the host machine's CPU allocation in virtualized environments like Docker, K8s, ECS, etc.
    """
    return os.getenv("DAGSTER_CPU_USAGE_PATH", "/sys/fs/cgroup/cpuacct/cpuacct.usage")


def cpu_stat_path_cgroup_v2():
    """Path to the cgroup file containing current CPU stats in microseconds.

    We use cgroup files instead of the psutil library because psutil uses the host machine's CPU allocation in virtualized environments like Docker, K8s, ECS, etc.
    """
    return os.getenv("DAGSTER_CPU_STAT_PATH", "/sys/fs/cgroup/cpu.stat")


def cpu_max_path_cgroup_v2():
    """Path to the cgroup file containing the maximum CPU quota per period in microseconds.

    We use cgroup files instead of the psutil library because psutil uses the host machine's CPU allocation in virtualized environments like Docker, K8s, ECS, etc.
    """
    return os.getenv("DAGSTER_CPU_MAX_PATH", "/sys/fs/cgroup/cpu.max")


def cpu_cfs_quota_us_path():
    """Path to the cgroup file containing the CPU quota in microseconds.

    We use cgroup files instead of the psutil library because psutil uses the host machine's CPU allocation in virtualized environments like Docker, K8s, ECS, etc.
    """
    return os.getenv("DAGSTER_CPU_CFS_QUOTA_US_PATH", "/sys/fs/cgroup/cpu/cpu.cfs_quota_us")


def cpu_cfs_period_us_path():
    """Path to the cgroup file containing the CPU period in microseconds.

    We use cgroup files instead of the psutil library because psutil uses the host machine's CPU allocation in virtualized environments like Docker, K8s, ECS, etc.
    """
    return os.getenv("DAGSTER_CPU_CFS_PERIOD_US_PATH", "/sys/fs/cgroup/cpu/cpu.cfs_period_us")


def cpu_shares_path():
    """Path to the cgroup file containing the CPU shares.

    We use cgroup files instead of the psutil library because psutil uses the host machine's CPU allocation in virtualized environments like Docker, K8s, ECS, etc.
    """
    return os.getenv("DAGSTER_CPU_SHARES_PATH", "/sys/fs/cgroup/cpu/cpu.shares")


def cpu_info_path():
    """Path to the file containing the number of cores allocated to the container."""
    return os.getenv("DAGSTER_CPU_INFO_PATH", "/proc/cpuinfo")


def memory_usage_path_cgroup_v1():
    """Path to the cgroup file containing the memory usage in bytes.

    We use cgroup files instead of the psutil library because psutil uses the host machine's memory allocation in virtualized environments like Docker, K8s, ECS, etc.
    """
    return os.getenv("DAGSTER_MEMORY_USAGE_PATH_V1", "/sys/fs/cgroup/memory/memory.usage_in_bytes")


def memory_usage_path_cgroup_v2():
    """Path to the cgroup file containing the memory usage in bytes.

    We use cgroup files instead of the psutil library because psutil uses the host machine's memory allocation in virtualized environments like Docker, K8s, ECS, etc.
    """
    return os.getenv("DAGSTER_MEMORY_USAGE_PATH_V2", "/sys/fs/cgroup/memory.current")


def memory_limit_path_cgroup_v1():
    """Path to the cgroup file containing the memory limit in bytes.

    We use cgroup files instead of the psutil library because psutil uses the host machine's memory allocation in virtualized environments like Docker, K8s, ECS, etc.
    """
    return os.getenv("DAGSTER_MEMORY_LIMIT_PATH_V1", "/sys/fs/cgroup/memory/memory.limit_in_bytes")


def memory_limit_path_cgroup_v2():
    """Path to the cgroup file containing the memory limit in bytes.

    We use cgroup files instead of the psutil library because psutil uses the host machine's memory allocation in virtualized environments like Docker, K8s, ECS, etc.
    """
    return os.getenv("DAGSTER_MEMORY_LIMIT_PATH_V2", "/sys/fs/cgroup/memory.max")


def filestystem_info_path():
    """Path to the file containing the filesystem information."""
    return os.getenv("DAGSTER_FILESYSTEM_INFO_PATH", "/proc/filesystems")


class CGroupVersion(Enum):
    V1 = "V1"
    V2 = "V2"


class ContainerUtilizationMetrics(TypedDict):
    num_allocated_cores: Optional[int]
    cpu_usage: Optional[float]  # CPU usage in seconds
    cpu_cfs_quota_us: Optional[float]  # CPU quota per period in microseconds
    cpu_cfs_period_us: Optional[float]  # CPU period in microseconds
    memory_usage: Optional[float]  # Memory usage in bytes
    memory_limit: Optional[int]  # Memory limit in bytes
    measurement_timestamp: Optional[float]
    previous_cpu_usage: Optional[float]
    previous_measurement_timestamp: Optional[float]
    cgroup_version: Optional[str]


def retrieve_containerized_utilization_metrics(
    logger: Optional[logging.Logger],
    previous_measurement_timestamp: Optional[float],
    previous_cpu_usage: Optional[float],
) -> ContainerUtilizationMetrics:
    """Retrieve the CPU and memory utilization metrics from cgroup and proc files."""
    cgroup_version = _retrieve_cgroup_version(logger)
    return {
        "num_allocated_cores": _retrieve_containerized_num_allocated_cores(logger),
        "cpu_usage": _retrieve_containerized_cpu_usage(logger, cgroup_version),
        "previous_cpu_usage": previous_cpu_usage,
        "previous_measurement_timestamp": previous_measurement_timestamp,
        "cpu_cfs_quota_us": _retrieve_containerized_cpu_cfs_quota_us(logger, cgroup_version),
        "cpu_cfs_period_us": _retrieve_containerized_cpu_cfs_period_us(logger, cgroup_version),
        "memory_usage": _retrieve_containerized_memory_usage(logger, cgroup_version),
        "memory_limit": _retrieve_containerized_memory_limit(logger, cgroup_version),
        "measurement_timestamp": pendulum.now("UTC").float_timestamp,
        "cgroup_version": cgroup_version.value if cgroup_version else None,
    }


def _retrieve_cgroup_version(logger: Optional[logging.Logger]) -> Optional[CGroupVersion]:
    try:
        with open(filestystem_info_path()) as f:
            lines = f.readlines()
            if any("cgroup2" in line for line in lines):
                return CGroupVersion.V2
            elif any("cgroup" in line for line in lines):
                return CGroupVersion.V1
            else:
                return None
    except Exception as e:
        if logger:
            logger.info(f"No cgroup version found: {e}")
        return None


def _retrieve_containerized_cpu_usage(
    logger: Optional[logging.Logger], cgroup_version: Optional[CGroupVersion]
) -> Optional[float]:
    """Retrieve the CPU time in seconds from the cgroup file."""
    if cgroup_version == CGroupVersion.V1:
        return _retrieve_containerized_cpu_usage_v1(logger)
    elif cgroup_version == CGroupVersion.V2:
        return _retrieve_containerized_cpu_usage_v2(logger)
    else:
        return None


def _retrieve_containerized_cpu_usage_v1(logger: Optional[logging.Logger]) -> Optional[float]:
    try:
        with open(cpu_usage_path_cgroup_v1()) as f:
            return float(f.read()) / 1e9  # Cpuacct.usage is in nanoseconds
    except Exception as e:
        if logger:
            logger.error(f"Failed to retrieve CPU time from cgroup: {e}")
        return None


def _retrieve_containerized_cpu_usage_v2(logger: Optional[logging.Logger]) -> Optional[float]:
    try:
        with open(cpu_stat_path_cgroup_v2()) as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith("usage_usec"):
                    return float(line.split()[1]) / 1e6  # Cpu.stat usage_usec is in microseconds
            return None
    except Exception as e:
        if logger:
            logger.error(f"Failed to retrieve CPU time from cgroup: {e}")
        return None


def _retrieve_containerized_num_allocated_cores(logger: Optional[logging.Logger]) -> Optional[int]:
    """Retrieve the number of cores from the /proc/cpuinfo file."""
    try:
        with open(cpu_info_path()) as f:
            return len([line for line in f if line.startswith("processor")])
    except Exception as e:
        if logger:
            logger.error(f"Failed to retrieve number of cores from /proc/cpuinfo: {e}")
        return None


def _retrieve_containerized_memory_usage(
    logger: Optional[logging.Logger], cgroup_version: Optional[CGroupVersion]
) -> Optional[int]:
    """Retrieve the memory usage in bytes from the cgroup file."""
    if cgroup_version == CGroupVersion.V1:
        return _retrieve_containerized_memory_usage_v1(logger)
    elif cgroup_version == CGroupVersion.V2:
        return _retrieve_containerized_memory_usage_v2(logger)
    else:
        return None


def _retrieve_containerized_memory_usage_v1(logger: Optional[logging.Logger]) -> Optional[int]:
    try:
        with open(memory_usage_path_cgroup_v1()) as f:
            return int(f.read())
    except Exception as e:
        if logger:
            logger.error(f"Failed to retrieve memory usage from cgroup: {e}")
        return None


def _retrieve_containerized_memory_usage_v2(logger: Optional[logging.Logger]) -> Optional[int]:
    try:
        with open(memory_usage_path_cgroup_v2()) as f:
            return int(f.read())
    except Exception as e:
        if logger:
            logger.error(f"Failed to retrieve memory usage from cgroup: {e}")
        return None


def _retrieve_containerized_memory_limit(
    logger: Optional[logging.Logger], cgroup_version: Optional[CGroupVersion]
) -> Optional[int]:
    """Retrieve the memory limit in bytes from the cgroup file."""
    if cgroup_version == CGroupVersion.V1:
        return _retrieve_containerized_memory_limit_v1(logger)
    elif cgroup_version == CGroupVersion.V2:
        return _retrieve_containerized_memory_limit_v2(logger)
    else:
        return None


def _retrieve_containerized_memory_limit_v1(logger: Optional[logging.Logger]) -> Optional[int]:
    try:
        with open(memory_limit_path_cgroup_v1()) as f:
            return int(f.read())
    except:
        if logger:
            logger.exception("Failed to retrieve memory limit from cgroup")
        return None


def _retrieve_containerized_memory_limit_v2(logger: Optional[logging.Logger]) -> Optional[int]:
    try:
        with open(memory_limit_path_cgroup_v2()) as f:
            return int(f.read())
    except:
        if logger:
            logger.exception("Failed to retrieve memory limit from cgroup")
        return None


def _retrieve_containerized_cpu_cfs_period_us(
    logger: Optional[logging.Logger], cgroup_version: Optional[CGroupVersion]
) -> Optional[float]:
    """Retrieve the CPU period in microseconds from the cgroup file."""
    if cgroup_version == CGroupVersion.V1:
        return _retrieve_containerized_cpu_cfs_period_us_v1(logger)
    elif cgroup_version == CGroupVersion.V2:
        return _retrieve_containerized_cpu_cfs_period_us_v2(logger)
    else:
        return None


def _retrieve_containerized_cpu_cfs_period_us_v1(
    logger: Optional[logging.Logger],
) -> Optional[float]:
    try:
        with open(cpu_cfs_period_us_path()) as f:
            return float(f.read())
    except:
        if logger:
            logger.exception("Failed to retrieve CPU period from cgroup")
        return None


def _retrieve_containerized_cpu_cfs_period_us_v2(
    logger: Optional[logging.Logger],
) -> Optional[float]:
    # We can retrieve period information from the cpu.max file. The file is in the format $MAX $PERIOD and is only one line.
    try:
        with open(cpu_max_path_cgroup_v2()) as f:
            line = f.readline()
            return float(line.split()[1])
    except:
        if logger:
            logger.exception("Failed to retrieve CPU period from cgroup")
        return None


def _retrieve_containerized_cpu_cfs_quota_us(
    logger: Optional[logging.Logger], cgroup_version: Optional[CGroupVersion]
) -> Optional[float]:
    """Retrieve the CPU quota in microseconds from the cgroup file."""
    if cgroup_version == CGroupVersion.V1:
        return _retrieve_containerized_cpu_cfs_quota_us_v1(logger)
    elif cgroup_version == CGroupVersion.V2:
        return _retrieve_containerized_cpu_cfs_quota_us_v2(logger)
    else:
        return None


def _retrieve_containerized_cpu_cfs_quota_us_v1(
    logger: Optional[logging.Logger],
) -> Optional[float]:
    try:
        with open(cpu_cfs_quota_us_path()) as f:
            return float(f.read())
    except:
        if logger:
            logger.exception("Failed to retrieve CPU quota from cgroup")
        return None


def _retrieve_containerized_cpu_cfs_quota_us_v2(
    logger: Optional[logging.Logger],
) -> Optional[float]:
    # We can retrieve quota information from the cpu.max file. The file is in the format $MAX $PERIOD .
    try:
        with open(cpu_max_path_cgroup_v2()) as f:
            line = f.readline()
            return float(line.split()[0])
    except:
        if logger:
            logger.exception("Failed to retrieve CPU quota from cgroup")
        return None
