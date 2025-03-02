from importlib.metadata import PackageNotFoundError, version
from typing import Optional

try:
    __version__ = version("smallpond")
except PackageNotFoundError:
    # package is not installed
    __version__ = "unknown"


def init(
    job_id: Optional[str] = None,
    job_time: Optional[float] = None,
    job_name: Optional[str] = None,
    data_root: Optional[str] = None,
    num_executors: Optional[int] = None,
    ray_address: Optional[str] = None,
    bind_numa_node: Optional[bool] = None,
    platform: Optional[str] = None,
    _remove_output_root: bool = True,
    **kwargs,
) -> "Session":
    """
    Initialize smallpond environment.

    This is the entry point for smallpond::

        import smallpond
        sp = smallpond.init()

    By default, it will use a local ray cluster as worker node.
    To use more worker nodes, please specify the argument::

        sp = smallpond.init(num_executors=10)

    It will create an task to run the workers.

    Parameters
    ----------
    All parameters are optional. If not specified, read from environment variables. If not set, use default values.

    job_id (SP_JOBID)
        Unique job id. Default to a random uuid.
    job_time (SP_JOB_TIME)
        Job create time (seconds since epoch). Default to current time.
    job_name (SP_JOB_NAME)
        Job display name. Default to the filename of the current script.
    data_root (SP_DATA_ROOT)
        The root folder for all files generated at runtime.
    num_executors (SP_NUM_EXECUTORS)
        The number of executors.
        Default to 0, which means all tasks will be run on the current node.
    ray_address (SP_RAY_ADDRESS)
        If specified, use the given address to connect to an existing ray cluster.
        Otherwise, create a new ray cluster.
    bind_numa_node (SP_BIND_NUMA_NODE)
        If true, bind executor processes to numa nodes.
    memory_allocator (SP_MEMORY_ALLOCATOR)
        The memory allocator used by worker processes.
        Choices: "system", "jemalloc", "mimalloc". Default to "mimalloc".
    platform (SP_PLATFORM)
        The platform to use. Choices: "mpi".
        By default, it will automatically detect the environment and choose the most suitable platform.
    _remove_output_root
        If true, remove the "{data_root}/output" directory after the job is finished.
        Default to True. This is only used for compatibility. User should use `write_parquet` for saving outputs.

    Spawning a new job
    ------------------
    If the environment variable `SP_SPAWN` is set to "1", it will spawn a new job to run the current script.
    """
    import atexit

    from smallpond.dataframe import Session

    session = Session(
        job_id=job_id,
        job_time=job_time,
        job_name=job_name,
        data_root=data_root,
        num_executors=num_executors,
        ray_address=ray_address,
        bind_numa_node=bind_numa_node,
        platform=platform,
        _remove_output_root=_remove_output_root,
        **kwargs,
    )
    atexit.register(session.shutdown)
    return session
