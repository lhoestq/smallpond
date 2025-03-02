import os
import signal
import subprocess
import uuid
from datetime import datetime
from typing import List, Optional


class Platform:
    """
    Base class for all platforms.
    """

    @staticmethod
    def is_available() -> bool:
        """
        Whether the platform is available in the current environment.
        """
        return False

    @classmethod
    def __str__(cls) -> str:
        return cls.__name__

    def start_job(
        self,
        num_nodes: int,
        entrypoint: str,
        args: List[str],
        envs: dict = {},
        extra_opts: dict = {},
    ) -> List[str]:
        """
        Start a job on the platform.
        Return the job ids.
        """
        pids = []
        for _ in range(num_nodes):
            popen = subprocess.Popen(
                ["python", entrypoint, *args],
                env={**os.environ, **envs},
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )
            pids.append(str(popen.pid))
        return pids

    def stop_job(self, pid: str) -> None:
        """
        Stop the job.
        """
        os.kill(int(pid), signal.SIGKILL)

    @staticmethod
    def default_job_id() -> str:
        """
        Return the default job id.
        """
        return str(uuid.uuid4())

    @staticmethod
    def default_job_time() -> datetime:
        """
        Return the default job time.
        """
        return datetime.now()

    @staticmethod
    def default_data_root() -> Optional[str]:
        """
        Get the default data root for the platform.
        If the platform does not have a default data root, return None.
        """
        from loguru import logger

        default = os.path.expanduser("~/.smallpond/data")
        logger.warning(f"data root is not set, using default: {default}")
        return default

    @staticmethod
    def default_share_log_analytics() -> bool:
        """
        Whether to share log analytics by default.
        """
        return False

    @staticmethod
    def shared_log_root() -> Optional[str]:
        """
        Return the shared log root.
        """
        return None

    @staticmethod
    def grafana_homepath() -> Optional[str]:
        """
        Return the homepath of grafana.
        """
        homebrew_installed_homepath = "/opt/homebrew/opt/grafana/share/grafana"
        if os.path.exists(homebrew_installed_homepath):
            return homebrew_installed_homepath
        return None

    @staticmethod
    def default_memory_allocator() -> str:
        """
        Get the default memory allocator for the platform.
        """
        return "system"
