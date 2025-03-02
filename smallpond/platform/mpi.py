import shutil
import subprocess
from typing import List
from loguru import logger

from smallpond.platform.base import Platform


class MPI(Platform):
    """
    MPI platform.
    """

    @staticmethod
    def is_available() -> bool:
        return shutil.which("mpirun") is not None

    def start_job(
        self,
        num_nodes: int,
        entrypoint: str,
        args: List[str],
        envs: dict = {},
        extra_opts: dict = {},
    ) -> List[str]:
        mpirun_cmd = ["mpirun", "-n", str(num_nodes)]
        for key, value in envs.items():
            mpirun_cmd += ["-x", f"{key}={value}"]
        mpirun_cmd += ["python", entrypoint] + args

        logger.debug(f"start job with command: {' '.join(mpirun_cmd)}")
        subprocess.Popen(
            mpirun_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            text=True,
        )

        return []
