.. currentmodule:: smallpond.execution

.. _execution:

Execution
=========

Submit a Job
------------

After constructing the LogicalPlan, you can use the JobManager to create a Job in the cluster to execute it. However, in most cases, you only need to use the Driver as the entry point of the entire script and then submit the plan. The Driver is a simple wrapper around the JobManager. It reads the configuration from the command line arguments and passes it to the JobManager.

.. code-block:: python

   from smallpond.execution.driver import Driver

   if __name__ == "__main__":
      driver = Driver()
      # add your own arguments
      driver.add_argument("-i", "--input_paths", nargs="+")
      driver.add_argument("-n", "--npartitions", type=int, default=10)
      # build and run logical plan
      plan = my_pipeline(**driver.get_arguments())
      driver.run(plan)


.. autosummary::
   :toctree: ../generated

   ~driver.Driver
   ~manager.JobManager

Scheduler and Executor
----------------------

Scheduler and Executor are lower-level APIs. They are directly responsible for scheduling and executing tasks, respectively. Generally, users do not need to use them directly.

.. autosummary::
   :toctree: ../generated

   ~scheduler.Scheduler
   ~executor.Executor

.. _platform:

Customize Platform
------------------

Smallpond supports user-defined task execution platforms. A Platform includes methods for submitting jobs and a series of default configurations. By default, smallpond automatically detects the current environment and selects the most suitable platform. If it cannot detect one, it uses the default platform.

You can specify a built-in platform via parameters:

.. code-block:: bash

   # run with your platform
   python script.py --platform mpi


Or implement your own Platform class:

.. code-block:: python

   # path/to/my/platform.py
   from smallpond.platform import Platform

   class MyPlatform(Platform):
      def start_job(self, ...) -> List[str]:
         ...

.. code-block:: bash

   # run with your platform
   # if using Driver
   python script.py --platform path.to.my.platform

   # if using smallpond.init
   SP_PLATFORM=path.to.my.platform python script.py

.. currentmodule:: smallpond

.. autosummary::
   :toctree: ../generated

   ~platform.Platform
   ~platform.MPI
