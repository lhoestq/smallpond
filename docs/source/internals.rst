Internals
=========

Data Root
---------

Smallpond stores all data in a single directory called data root.

This directory has the following structure:

.. code-block:: bash

    data_root
    └── 2024-12-11-12-00-28.2cc39990-296f-48a3-8063-78cf6dca460b # job_time.job_id
        ├── config  # configuration and state
        │   ├── exec_plan.pickle
        │   ├── logical_plan.pickle
        │   └── runtime_ctx.pickle
        ├── log     # logs
        │   ├── graph.png
        │   └── scheduler.log
        ├── queue   # message queue between scheduler and workers
        ├── output  # output data
        ├── staging # intermediate data
        │   ├── DataSourceTask.000001
        │   ├── EvenlyDistributedPartitionProducerTask.000002
        │   ├── completed_tasks  # output dataset of completed tasks
        │   └── started_tasks    # used for checkpoint
        └── temp    # temporary data
            ├── DataSourceTask.000001
            └── EvenlyDistributedPartitionProducerTask.000002

Failure Recovery
----------------

Smallpond can recover from failure and resume execution from the last checkpoint.
Checkpoint is task-level. A few tasks, such as `ArrowBatchTask`, support checkpointing at the batch level.
