.. currentmodule:: smallpond.execution.task

.. _tasks:

Tasks
=====

.. code-block:: python

   # create a runtime context
   runtime_ctx = RuntimeContext(JobId.new(), data_root)
   runtime_ctx.initialize(socket.gethostname(), cleanup_root=True)

   # create a logical plan
   plan = create_logical_plan()

   # create an execution plan
   planner = Planner(runtime_ctx)
   exec_plan = planner.create_exec_plan(plan)

You can then execute the tasks in a scheduler, see :ref:`execution`.

RuntimeContext
--------------

.. autosummary::
   :toctree: ../generated

   RuntimeContext
   JobId
   TaskId
   TaskRuntimeId
   PartitionInfo
   PerfStats


ExecutionPlan
-------------

.. autosummary::
   :toctree: ../generated

   ExecutionPlan


Tasks
-----

.. autosummary::
   :toctree: ../generated

   Task
   ArrowBatchTask
   ArrowComputeTask
   ArrowStreamTask
   DataSinkTask
   DataSourceTask
   EvenlyDistributedPartitionProducerTask
   HashPartitionArrowTask
   HashPartitionDuckDbTask
   HashPartitionTask
   LoadPartitionedDataSetProducerTask
   MergeDataSetsTask
   PandasBatchTask
   PandasComputeTask
   PartitionConsumerTask
   PartitionProducerTask
   ProjectionTask
   PythonScriptTask
   RangePartitionTask
   RepeatPartitionProducerTask
   RootTask
   SplitDataSetTask
   SqlEngineTask
   UserDefinedPartitionProducerTask

