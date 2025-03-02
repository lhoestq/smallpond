.. currentmodule:: smallpond.logical.node

.. _nodes:

Nodes
=====

Nodes represent the fundamental building blocks of a data processing pipeline. Each node encapsulates a specific operation or transformation that can be applied to a dataset.
Nodes can be chained together to form a logical plan, which is a directed acyclic graph (DAG) of nodes that represent the overall data processing workflow.

A typical workflow to create a logical plan is as follows:

.. code-block:: python

   # Create a global context
   ctx = Context()

   # Create a dataset
   dataset = ParquetDataSet("path/to/dataset/*.parquet")

   # Create a data source node
   node = DataSourceNode(ctx, dataset)

   # Partition the data
   node = DataSetPartitionNode(ctx, (node,), npartitions=2)

   # Create a SQL engine node to transform the data
   node = SqlEngineNode(ctx, (node,), "SELECT * FROM {0}")

   # Create a logical plan from the root node
   plan = LogicalPlan(ctx, node)

You can then create tasks from the logical plan, see :ref:`tasks`.

Notable properties of Node:

1. Nodes are partitioned. Each Node generates a series of tasks, with each task processing one partition of data.
2. The input and output of a Node are a series of partitioned Datasets. A Node may write data to shared storage and return a new Dataset, or it may simply recombine the input Datasets.

Context
-------

.. autosummary::
   :toctree: ../generated

   Context
   NodeId

LogicalPlan
-----------

.. autosummary::
   :toctree: ../generated

   LogicalPlan
   LogicalPlanVisitor
   .. Planner

Nodes
-----

.. autosummary::
   :toctree: ../generated

   Node
   DataSetPartitionNode
   ArrowBatchNode
   ArrowComputeNode
   ArrowStreamNode
   ConsolidateNode
   DataSinkNode
   DataSourceNode
   EvenlyDistributedPartitionNode
   HashPartitionNode
   LimitNode
   LoadPartitionedDataSetNode
   PandasBatchNode
   PandasComputeNode
   PartitionNode
   ProjectionNode
   PythonScriptNode
   RangePartitionNode
   RepeatPartitionNode
   RootNode
   ShuffleNode
   SqlEngineNode
   UnionNode
   UserDefinedPartitionNode
   UserPartitionedDataSourceNode
