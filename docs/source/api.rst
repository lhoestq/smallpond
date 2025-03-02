API Reference
=============

Smallpond provides both high-level and low-level APIs.

.. note::
   Currently, smallpond provides two different APIs, supporting dynamic and static construction of data flow graphs respectively. Due to historical reasons, these two APIs use different scheduler backends and support different configuration options.

   - The High-level API currently uses Ray as the backend, supporting dynamic construction and execution of data flow graphs.
   - The Low-level API uses a built-in scheduler and only supports one-time execution of static data flow graphs. However, it offers more performance optimizations and richer configuration options.

   We are working to merge them so that in the future, you can use a unified high-level API and freely choose between Ray or the built-in scheduler.

High-level API
--------------

The high-level API is centered around :ref:`dataframe`. It allows dynamic construction of data flow graphs, execution, and result retrieval.

A typical workflow looks like this:

.. code-block:: python

   import smallpond

   sp = smallpond.init()

   df = sp.read_parquet("path/to/dataset/*.parquet")
   df = df.repartition(10)
   df = df.map("x + 1")
   df.write_parquet("path/to/output")

.. toctree::
   :maxdepth: 2

   api/dataframe

It is recommended to use the DataFrame API.

Low-level API
-------------

In the low-level API, users manually create :ref:`nodes` to construct static data flow graphs, then submit them to smallpond to generate :ref:`tasks` and wait for all tasks to complete.

A complete example is shown below.

.. code-block:: python

   from smallpond.logical.dataset import ParquetDataSet
   from smallpond.logical.node import Context, DataSourceNode, DataSetPartitionNode, SqlEngineNode, LogicalPlan
   from smallpond.execution.driver import Driver

   def my_pipeline(input_paths: List[str], npartitions: int):
      ctx = Context()
      dataset = ParquetDataSet(input_paths)
      node = DataSourceNode(ctx, dataset)
      node = DataSetPartitionNode(ctx, (node,), npartitions=npartitions)
      node = SqlEngineNode(ctx, (node,), "SELECT * FROM {0}")
      return LogicalPlan(ctx, node)

   if __name__ == "__main__":
      driver = Driver()
      driver.add_argument("-i", "--input_paths", nargs="+")
      driver.add_argument("-n", "--npartitions", type=int, default=10)

      plan = my_pipeline(**driver.get_arguments())
      driver.run(plan)

To run this script:

.. code-block:: bash

   python script.py -i "path/to/*.parquet" -n 10


.. toctree::
   :maxdepth: 2

   api/dataset
   api/nodes
   api/tasks
   api/execution
