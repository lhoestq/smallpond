.. _dataframe:

DataFrame
=========

DataFrame is the main class in smallpond. It represents a lazily computed, partitioned data set.

A typical workflow looks like this:

.. code-block:: python

   import smallpond

   sp = smallpond.init()

   df = sp.read_parquet("path/to/dataset/*.parquet")
   df = df.repartition(10)
   df = df.map("x + 1")
   df.write_parquet("path/to/output")

Initialization
--------------

.. autosummary::
   :toctree: ../generated

   smallpond.init

.. currentmodule:: smallpond.dataframe

.. _loading_data:

Loading Data
------------

.. autosummary::
   :toctree: ../generated

   Session.from_items
   Session.from_arrow
   Session.from_pandas
   Session.read_csv
   Session.read_json
   Session.read_parquet

.. _partitioning_data:

Partitioning Data
-----------------

.. autosummary::
   :toctree: ../generated

   DataFrame.repartition

.. _transformations:

Transformations
---------------

Apply transformations and return a new DataFrame.

.. autosummary::
   :toctree: ../generated

   Session.partial_sql
   DataFrame.map
   DataFrame.map_batches
   DataFrame.flat_map
   DataFrame.filter
   DataFrame.limit
   DataFrame.partial_sort
   DataFrame.random_shuffle

.. _consuming_data:

Consuming Data
--------------

These operations will trigger execution of the lazy transformations performed on this DataFrame.

.. autosummary::
   :toctree: ../generated

   DataFrame.count
   DataFrame.take
   DataFrame.take_all
   DataFrame.to_arrow
   DataFrame.to_pandas
   DataFrame.write_parquet
   DataFrame.write_parquet_lazy

Execution
---------

DataFrames are lazily computed. You can use these methods to manually trigger computation.

.. autosummary::
   :toctree: ../generated

   DataFrame.compute
   DataFrame.is_computed
   DataFrame.recompute
   Session.wait
