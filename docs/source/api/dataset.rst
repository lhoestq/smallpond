.. currentmodule:: smallpond.logical.dataset

Dataset
=======

Dataset represents a collection of files.

To create a dataset:

.. code-block:: python

   dataset = ParquetDataSet("path/to/dataset/*.parquet")

DataSets
--------

.. autosummary::
   :toctree: ../generated

   DataSet
   FileSet
   ParquetDataSet
   CsvDataSet
   JsonDataSet
   ArrowTableDataSet
   PandasDataSet
   PartitionedDataSet
   SqlQueryDataSet
