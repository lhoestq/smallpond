Getting Started
===============

Installation
------------

Python 3.8+ is required.

.. code-block:: bash

   pip install smallpond

Initialization
--------------

The first step is to initialize the smallpond session:

.. code-block:: python

   import smallpond

   sp = smallpond.init()

Loading Data
------------

Create a DataFrame from a set of files:

.. code-block:: python

   df = sp.read_parquet("path/to/dataset/*.parquet")

To learn more about loading data, please refer to :ref:`loading_data`.

Partitioning Data
-----------------

Smallpond requires users to manually specify data partitions for now.

.. code-block:: python

   df = df.repartition(3)                 # repartition by files
   df = df.repartition(3, by_row=True)    # repartition by rows
   df = df.repartition(3, hash_by="host") # repartition by hash of column

To learn more about partitioning data, please refer to :ref:`partitioning_data`.

Transforming Data
-----------------

Apply python functions or SQL expressions to transform data.

.. code-block:: python

   df = df.map('a + b as c')
   df = df.map(lambda row: {'c': row['a'] + row['b']})

To learn more about transforming data, please refer to :ref:`transformations`.

Saving Data
-----------

Save the transformed data to a set of files:

.. code-block:: python

   df.write_parquet("path/to/output")

To learn more about saving data, please refer to :ref:`consuming_data`.

Monitoring
----------

Smallpond uses `Ray Core`_ as the task scheduler. You can use `Ray Dashboard`_ to monitor the task execution.

.. _Ray Core: https://docs.ray.io/en/latest/ray-core/walkthrough.html
.. _Ray Dashboard: https://docs.ray.io/en/latest/ray-observability/getting-started.html

When smallpond starts, it will print the Ray Dashboard URL:

.. code-block:: bash

   ... Started a local Ray instance. View the dashboard at http://127.0.0.1:8008
