Setup
=====

Make sure you have SQL Server running with trusted connection.

Download the database backup from  `Microsoft`_ and restore it.

Create a new user with access to the database and following values:

:Username: user
:Password: pass

Ensure you have the relevant drivers installed in your system, this has been tested with:

* SQL Server Native Client 11.0
* ODBC Driver 17 for SQL Server

Running the acceptance tests
============================

Tests also require robotstatuschecker:

::

    pip install robotstatuschecker

Tests are ran via the python script `python atest/run.py`. The script prints help when ran without parameters.

::

    python atest/run.py atest

.. _Microsoft: https://github.com/microsoft/sql-server-samples/releases/tag/adventureworks