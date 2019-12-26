Running the acceptance tests
============================

Tests also require robotstatuschecker:

::

    pip install robotstatuschecker

Tests are ran using Bash script `python atest/run.py`. The script prints help when ran without parameters.

In order to run the tests with IPv6, the ``::1`` must be used as host variable when running ``atest/run.py`` script::

    python atest/run.py --variable=HOST:::1 atest