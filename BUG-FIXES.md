# 07-16-2025
```bash
Harvy Jones@LAPTOP-15MRQFI9 MINGW64 /c/xampp/htdocs/data-engineering/real-time-graph-based-risk-engine/real-time-graph-based-risk-engine (main)
$ docker compose logs -f ingestion
ingestion  | 2025-07-15 21:35:30,581 - __main__ - INFO - Starting data extraction from /data/raw/creditcard.csv
ingestion  | 2025-07-15 21:35:30,768 - app.extractor - INFO - Input file '/data/raw/creditcard.csv' size: 143.84 MB
ingestion  | 2025-07-15 21:36:32,423 - app.extractor - INFO - Data extracted successfully from /data/raw/creditcard.csv
ingestion  | /usr/local/lib/python3.12/site-packages/pandera/_pandas_deprecated.py:146: FutureWarning: Importing pandas-specific classes and functions from the
ingestion  | top-level pandera module will be **removed in a future version of pandera**.
ingestion  | If you're using pandera to validate pandas objects, we highly recommend updating
ingestion  | your import:
ingestion  | 
ingestion  | ```
ingestion  | # old import
ingestion  | import pandera as pa
ingestion  | 
ingestion  | # new import
ingestion  | import pandera.pandas as pa
ingestion  | ```
ingestion  |
ingestion  | If you're using pandera to validate objects from other compatible libraries
ingestion  | like pyspark or polars, see the supported libraries section of the documentation
ingestion  | for more information on how to import pandera:
ingestion  |
ingestion  | https://pandera.readthedocs.io/en/stable/supported_libraries.html
ingestion  |
ingestion  | To disable this warning, set the environment variable:
ingestion  |
ingestion  | ```
ingestion  | export DISABLE_PANDERA_IMPORT_WARNING=True
ingestion  | ```
ingestion  |
ingestion  |   warnings.warn(_future_warning, FutureWarning)
ingestion  | 2025-07-15 21:36:54,314 - app.processor - INFO - Starting data cleaning...
ingestion  | 2025-07-15 21:37:00,919 - app.processor - INFO - Dropped 0 rows with missing values.
ingestion  | 2025-07-15 21:37:18,335 - app.processor - INFO - Dropped 1081 duplicates rows.
ingestion  | 2025-07-15 21:37:18,339 - app.processor - INFO - Data cleaning completed.
ingestion  | 2025-07-15 21:37:18,399 - app.processor - INFO - Starting feature engineering...
ingestion  | 2025-07-15 21:37:48,172 - app.processor - INFO - Feature engineering completed.
ingestion  | 2025-07-15 21:37:48,195 - app.db - INFO - Creating SQLAlchemy Engine for URL: localhost:6543/development
ingestion  | 2025-07-15 21:37:50,150 - app.db - CRITICAL - Failed to create or connect to database engine: (psycopg2.OperationalError) connection to server at "localhost" (::1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  | connection to server at "localhost" (127.0.0.1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  |
ingestion  | (Background on this error at: https://sqlalche.me/e/20/e3q8)
ingestion  | Traceback (most recent call last):
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 145, in __init__
ingestion  |     self._dbapi_connection = engine.raw_connection()
ingestion  |                              ^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 3297, in raw_connection
ingestion  |     return self.pool.connect()
ingestion  |            ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 449, in connect
ingestion  |     return _ConnectionFairy._checkout(self)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 1264, in _checkout
ingestion  |     fairy = _ConnectionRecord.checkout(pool)
ingestion  |             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 713, in checkout
ingestion  |     rec = pool._do_get()
ingestion  |           ^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/impl.py", line 179, in _do_get
ingestion  |     with util.safe_reraise():
ingestion  |          ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/util/langhelpers.py", line 224, in __exit__
ingestion  |     raise exc_value.with_traceback(exc_tb)
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/impl.py", line 177, in _do_get
ingestion  |     return self._create_connection()
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 390, in _create_connection
ingestion  |     return _ConnectionRecord(self)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 675, in __init__
ingestion  |     self.__connect()
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 901, in __connect
ingestion  |     with util.safe_reraise():
ingestion  |          ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/util/langhelpers.py", line 224, in __exit__
ingestion  |     raise exc_value.with_traceback(exc_tb)
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 897, in __connect
ingestion  |     self.dbapi_connection = connection = pool._invoke_creator(self)
ingestion  |                                          ^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/create.py", line 646, in connect
ingestion  |     return dialect.connect(*cargs, **cparams)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 625, in connect
ingestion  |     return self.loaded_dbapi.connect(*cargs, **cparams)  # type: ignore[no-any-return]  # NOQA: E501
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/psycopg2/__init__.py", line 122, in connect
ingestion  |     conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  | psycopg2.OperationalError: connection to server at "localhost" (::1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  | connection to server at "localhost" (127.0.0.1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  |
ingestion  |
ingestion  | The above exception was the direct cause of the following exception:
ingestion  |
ingestion  | Traceback (most recent call last):
ingestion  |   File "/app/app/db.py", line 30, in get_engine
ingestion  |     with _engine.connect() as connection:
ingestion  |          ^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 3273, in connect
ingestion  |     return self._connection_cls(self)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 147, in __init__
ingestion  |     Connection._handle_dbapi_exception_noconnection(
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2436, in _handle_dbapi_exception_noconnection
ingestion  |     raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 145, in __init__
ingestion  |     self._dbapi_connection = engine.raw_connection()
ingestion  |                              ^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 3297, in raw_connection
ingestion  |     return self.pool.connect()
ingestion  |            ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 449, in connect
ingestion  |     return _ConnectionFairy._checkout(self)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 1264, in _checkout
ingestion  |     fairy = _ConnectionRecord.checkout(pool)
ingestion  |             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 713, in checkout
ingestion  |     rec = pool._do_get()
ingestion  |           ^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/impl.py", line 179, in _do_get
ingestion  |     with util.safe_reraise():
ingestion  |          ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/util/langhelpers.py", line 224, in __exit__
ingestion  |     raise exc_value.with_traceback(exc_tb)
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/impl.py", line 177, in _do_get
ingestion  |     return self._create_connection()
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 390, in _create_connection
ingestion  |     return _ConnectionRecord(self)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 675, in __init__
ingestion  |     self.__connect()
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 901, in __connect
ingestion  |     with util.safe_reraise():
ingestion  |          ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/util/langhelpers.py", line 224, in __exit__
ingestion  |     raise exc_value.with_traceback(exc_tb)
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 897, in __connect
ingestion  |     self.dbapi_connection = connection = pool._invoke_creator(self)
ingestion  |                                          ^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/create.py", line 646, in connect
ingestion  |     return dialect.connect(*cargs, **cparams)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 625, in connect
ingestion  |     return self.loaded_dbapi.connect(*cargs, **cparams)  # type: ignore[no-any-return]  # NOQA: E501
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/psycopg2/__init__.py", line 122, in connect
ingestion  |     conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  | sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) connection to server at "localhost" (::1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  | connection to server at "localhost" (127.0.0.1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  |
ingestion  | (Background on this error at: https://sqlalche.me/e/20/e3q8)
ingestion  | 2025-07-15 21:37:50,289 - __main__ - ERROR - Data ingestion failed: (psycopg2.OperationalError) connection to server at "localhost" (::1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  | connection to server at "localhost" (127.0.0.1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  |
ingestion  | (Background on this error at: https://sqlalche.me/e/20/e3q8)
ingestion  | Traceback (most recent call last):
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 145, in __init__
ingestion  |     self._dbapi_connection = engine.raw_connection()
ingestion  |                              ^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 3297, in raw_connection
ingestion  |     return self.pool.connect()
ingestion  |            ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 449, in connect
ingestion  |     return _ConnectionFairy._checkout(self)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 1264, in _checkout
ingestion  |     fairy = _ConnectionRecord.checkout(pool)
ingestion  |             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 713, in checkout
ingestion  |     rec = pool._do_get()
ingestion  |           ^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/impl.py", line 179, in _do_get
ingestion  |     with util.safe_reraise():
ingestion  |          ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/util/langhelpers.py", line 224, in __exit__
ingestion  |     raise exc_value.with_traceback(exc_tb)
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/impl.py", line 177, in _do_get
ingestion  |     return self._create_connection()
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 390, in _create_connection
ingestion  |     return _ConnectionRecord(self)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 675, in __init__
ingestion  |     self.__connect()
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 901, in __connect
ingestion  |     with util.safe_reraise():
ingestion  |          ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/util/langhelpers.py", line 224, in __exit__
ingestion  |     raise exc_value.with_traceback(exc_tb)
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 897, in __connect
ingestion  |     self.dbapi_connection = connection = pool._invoke_creator(self)
ingestion  |                                          ^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/create.py", line 646, in connect
ingestion  |     return dialect.connect(*cargs, **cparams)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 625, in connect
ingestion  |     return self.loaded_dbapi.connect(*cargs, **cparams)  # type: ignore[no-any-return]  # NOQA: E501
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/psycopg2/__init__.py", line 122, in connect
ingestion  |     conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  | psycopg2.OperationalError: connection to server at "localhost" (::1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  | connection to server at "localhost" (127.0.0.1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  |
ingestion  |
ingestion  | The above exception was the direct cause of the following exception:
ingestion  |
ingestion  | Traceback (most recent call last):
ingestion  |   File "<frozen runpy>", line 198, in _run_module_as_main
ingestion  |   File "<frozen runpy>", line 88, in _run_code
ingestion  |   File "/app/app/main.py", line 110, in <module>
ingestion  |     ingest.kaggle()
ingestion  |   File "/app/app/main.py", line 48, in kaggle
ingestion  |     raise e
ingestion  |   File "/app/app/main.py", line 39, in kaggle
ingestion  |     save_dataframe_to_db(df, 'credit_card_fraud', if_exists='replace', chunksize=settings.DB_SAVE_CHUNKSIZE if hasattr(settings, 'DB_SAVE_CHUNKSIZE') else 10000)      
ingestion  |   File "/app/app/db.py", line 48, in save_dataframe_to_db
ingestion  |     engine = get_engine()
ingestion  |              ^^^^^^^^^^^^
ingestion  |   File "/app/app/db.py", line 30, in get_engine
ingestion  |     with _engine.connect() as connection:
ingestion  |          ^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 3273, in connect
ingestion  |     return self._connection_cls(self)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 147, in __init__
ingestion  |     Connection._handle_dbapi_exception_noconnection(
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2436, in _handle_dbapi_exception_noconnection
ingestion  |     raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 145, in __init__
ingestion  |     self._dbapi_connection = engine.raw_connection()
ingestion  |                              ^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 3297, in raw_connection
ingestion  |     return self.pool.connect()
ingestion  |            ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 449, in connect
ingestion  |     return _ConnectionFairy._checkout(self)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 1264, in _checkout
ingestion  |     fairy = _ConnectionRecord.checkout(pool)
ingestion  |             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 713, in checkout
ingestion  |     rec = pool._do_get()
ingestion  |           ^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/impl.py", line 179, in _do_get
ingestion  |     with util.safe_reraise():
ingestion  |          ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/util/langhelpers.py", line 224, in __exit__
ingestion  |     raise exc_value.with_traceback(exc_tb)
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/impl.py", line 177, in _do_get
ingestion  |     return self._create_connection()
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 390, in _create_connection
ingestion  |     return _ConnectionRecord(self)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 675, in __init__
ingestion  |     self.__connect()
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 901, in __connect
ingestion  |     with util.safe_reraise():
ingestion  |          ^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/util/langhelpers.py", line 224, in __exit__
ingestion  |     raise exc_value.with_traceback(exc_tb)
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/pool/base.py", line 897, in __connect
ingestion  |     self.dbapi_connection = connection = pool._invoke_creator(self)
ingestion  |                                          ^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/create.py", line 646, in connect
ingestion  |     return dialect.connect(*cargs, **cparams)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 625, in connect
ingestion  |     return self.loaded_dbapi.connect(*cargs, **cparams)  # type: ignore[no-any-return]  # NOQA: E501
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  |   File "/usr/local/lib/python3.12/site-packages/psycopg2/__init__.py", line 122, in connect
ingestion  |     conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
ingestion  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ingestion  | sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) connection to server at "localhost" (::1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  | connection to server at "localhost" (127.0.0.1), port 6543 failed: Connection refused
ingestion  |    Is the server running on that host and accepting TCP/IP connections?
ingestion  |
ingestion  | (Background on this error at: https://sqlalche.me/e/20/e3q8)
ingestion exited with code 1

Harvy Jones@LAPTOP-15MRQFI9 MINGW64 /c/xampp/htdocs/data-engineering/real-time-graph-based-risk-engine/real-time-graph-based-risk-engine (main)
$
```