# Adaptation layer for PostgreSQL database

- Specifies adhoc PostgreSQL SQL types in [PostgreSQLTypeMapping](src/main/java/org/codefilarete/stalactite/sql/binder/PostgreSQLTypeMapping.java)
- Redefines the way how Blobs are written to database since PostgreSQL has its own way

# Limitations
- When updating Blob column, due to PostgreSQL way of storing Blob objects, one should update Blob content loaded from database instead of creating a brand new one and call update statement with it. Else a new large object entry will be created and previous one will remain on database making it silently grows.
- Note that PostgreSQL doesn't support Blob writing with connection autocommit set to true, hence a Blob should always be written inside a transaction.