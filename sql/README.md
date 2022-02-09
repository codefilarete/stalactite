# SQL module of Stalactite

This module is a small layer over JDBC to ease some manipulations over it such as :
* [Iterating](/org/codefilarete/stalactite/sql/result/ResultSetIterator.java) over ResultSet and [build graph object from it](src/test/java/org/codefilarete/sql/result/ResultSetRowConverterTest.java)
* Dinstinguish [read](/org/codefilarete/stalactite/sql/dml/ReadOperation.java) and [write](src/main/java/org/codefilarete/stalactite/sql/dml/WriteOperation.java) operation (mixed in Statement)
* [Manage transactions](src/main/java/org/codefilarete/stalactite/sql/TransactionAdapter.java) and [listen to them](src/main/java/org/codefilarete/stalactite/sql/TransactionListener.java)
* Set values through [binders](src/main/java/org/codefilarete/stalactite/sql/binder/binders.md)