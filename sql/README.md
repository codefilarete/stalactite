# SQL module of Stalactite

This module is a small layer over JDBC to ease some manipulations over it such as :
* [Iterating](/org/gama/stalactite/sql/result/ResultSetIterator.java) over ResultSet and [build graph object from it](src/test/java/org/gama/sql/result/ResultSetRowConverterTest.java)
* Dinstinguish [read](/org/gama/stalactite/sql/dml/ReadOperation.java) and [write](src/main/java/org/gama/stalactite/sql/dml/WriteOperation.java) operation (mixed in Statement)
* [Manage transactions](src/main/java/org/gama/stalactite/sql/TransactionAdapter.java) and [listen to them](src/main/java/org/gama/stalactite/sql/TransactionListener.java)
* Set values through [binders](src/main/java/org/gama/stalactite/sql/binder/binders.md)