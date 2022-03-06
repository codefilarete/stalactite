## Binders overview

Binders aim at giving access to methods of `ResultSet` and `PreparedStatement` classes in a functional programming way, these accesses are given respectively through functional interfaces:
- [ResultSet read](ResultSetReader.java)
- [PreparedStatement write](PreparedStatementWriter.java)

A read and write wrapper is made through [ParameterBinder](ParameterBinder.java).

## Predefined binders

Standards and well-known accessors are defined as constants respectively in:
 - [DefaultResultSetReaders](DefaultResultSetReaders.java)
 - [DefaultPreparedStatementWriters](DefaultPreparedStatementWriters.java)
 - [DefaultParameterBinders](DefaultParameterBinders.java)

## FAQ

###### Why using a column name signature in ResultSetReader#get(..), not an index ?

This is done to enforce good practice that allows column order change and gives more readability. 

###### Why using an index signature in PreparedStatementWriter#set(..), not a parameter name ?

This is done to enforce good practice that avoid SQL injection in PreparedStatement.
Upper layers use named parameters to give readability but convert them into PreparedStatement placeholder : `?`

