# Defining persistence mapping

The main class for this is [ClassMappingStrategy](ClassMappingStrategy.java) which allows to map a class to a table.
Some more aside mapping is possible through [ColumnedCollectionMappingStrategy](ColumnedCollectionMappingStrategy.java) or
[ColumnedMapMappingStrategy](ColumnedMapMappingStrategy.java) which can be considered less clean from an Object oriented point of
view, and for this reason it is not recommanded to use them widely, but can be obviously useful.