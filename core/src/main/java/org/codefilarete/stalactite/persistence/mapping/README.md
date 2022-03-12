# Package focused on entity mapping

The main class for this is [ClassMappingStrategy](ClassMappingStrategy.java) which allows to map a class to a table.

Some dedicated mapping can be created for specific type such as [ZoneDateTime](ZonedDateTimeMappingStrategy.java) and even more aside mapping is possible such as [ColumnedCollectionMappingStrategy](ColumnedCollectionMappingStrategy.java) or [ColumnedMapMappingStrategy](ColumnedMapMappingStrategy.java).