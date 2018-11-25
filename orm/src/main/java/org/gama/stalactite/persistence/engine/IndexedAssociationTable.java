package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Database.Schema;

/**
 * @author Guillaume Mary
 */
public class IndexedAssociationTable extends AssociationTable<IndexedAssociationTable> {
	
	private final Column<IndexedAssociationTable, Integer> indexColumn;
	
	public IndexedAssociationTable(Schema schema,
								   String name,
								   Column oneSidePrimaryKey,
								   Column manySidePrimaryKey,
								   AssociationTableNamingStrategy namingStrategy,
								   ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		super(schema, name, oneSidePrimaryKey, manySidePrimaryKey, namingStrategy, foreignKeyNamingStrategy);
		this.indexColumn = addColumn("idx", int.class).primaryKey();
		getPrimaryKey().addColumn(indexColumn);
	}
	
	public Column<IndexedAssociationTable, Integer> getIndexColumn() {
		return indexColumn;
	}
}
