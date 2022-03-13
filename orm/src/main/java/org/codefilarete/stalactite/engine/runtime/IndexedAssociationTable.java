package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;

/**
 * @author Guillaume Mary
 */
public class IndexedAssociationTable extends AssociationTable<IndexedAssociationTable> {
	
	private final Column<IndexedAssociationTable, Integer> indexColumn;
	
	public IndexedAssociationTable(Schema schema,
								   String name,
								   Column oneSidePrimaryKey,
								   Column manySidePrimaryKey,
								   AccessorDefinition accessorDefinition,
								   AssociationTableNamingStrategy namingStrategy,
								   ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		super(schema, name, oneSidePrimaryKey, manySidePrimaryKey, accessorDefinition, namingStrategy, foreignKeyNamingStrategy);
		this.indexColumn = addColumn("idx", int.class).primaryKey();
		getPrimaryKey().addColumn(indexColumn);
	}
	
	public Column<IndexedAssociationTable, Integer> getIndexColumn() {
		return indexColumn;
	}
}
