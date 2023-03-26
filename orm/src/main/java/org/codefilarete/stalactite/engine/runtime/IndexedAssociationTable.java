package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public class IndexedAssociationTable<
		SELF extends IndexedAssociationTable<SELF, LEFTTABLE, RIGHTTABLE, LEFTID, RIGHTID>,
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>,
		LEFTID,
		RIGHTID>
		extends AssociationTable<SELF, LEFTTABLE, RIGHTTABLE, LEFTID, RIGHTID> {
	
	private final Column<SELF, Integer> indexColumn;
	
	public IndexedAssociationTable(Schema schema,
								   String name,
								   PrimaryKey<LEFTTABLE, LEFTID> oneSidePrimaryKey,
								   PrimaryKey<RIGHTTABLE, RIGHTID> manySidePrimaryKey,
								   AccessorDefinition accessorDefinition,
								   AssociationTableNamingStrategy namingStrategy,
								   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
								   boolean createManySideForeignKey) {
		super(schema, name, oneSidePrimaryKey, manySidePrimaryKey, accessorDefinition, namingStrategy, foreignKeyNamingStrategy, createManySideForeignKey);
		// index column is part of the primary key for indexed association 
		this.indexColumn = addColumn("idx", int.class).primaryKey();
		getPrimaryKey().addColumn(indexColumn);
	}
	
	public Column<SELF, Integer> getIndexColumn() {
		return indexColumn;
	}
}
