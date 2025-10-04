package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.tool.bean.Objects.preventNull;

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
	
	private static final String DEFAULT_INDEX_COLUMN_NAME = "idx";
	
	private final Column<SELF, Integer> indexColumn;
	
	/**
	 * @param schema the database schema
	 * @param name the table name
	 * @param oneSidePrimaryKey primary key of the "one" side table
	 * @param manySidePrimaryKey primary key of the "many" side table
	 * @param accessorDefinition representation of the method that gives the collection to be persisted
	 * @param namingStrategy strategy for naming the association table and its columns
	 * @param foreignKeyNamingStrategy strategy for naming foreign keys
	 * @param createOneSideForeignKey whether to create a foreign key for the one side
	 * @param createManySideForeignKey whether to create a foreign key for the many side
	 * @param columnName name of the index column, defaults to "idx" if null
	 */
	public IndexedAssociationTable(Schema schema,
								   String name,
								   PrimaryKey<LEFTTABLE, LEFTID> oneSidePrimaryKey,
								   PrimaryKey<RIGHTTABLE, RIGHTID> manySidePrimaryKey,
								   AccessorDefinition accessorDefinition,
								   AssociationTableNamingStrategy namingStrategy,
								   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
								   boolean createOneSideForeignKey,
								   boolean createManySideForeignKey,
								   @Nullable String columnName) {
		super(schema, name, oneSidePrimaryKey, manySidePrimaryKey, accessorDefinition, namingStrategy, foreignKeyNamingStrategy, createOneSideForeignKey, createManySideForeignKey);
		// index column is part of the primary key for indexed association 
		this.indexColumn = addColumn(preventNull(columnName, DEFAULT_INDEX_COLUMN_NAME), int.class).primaryKey();
		getPrimaryKey().addColumn(indexColumn);
	}
	
	public IndexedAssociationTable(Schema schema,
								   String name,
								   PrimaryKey<LEFTTABLE, LEFTID> oneSidePrimaryKey,
								   PrimaryKey<RIGHTTABLE, RIGHTID> manySidePrimaryKey,
								   AccessorDefinition accessorDefinition,
								   AssociationTableNamingStrategy namingStrategy,
								   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
								   boolean createOneSideForeignKey,
								   boolean createManySideForeignKey,
								   Column<SELF, Integer> indexColumn) {
		super(schema, name, oneSidePrimaryKey, manySidePrimaryKey, accessorDefinition, namingStrategy, foreignKeyNamingStrategy, createOneSideForeignKey, createManySideForeignKey);
		this.indexColumn = indexColumn == null ? addColumn(DEFAULT_INDEX_COLUMN_NAME, int.class) : indexColumn;
		// index column is part of the primary key for indexed association 
		this.indexColumn.primaryKey();
		getPrimaryKey().addColumn(this.indexColumn);
	}
	
	public Column<SELF, Integer> getIndexColumn() {
		return indexColumn;
	}
}
