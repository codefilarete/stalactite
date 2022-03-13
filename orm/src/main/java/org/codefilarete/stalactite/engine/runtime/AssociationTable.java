package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nonnull;

import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.tool.Duo;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public class AssociationTable<SELF extends AssociationTable<SELF>> extends Table<SELF> {
	
	/**
	 * Column pointing to left table primary key.
	 * Expected to be joined with {@link #oneSidePrimaryKey}
	 */
	private final Column<SELF, Object> oneSideKeyColumn;
	
	/**
	 * Primary key (one column for now) of the source entities, on the (undefined) target table.
	 * Expected to be joined with {@link #oneSideKeyColumn}
	 */
	private final Column oneSidePrimaryKey;
	
	/**
	 * Column pointing to right table primary key.
	 * Expected to be joined with {@link #manySidePrimaryKey}
	 */
	private final Column<SELF, Object> manySideKeyColumn;
	
	/**
	 * Primary key (one column for now) of the collection entities, on the (undefined) target table
	 * Expected to be joined with {@link #manySideKeyColumn}
	 */
	private final Column manySidePrimaryKey;
	
	/**
	 * Primary key of this table, contains {@link #oneSideKeyColumn} and {@link #manySideKeyColumn}
	 */
	private final PrimaryKey<SELF> primaryKey;
	
	public AssociationTable(Schema schema,
							String name,
							Column oneSidePrimaryKey,
							Column manySidePrimaryKey,
							AccessorDefinition accessorDefinition,
							AssociationTableNamingStrategy namingStrategy,
							ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		super(schema, name);
		this.oneSidePrimaryKey = oneSidePrimaryKey;
		this.manySidePrimaryKey = manySidePrimaryKey;
		Duo<String, String> columnNames = namingStrategy.giveColumnNames(accessorDefinition, oneSidePrimaryKey, manySidePrimaryKey);
		this.oneSideKeyColumn = addColumn(columnNames.getLeft(), oneSidePrimaryKey.getJavaType()).primaryKey();
		this.manySideKeyColumn = addColumn(columnNames.getRight(), manySidePrimaryKey.getJavaType()).primaryKey();
		this.primaryKey = new PrimaryKey<>(oneSideKeyColumn, manySideKeyColumn);
	}
	
	public Column<SELF, Object> getOneSideKeyColumn() {
		return oneSideKeyColumn;
	}
	
	public Column getOneSidePrimaryKey() {
		return oneSidePrimaryKey;
	}
	
	public Column<SELF, Object> getManySideKeyColumn() {
		return manySideKeyColumn;
	}
	
	public Column getManySidePrimaryKey() {
		return manySidePrimaryKey;
	}
	
	@Nonnull
	@Override
	public PrimaryKey<SELF> getPrimaryKey() {
		return this.primaryKey;
	}
}
