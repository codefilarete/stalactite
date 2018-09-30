package org.gama.stalactite.persistence.engine;

import javax.annotation.Nonnull;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Database.Schema;
import org.gama.stalactite.persistence.structure.PrimaryKey;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class AssociationTable<SELF extends AssociationTable<SELF>> extends Table<SELF> {
	
	private final Column<SELF, Object> oneSideKeyColumn;
	private final Column oneSidePrimaryKey;
	private final Column<SELF, Object> manySideKeyColumn;
	private final Column manySidePrimaryKey;
	private final PrimaryKey<SELF> primaryKey;
	
	public AssociationTable(Schema schema,
							Column oneSidePrimaryKey,
							Column manySidePrimaryKey,
							AssociationTableNamingStrategy namingStrategy,
							ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		super(schema, namingStrategy.giveName(oneSidePrimaryKey, manySidePrimaryKey));
		this.oneSidePrimaryKey = oneSidePrimaryKey;
		this.manySidePrimaryKey = manySidePrimaryKey;
		this.oneSideKeyColumn = addColumn(namingStrategy.giveOneSideColumnName(oneSidePrimaryKey), oneSidePrimaryKey.getJavaType()).primaryKey();
		this.manySideKeyColumn = addColumn(namingStrategy.giveManySideColumnName(manySidePrimaryKey), manySidePrimaryKey.getJavaType()).primaryKey();
		addForeignKey(foreignKeyNamingStrategy.giveName(oneSideKeyColumn, oneSidePrimaryKey), oneSideKeyColumn, oneSidePrimaryKey);
		addForeignKey(foreignKeyNamingStrategy.giveName(manySideKeyColumn, manySidePrimaryKey), manySideKeyColumn, manySidePrimaryKey);
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
