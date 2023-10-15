package org.codefilarete.stalactite.engine.configurer;

import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.TableNamingStrategy;

/**
 * Storage for all naming strategy.
 * Made to avoid having all those attributes in every class that requires them.
 * 
 * @author Guillaume Mary
 */
public class NamingConfiguration {
	
	private TableNamingStrategy tableNamingStrategy;
	private ColumnNamingStrategy columnNamingStrategy;
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy;
	private JoinColumnNamingStrategy joinColumnNamingStrategy;
	private ColumnNamingStrategy indexColumnNamingStrategy;
	private AssociationTableNamingStrategy associationTableNamingStrategy;
	
	public NamingConfiguration(TableNamingStrategy tableNamingStrategy,
							   ColumnNamingStrategy columnNamingStrategy,
							   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
							   ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
							   JoinColumnNamingStrategy joinColumnNamingStrategy,
							   ColumnNamingStrategy indexColumnNamingStrategy,
							   AssociationTableNamingStrategy associationTableNamingStrategy) {
		this.tableNamingStrategy = tableNamingStrategy;
		this.columnNamingStrategy = columnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.elementCollectionTableNamingStrategy = elementCollectionTableNamingStrategy;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
		this.associationTableNamingStrategy = associationTableNamingStrategy;
	}
	
	public TableNamingStrategy getTableNamingStrategy() {
		return tableNamingStrategy;
	}
	
	public void setTableNamingStrategy(TableNamingStrategy tableNamingStrategy) {
		this.tableNamingStrategy = tableNamingStrategy;
	}
	
	public ColumnNamingStrategy getColumnNamingStrategy() {
		return columnNamingStrategy;
	}
	
	public void setColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
		this.columnNamingStrategy = columnNamingStrategy;
	}
	
	public ForeignKeyNamingStrategy getForeignKeyNamingStrategy() {
		return foreignKeyNamingStrategy;
	}
	
	public void setForeignKeyNamingStrategy(ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
	}
	
	public ElementCollectionTableNamingStrategy getElementCollectionTableNamingStrategy() {
		return elementCollectionTableNamingStrategy;
	}
	
	public void setElementCollectionTableNamingStrategy(ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy) {
		this.elementCollectionTableNamingStrategy = elementCollectionTableNamingStrategy;
	}
	
	public JoinColumnNamingStrategy getJoinColumnNamingStrategy() {
		return joinColumnNamingStrategy;
	}
	
	public void setJoinColumnNamingStrategy(JoinColumnNamingStrategy joinColumnNamingStrategy) {
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
	}
	
	public ColumnNamingStrategy getIndexColumnNamingStrategy() {
		return indexColumnNamingStrategy;
	}
	
	public void setIndexColumnNamingStrategy(ColumnNamingStrategy indexColumnNamingStrategy) {
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
	}
	
	public AssociationTableNamingStrategy getAssociationTableNamingStrategy() {
		return associationTableNamingStrategy;
	}
	
	public void setAssociationTableNamingStrategy(AssociationTableNamingStrategy associationTableNamingStrategy) {
		this.associationTableNamingStrategy = associationTableNamingStrategy;
	}
}
