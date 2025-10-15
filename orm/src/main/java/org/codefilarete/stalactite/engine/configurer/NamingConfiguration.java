package org.codefilarete.stalactite.engine.configurer;

import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.IndexNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;

/**
 * Storage for all naming strategy.
 * Made to avoid having all those attributes in every class that requires them.
 * 
 * @author Guillaume Mary
 */
public class NamingConfiguration {
	
	private final TableNamingStrategy tableNamingStrategy;
	private final ColumnNamingStrategy columnNamingStrategy;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final IndexNamingStrategy indexNamingStrategy;
	private final ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy;
	private final MapEntryTableNamingStrategy mapEntryTableNamingStrategy;
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	private final AssociationTableNamingStrategy associationTableNamingStrategy;
	
	public NamingConfiguration(TableNamingStrategy tableNamingStrategy,
							   ColumnNamingStrategy columnNamingStrategy,
							   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
							   IndexNamingStrategy indexNamingStrategy,
							   ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
							   MapEntryTableNamingStrategy mapEntryTableNamingStrategy,
							   JoinColumnNamingStrategy joinColumnNamingStrategy,
							   ColumnNamingStrategy indexColumnNamingStrategy,
							   AssociationTableNamingStrategy associationTableNamingStrategy) {
		this.tableNamingStrategy = tableNamingStrategy;
		this.columnNamingStrategy = columnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.indexNamingStrategy = indexNamingStrategy;
		this.elementCollectionTableNamingStrategy = elementCollectionTableNamingStrategy;
		this.mapEntryTableNamingStrategy = mapEntryTableNamingStrategy;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
		this.associationTableNamingStrategy = associationTableNamingStrategy;
	}
	
	public TableNamingStrategy getTableNamingStrategy() {
		return tableNamingStrategy;
	}
	
	public ColumnNamingStrategy getColumnNamingStrategy() {
		return columnNamingStrategy;
	}
	
	public ForeignKeyNamingStrategy getForeignKeyNamingStrategy() {
		return foreignKeyNamingStrategy;
	}
	
	public IndexNamingStrategy getIndexNamingStrategy() {
		return indexNamingStrategy;
	}
	
	public ElementCollectionTableNamingStrategy getElementCollectionTableNamingStrategy() {
		return elementCollectionTableNamingStrategy;
	}
	
	public MapEntryTableNamingStrategy getEntryMapTableNamingStrategy() {
		return mapEntryTableNamingStrategy;
	}
	
	public JoinColumnNamingStrategy getJoinColumnNamingStrategy() {
		return joinColumnNamingStrategy;
	}
	
	public ColumnNamingStrategy getIndexColumnNamingStrategy() {
		return indexColumnNamingStrategy;
	}
	
	public AssociationTableNamingStrategy getAssociationTableNamingStrategy() {
		return associationTableNamingStrategy;
	}
}
