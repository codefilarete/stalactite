package org.codefilarete.stalactite.engine.configurer.onetomany;

import javax.annotation.Nullable;
import java.util.Collection;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointByMethodReference;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * A {@link OneToManyRelation} dedicated to {@link Collection} with persisted order such as {@link java.util.List} or
 * {@link java.util.LinkedHashSet}.
 * 
 * 
 * @param <SRC> the "one" type
 * @param <TRGT> the "many" type
 * @param <TRGTID> identifier type of O
 */
public class OneToManyListRelation<SRC, TRGT, TRGTID, C extends Collection<TRGT>> extends OneToManyRelation<SRC, TRGT, TRGTID, C> {
	
	@Nullable
	private Column indexingColumn;
	
	@Nullable
	private String indexingColumnName;
	
	private boolean ordered = false;
	
	public <T extends Table> OneToManyListRelation(ReversibleAccessor<SRC, C> collectionProvider,
												   ValueAccessPointByMethodReference<SRC> methodReference,
												   EntityMappingConfiguration<? extends TRGT, TRGTID> targetMappingConfiguration,
												   T targetTable) {
		super(collectionProvider, methodReference, targetMappingConfiguration, targetTable);
	}
	
	public <T extends Table> OneToManyListRelation(ReversibleAccessor<SRC, C> collectionProvider,
												   ValueAccessPointByMethodReference<SRC> methodReference,
												   EntityMappingConfigurationProvider<? extends TRGT, TRGTID> targetMappingConfiguration,
												   T targetTable) {
		super(collectionProvider, methodReference, targetMappingConfiguration, targetTable);
	}
	
	public void setIndexingColumn(Column<? extends Table, Integer> indexingColumn) {
		indexed();
		this.indexingColumn = indexingColumn;
	}
	
	@Nullable
	public <T extends Table, O extends Object> Column<T, O> getIndexingColumn() {
		return indexingColumn;
	}
	
	public void setIndexingColumnName(String columnName) {
		indexed();
		this.indexingColumnName = columnName;
	}
	
	@Nullable
	public String getIndexingColumnName() {
		return indexingColumnName;
	}
	
	public void indexed() {
		this.ordered = true;
	}
	
	public boolean isOrdered() {
		return this.ordered;
	}
}
