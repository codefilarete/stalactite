package org.codefilarete.stalactite.engine.runtime.tableperclass;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.ReadListenerWrapper;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister.ExecutableEntityQueryCriteria;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.projection.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TablePerClassPolymorphismReader<C, I, T extends Table<T>> extends ReadListenerWrapper<C, I> implements ConfiguredEntityReader<C, I, T> {
	
	protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	
	private final TablePerClassPolymorphismEntityFinder<C, I, T> entityFinder;
	private final ConfiguredEntityReader<C, I, T> mainReader;
	
	public TablePerClassPolymorphismReader(ConfiguredEntityReader<C, I, T> mainReader,
	                                       Map<? extends Class<C>, ? extends ConfiguredEntityReader<C, I, ?>> subEntitiesPersisters,
	                                       ConnectionProvider connectionProvider,
	                                       Dialect dialect) {
		this.mainReader = mainReader;
		this.entityFinder = new TablePerClassPolymorphismEntityFinder<>(
				mainReader.getEntityJoinTree(),
				mainReader,
				subEntitiesPersisters,
				connectionProvider,
				dialect);
	}
	
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return entityFinder.getEntityJoinTree();
	}
	
	@Override
	public EntityMapping<C, I, T> getMapping() {
		return mainReader.getMapping();
	}
	
	@Override
	public SelectListener<C, I> getSelectListener() {
		return mainReader.getSelectListener();
	}
	
	@Override
	public ExecutableEntityQueryCriteria<C, ?> selectWhere() {
		return newCriteriaSupport().wrapIntoExecutable();
	}
	
	public EntityQueryCriteriaSupport<C, I> newCriteriaSupport() {
		return entityFinder.newCriteriaSupport();
	}
	
	public ProjectionQueryCriteriaSupport<C, I> newProjectionCriteriaSupport(Consumer<SelectAdapter<C>> selectAdapter) {
		return new ProjectionQueryCriteriaSupport<>(entityFinder, newCriteriaSupport().getEntityCriteriaSupport(), selectAdapter);
	}
	
	@Override
	public ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter) {
		ProjectionQueryCriteriaSupport<C, I> projectionSupport = new ProjectionQueryCriteriaSupport<>(entityFinder, selectAdapter);
		return projectionSupport.wrapIntoExecutable();
	}
	
	@Override
	public Set<C> doSelect(Iterable<I> ids) {
		return entityFinder.select(ids);
	}
}
