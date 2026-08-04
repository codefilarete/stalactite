package org.codefilarete.stalactite.engine.runtime.jointable;

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

public class JoinTablePolymorphismReader<C, I, T extends Table<T>> extends ReadListenerWrapper<C, I> implements ConfiguredEntityReader<C, I, T> {
	
	private final JoinTablePolymorphismEntityFinder<C, I, T> entityFinder;
	private final ConfiguredEntityReader<C, I, T> mainReader;
	
	public JoinTablePolymorphismReader(ConfiguredEntityReader<C, I, T> mainReader,
	                                   Map<Class<? extends C>, ? extends ConfiguredEntityReader<? extends C, I, ?>> subEntitiesPersisters,
	                                   ConnectionProvider connectionProvider,
	                                   Dialect dialect) {
		this.mainReader = mainReader;
		this.entityFinder = new JoinTablePolymorphismEntityFinder<>(
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
