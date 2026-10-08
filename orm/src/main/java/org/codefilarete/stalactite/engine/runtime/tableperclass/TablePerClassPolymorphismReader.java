package org.codefilarete.stalactite.engine.runtime.tableperclass;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass.TablePerClassAppender.TablePerClassUnion;
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

public class TablePerClassPolymorphismReader<C, I, T extends Table<T>, SUBENTITY extends C> extends ReadListenerWrapper<C, I> implements ConfiguredEntityReader<C, I, T> {
	
	private final TablePerClassPolymorphismEntityFinder<C, I, T> entityFinder;
	private final ConfiguredEntityReader<C, I, T> mainReader;
	private final Map<Class<SUBENTITY>, ? extends ConfiguredEntityReader<SUBENTITY, I, ?>> subEntitiesPersisters;
	private final TablePerClassUnion<SUBENTITY, I> tablePerClassUnion;
	
	public TablePerClassPolymorphismReader(ConfiguredEntityReader<C, I, T> mainReader,
	                                       Map<Class<SUBENTITY>, ? extends ConfiguredEntityReader<SUBENTITY, I, ?>> subEntitiesPersisters,
										   TablePerClassUnion<SUBENTITY, I> tablePerClassUnion,
	                                       ConnectionProvider connectionProvider,
	                                       Dialect dialect) {
		this.mainReader = mainReader;
		this.subEntitiesPersisters = subEntitiesPersisters;
		this.tablePerClassUnion = tablePerClassUnion;
		this.entityFinder = new TablePerClassPolymorphismEntityFinder<>(
				mainReader.getEntityJoinTree(),
				mainReader,
				new HashSet<>(subEntitiesPersisters.values()),
				tablePerClassUnion,
				connectionProvider,
				dialect);
	}
	
	public Map<Class<SUBENTITY>, ConfiguredEntityReader<SUBENTITY, I, ?>> getSubEntitiesPersisters() {
		return new HashMap<>(subEntitiesPersisters);
	}
	
	public TablePerClassUnion<SUBENTITY, I> getTablePerClassUnion() {
		return tablePerClassUnion;
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
