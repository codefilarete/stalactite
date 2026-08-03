package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.EntityCriteria;
import org.codefilarete.stalactite.engine.EntitySelector;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.ReadListenerWrapper;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityFinder;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister.ExecutableEntityQueryCriteria;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.projection.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.query.Operators;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.collection.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityReader<C, I, T extends Table<T>> extends ReadListenerWrapper<C, I> implements ConfiguredEntityReader<C, I>, EntitySelector<C> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(EntityReader.class);
	
	/**
	 * Support for {@link EntityCriteria} query execution
	 */
	private final EntityFinder<C, I> entityFinder;
	/**
	 * Support for defining entity criteria on {@link #selectWhere()}
	 */
	private final EntityCriteriaSupport<C> criteriaSupport;
	private final EntityJoinTree<C, I> entityJoinTree;
	private final EntityMapping<C, I, T> mapping;
	private final Function<Iterable<I>, Set<C>> selector;
	
	public EntityReader(EntityMapping<C, I, T> mapping,
	                    ConnectionProvider connectionProvider,
	                    Dialect dialect) {
		this.entityJoinTree = new EntityJoinTree<>(new EntityMappingAdapter<>(mapping), mapping.getTargetTable());
		this.mapping = mapping;
		this.entityFinder = new RelationalEntityFinder<>(entityJoinTree, this, connectionProvider, dialect);
		this.criteriaSupport = new EntityCriteriaSupport<>(entityJoinTree);
		// computing selector in the constructor to avoid making it at each select call
		IdMapping<C, I> idMapping = mapping.getIdMapping();
		if (idMapping.getIdentifierAssembler() instanceof ComposedIdentifierAssembler) {
			// && dialect.supportTupleIn
			selector = ids -> {
				Map<? extends Column<?, ?>, ?> columnValues = ((ComposedIdentifierAssembler<I, ?>) idMapping.getIdentifierAssembler()).getColumnValues(ids);
				TupleIn tupleIn = TupleIn.transformBeanColumnValuesToTupleInValues(Iterables.size(ids), columnValues);
				EntityQueryCriteriaSupport<C, I> newCriteriaSupport = newCriteriaSupport();
				newCriteriaSupport.getEntityCriteriaSupport().getCriteria().and(tupleIn);
				return newCriteriaSupport.wrapIntoExecutable().execute(Accumulators.toSet());
			};
		} else {
			AccessorWrapperIdAccessor<C, I> idAccessor = (AccessorWrapperIdAccessor<C, I>) idMapping.getIdAccessor();
			selector = ids -> selectWhere().and(new AccessorChain<>(idAccessor.getIdAccessor()), Operators.in(ids)).execute(Accumulators.toSet());
		}
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return entityJoinTree;
	}
	
	@Override
	public EntityMapping<C, I, T> getMapping() {
		return mapping;
	}
	
	public SelectListener<C, I> getSelectListener() {
		return persisterListener.getSelectListener();
	}
	
	@Override
	public Set<C> doSelect(Iterable<I> ids) {
		LOGGER.debug("selecting entities {}", ids);
		return selector.apply(ids);
	}
	
	public EntityQueryCriteriaSupport<C, I> newCriteriaSupport() {
		return new EntityQueryCriteriaSupport<>(entityFinder, criteriaSupport.copy());
	}
	
	@Override
	public ExecutableEntityQueryCriteria<C, ?> selectWhere() {
		return newCriteriaSupport().wrapIntoExecutable();
	}
	
	public ProjectionQueryCriteriaSupport<C, I> newProjectionCriteriaSupport(Consumer<SelectAdapter<C>> selectAdapter) {
		return new ProjectionQueryCriteriaSupport<>(entityFinder, newCriteriaSupport().getEntityCriteriaSupport(), selectAdapter);
	}
	
	@Override
	public ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter) {
		return newProjectionCriteriaSupport(selectAdapter).wrapIntoExecutable();
	}
}
