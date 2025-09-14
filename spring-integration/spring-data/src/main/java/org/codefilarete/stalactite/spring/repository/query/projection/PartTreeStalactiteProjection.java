package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.spring.repository.query.execution.AbstractQueryExecutor;
import org.codefilarete.stalactite.spring.repository.query.execution.AbstractRepositoryQuery;
import org.codefilarete.stalactite.spring.repository.query.domain.DomainEntityQueryExecutor;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.spring.repository.query.execution.StalactiteQueryMethodInvocationParameters;
import org.codefilarete.stalactite.spring.repository.query.derivation.ToCriteriaPartTreeTransformer;
import org.codefilarete.tool.VisibleForTesting;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.query.parser.PartTree;

public class PartTreeStalactiteProjection<C, R> extends AbstractRepositoryQuery<C, R> {
	
	private final ToCriteriaPartTreeTransformer<C> projectionCriteriaAppender;
	
	/**
	 * Fills given {@link Map} with some more {@link Map}s to create a hierarchic structure from the given dotted property name, e.g. "a.b.c" will
	 * result in a map like:
	 * <pre>{@code
	 *   "a": {
	 *     "b": {
	 *       "c": value
	 *     }
	 *   }
	 * }</pre>
	 * If the given Map already contains data, then it will be filled without overriding the existing ones, e.g. given the above {@link Map}, if we
	 * call this method with "a.b.d" and a value, then the resulting {@link Map} will be:
	 * <pre>{@code
	 *   "a": {
	 *     "b": {
	 *       "c": value
	 *       "d": other_value
	 *     }
	 *   }
	 * }</pre>	 *
	 *
	 * @param dottedProperty the dotted property name
	 * @param value the value to set at the leaf of the map
	 * @param root the root map to build upon
	 */
	@VisibleForTesting
	public static void buildHierarchicMap(AccessorChain<?, ?> dottedProperty, Object value, Map<String, Object> root) {
		Map<String, Object> current = root;
		// Navigate through all parts except the last one
		int lengthMinus1 = dottedProperty.getAccessors().size() - 1;
		for (int i = 0; i < lengthMinus1; i++) {
			Accessor<?, ?> accessor = dottedProperty.getAccessors().get(i);
			String propertyName = AccessorDefinition.giveDefinition(accessor).getName();
			current = (Map<String, Object>) current.computeIfAbsent(propertyName, k -> new HashMap<>());
		}
		// Set the value in the leaf Map
		Accessor<?, ?> lastAccessor = dottedProperty.getAccessors().get(lengthMinus1);
		String propertyName = AccessorDefinition.giveDefinition(lastAccessor).getName();
		current.putIfAbsent(propertyName, value);
	}
	
	private final ProjectionMappingFinder<C> projectionMappingFinder;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final PartTree partTree;
	private final ProjectionFactory factory;
	private final PartTreeStalactiteCountProjection<C> countQuery;
	private final ProjectionQueryCriteriaSupport<C, ?> defaultProjectionQueryCriteriaSupport;
	
	public PartTreeStalactiteProjection(StalactiteQueryMethod method,
										AdvancedEntityPersister<C, ?> entityPersister,
										PartTree partTree,
										ProjectionFactory factory) {
		super(method);
		this.entityPersister = entityPersister;
		this.partTree = partTree;
		this.factory = factory;
		
		this.countQuery = new PartTreeStalactiteCountProjection<>(method, entityPersister, partTree);
		this.projectionMappingFinder = new ProjectionMappingFinder<>(factory, entityPersister);
		
		this.projectionMappingFinder.lookup(method.getDomainClass());
		// by default, we don't customize the select clause because it will be adapted at very last time, during execution according to the projection
		// type which can be dynamic
		this.defaultProjectionQueryCriteriaSupport = entityPersister.newProjectionCriteriaSupport(selectables -> {});
		this.projectionCriteriaAppender = new ToCriteriaPartTreeTransformer<>(partTree, entityPersister.getClassToPersist());
		this.projectionCriteriaAppender.applyTo(
				defaultProjectionQueryCriteriaSupport.getEntityCriteriaSupport(),
				defaultProjectionQueryCriteriaSupport.getQueryPageSupport(),
				defaultProjectionQueryCriteriaSupport.getQueryPageSupport());
	}
	
	@Override
	protected AbstractQueryExecutor<List<Object>, Object> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters) {
		// Extracting the Selectable and PropertyPath from the projection type
		boolean runProjectionQuery;
		IdentityHashMap<JoinLink<?, ?>, AccessorChain<C, ?>> propertiesColumns;
		if (method.getParameters().hasDynamicProjection()) {
			propertiesColumns = this.projectionMappingFinder.lookup(invocationParameters.getDynamicProjectionType());
			runProjectionQuery = factory.getProjectionInformation(invocationParameters.getDynamicProjectionType()).isClosed()
					&& !invocationParameters.getDynamicProjectionType().isAssignableFrom(entityPersister.getClassToPersist());
		} else {
			propertiesColumns = this.projectionMappingFinder.lookup(method.getReturnedObjectType());
			runProjectionQuery = factory.getProjectionInformation(method.getReturnedObjectType()).isClosed();
		}
		if (runProjectionQuery) {
			return new ProjectionQueryExecutor<>(method, defaultProjectionQueryCriteriaSupport, propertiesColumns);
		} else {
			// if the projection is not closed (contains @Value for example), then we must fetch the whole entity
			// because we can't know in advance which property will be required to evaluate the @Value
			// therefore we use the default query that select all columns of the aggregate
			// see https://docs.spring.io/spring-data/jpa/reference/repositories/projections.html
			return (AbstractQueryExecutor) new DomainEntityQueryExecutor<>(method, entityPersister, partTree);
		}
	}
	
	@Override
	protected LongSupplier buildCountSupplier(StalactiteQueryMethodInvocationParameters accessor) {
		return () -> countQuery.execute(accessor.getValues());
	}
}
