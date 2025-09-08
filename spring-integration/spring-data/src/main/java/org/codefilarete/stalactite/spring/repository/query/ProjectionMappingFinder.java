package org.codefilarete.stalactite.spring.repository.query;

import java.util.IdentityHashMap;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.query.AggregateAccessPointToColumnMapping;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Selectable;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.EntityProjectionIntrospector;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Helper class to provide the {@link Selectable}s necessary to build some projection instance from a database query (native or not).
 * The columns are taken from the aggregate type.
 *
 * @param <C> the aggregate (root entity) type
 * @author Guillaume Mary
 */
public class ProjectionMappingFinder<C> {
	
	private final AggregateAccessPointToColumnMapping<C> aggregateColumnMapping;
	private final EntityProjectionIntrospector entityProjectionIntrospector;
	private final Class<C> aggregateType;
	
	/**
	 * Initiates an instance capable of collecting aliases and their matching property of the aggregate of the given entityPersister
	 *
	 * @param factory
	 * @param entityPersister
	 */
	public ProjectionMappingFinder(ProjectionFactory factory, AdvancedEntityPersister<C, ?> entityPersister) {
		// The projection is closed: it means there's not @Value on the interface, so we can use Spring property introspector to look up for
		// properties to select in the query
		// If the projection is open (any method as a @Value on it), then, because Spring can't know in advance which field will be required to
		// evaluate the @Value expression, we must retrieve the whole aggregate as entities.
		// se https://docs.spring.io/spring-data/jpa/reference/repositories/projections.html
		EntityProjectionIntrospector.ProjectionPredicate isProjectionTest = (returnType, domainType)
				-> !returnType.isAssignableFrom(domainType);
		
		// We create a dumb MappingContext (un-typed) to "just make it work". I'm unsure about the way it works, but implementing it is a bit
		// complex for Stalactite due to the need to have PersistentEntity which requires also some concepts like PreferredConstructor or InstanceCreatorMetadata
		// => to be implemented if this dumb implementation is unsufficient
		MappingContext mappingContext = new RelationalMappingContext();
		this.entityProjectionIntrospector = EntityProjectionIntrospector.create(factory, isProjectionTest, mappingContext);
		this.aggregateColumnMapping = entityPersister.getEntityFinder().newCriteriaSupport().getEntityCriteriaSupport().getAggregateColumnMapping();
		this.aggregateType = entityPersister.getClassToPersist();
	}
	
	/**
	 * Extracts the {@link JoinLink} and {@link PropertyPath} from the {@link ProjectionFactory} and {@link AdvancedEntityPersister} of construction time.
	 * The algorithm is based on Spring property introspection to make us match the way it detects the properties of a projection. Thus, we are much more
	 * compatible with Spring Data than if we re-invent the wheel. Meanwhile, the will to re-invent it is very tempting because the algorithm is unclear,
	 * not well-documented, with a lot of closed / private classes.
	 *
	 * @param projectionTypeToIntrospect the projection type to introspect
	 * @return a map of {@link JoinLink} to {@link PropertyPath}
	 */
	public IdentityHashMap<JoinLink<?, ?>, AccessorChain<C, ?>> lookup(Class<?> projectionTypeToIntrospect) {
		IdentityHashMap<JoinLink<?, ?>, AccessorChain<C, ?>> result = new IdentityHashMap<>();
		
		EntityProjection<?, C> projectionTypeIntrospection = entityProjectionIntrospector.introspect(projectionTypeToIntrospect, aggregateType);
		projectionTypeIntrospection.forEachRecursive(projectionProperty -> {
			AccessorChain accessorChain = convertToAccessorChain(projectionProperty.getPropertyPath());
			try {
				JoinLink<?, ?> selectable = aggregateColumnMapping.giveColumn(accessorChain.getAccessors());
				result.put(selectable, accessorChain);
			} catch (RuntimeException e) {
				// MADE TO AVOID Error while looking for column of o.c.s.e.m.Republic.getPrimeMinister() : it is not declared in mapping of o.c.s.e.m.Republic
			}
		});
		return result;
	}
	
	private AccessorChain<?, ?> convertToAccessorChain(PropertyPath propertyPath2) {
		AccessorChain<?, ?> result = new AccessorChain<>();
		propertyPath2.forEach(propertyPath -> {
			result.add(Accessors.accessor(propertyPath.getOwningType().getType(), propertyPath.getSegment(), propertyPath.getType()));
		});
		return result;
	}
}
