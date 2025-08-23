package org.codefilarete.stalactite.spring.repository.query;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.query.AggregateAccessPointToColumnMapping;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.tool.collection.Iterables;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.EntityProjectionIntrospector;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

public class ProjectionTypeInformationExtractor<C> {

	private IdentityHashMap<Selectable<?>, String> aliases = new IdentityHashMap<>();
	private IdentityHashMap<Selectable<?>, PropertyPath> columnToProperties = new IdentityHashMap<>();
	private final AggregateAccessPointToColumnMapping<C> aggregateColumnMapping;
	private final EntityProjectionIntrospector entityProjectionIntrospector;
	private final Class<C> aggregateType;

	/**
	 * Initiates an instance capable of collecting aliases and their matching property of the aggregate of the given entityPersister
	 * @param factory 
	 * @param entityPersister
	 */
	public ProjectionTypeInformationExtractor(ProjectionFactory factory, AdvancedEntityPersister<C, ?> entityPersister) {
		// The projection is closed: it means there's not @Value on the interface, so we can use Spring property introspector to look up for
		// properties to select in the query
		// If the projection is open (any method as a @Value on it), then, because Spring can't know in advance which field will be required to
		// evaluate the @Value expression, we must retrieve the whole aggregate as entities.
		// se https://docs.spring.io/spring-data/jpa/reference/repositories/projections.html
		EntityProjectionIntrospector.ProjectionPredicate predicate = (returnType, domainType)
				-> !domainType.isAssignableFrom(returnType) && !returnType.isAssignableFrom(domainType);

		this.entityProjectionIntrospector = EntityProjectionIntrospector.create(factory, predicate, new RelationalMappingContext());
		this.aggregateColumnMapping = entityPersister.getEntityFinder().newCriteriaSupport().getEntityCriteriaSupport().getAggregateColumnMapping();
		this.aggregateType = entityPersister.getClassToPersist();
	}

	public IdentityHashMap<Selectable<?>, String> getAliases() {
		return aliases;
	}

	public IdentityHashMap<Selectable<?>, PropertyPath> getColumnToProperties() {
		return columnToProperties;
	}

	/**
	 * Extracts the {@link Selectable} and {@link PropertyPath} from the {@link ProjectionFactory} and {@link AdvancedEntityPersister} construction time.
	 * The result is stored in {@link #columnToProperties} and {@link #aliases}.
	 */
	public void extract(Class<?> projectionTypeToIntrospect) {
		aliases = new IdentityHashMap<>();
		columnToProperties = new IdentityHashMap<>();

		EntityProjection<?, C> projectionTypeIntrospection = entityProjectionIntrospector.introspect(projectionTypeToIntrospect, aggregateType);
		projectionTypeIntrospection.forEachRecursive(projectionProperty -> {
			AccessorChain accessorChain = new AccessorChain<>();
			List<PropertyPath> collect = projectionProperty.getPropertyPath().stream().collect(Collectors.toList());
			// there's a bug here: when property length is higher than 2 it contains an extra item which makes the whole algorithm broken (Spring bug ?)
			if (collect.size() >= 2) {
				collect = Iterables.cutTail(collect);
			}
			collect.forEach(propertyPath -> {
				propertyPath.forEach(propertyPath1 -> {
					accessorChain.add(Accessors.accessor(propertyPath1.getOwningType().getType(), propertyPath1.getSegment(), propertyPath1.getType()));
				});
			});
			Selectable<?> selectable = aggregateColumnMapping.giveColumn(accessorChain.getAccessors());
			columnToProperties.put(selectable, projectionProperty.getPropertyPath());
			String alias = projectionProperty.getPropertyPath().toDotPath().replace('.', '_');
			aliases.put(selectable, alias);
		});
	}
}
