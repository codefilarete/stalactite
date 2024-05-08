package org.codefilarete.stalactite.sql.spring.repository.query;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.operator.Equals;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.ModifiableInt;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.lang.Nullable;

/**
 * {@link RepositoryQuery} for Stalactite. Inspired by {@link org.springframework.data.jpa.repository.query.PartTreeJpaQuery}.
 * The parsing of the {@link QueryMethod} is made by Spring {@link PartTree}, hence this class only iterates over parts
 * to create a Stalactite query.
 * 
 * @param <T>
 * @author Guillaume Mary
 */
public class PartTreeStalactiteQuery<T> implements RepositoryQuery {
	
	private final QueryMethod method;
	private final Query<T> query;
	
	public PartTreeStalactiteQuery(QueryMethod method, EntityPersister<T, ?> entityPersister) {
		this.method = method;
		Parameters<?, ?> parameters = method.getParameters();
		
		Class<T> domainClass = entityPersister.getClassToPersist();
		
		boolean recreationRequired = parameters.hasDynamicProjection() || parameters.potentiallySortsDynamically();
		
		try {
			
			PartTree tree = new PartTree(method.getName(), domainClass);
//			validate(tree, parameters, method.toString());
//			this.countQuery = new CountQueryPreparer(recreationRequired);
//			this.query = tree.isCountProjection() ? countQuery : new QueryPreparer(recreationRequired);
			this.query = new Query<>(entityPersister, tree, parameters);
			
		} catch (Exception o_O) {
			throw new IllegalArgumentException(
					String.format("Failed to create query for method %s! %s", method, o_O.getMessage()), o_O);
		}
	}
	
	@Override
	@Nullable
	public Object execute(Object[] parameters) {
		Accumulator<T, ?, Object> accumulator = method.isCollectionQuery()
				? (Accumulator) Accumulators.toList()
				: (Accumulator) Accumulators.getFirstUnique();
		for (int i = 0; i < parameters.length; i++) {
			query.giveOperator(i).setValue(parameters[i]);
		}
		return query.executableEntityQuery.execute(accumulator);
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}
	
	static class Query<T> {
		
		private final ExecutableEntityQuery<T> executableEntityQuery;
		private final EntityPersister<T, ?> entityPersister;
		
//		private final Map<Parameter, ConditionalOperator> arguments = new HashMap<>();
		private final Map<Integer, ConditionalOperator> argumentsPerIndex = new HashMap<>();
		
		Query(EntityPersister<T, ?> entityPersister, PartTree tree, Parameters<?, ?> parameters) {
			this.entityPersister = entityPersister;
			Holder<ExecutableEntityQuery<T>> resultHolder = new Holder<>();
			ModifiableInt partIndex = new ModifiableInt(-1);    // because we increment it early
			tree.forEach(orPart -> {
				orPart.forEach(part -> {
							partIndex.increment();
							append(parameters, part, resultHolder, partIndex.getValue());
						}
				);
			});
			executableEntityQuery = resultHolder.get();
		}
		
		private void append(Parameters<?, ?> parameters, Part part, Holder<ExecutableEntityQuery<T>> resultHolder, int partIndex) {
			ConditionalOperator<Object, ?> operator = operator(part);
			// entityPersister doesn't support the creation of a ExecutableEntityQuery from scratch and requires to
			// create it from entityPersister.selectWhere : we call it for first part
			if (partIndex == 0) {
				ExecutableEntityQuery<T> criteriaHook = entityPersister.selectWhere(accessorChain(part.getProperty()), operator);
				resultHolder.set(criteriaHook);
			} else {
				resultHolder.get().and(accessorChain(part.getProperty()), operator);
			}
//			this.arguments.put(parameters.getBindableParameter(partIndex), operator);
			this.argumentsPerIndex.put(parameters.getBindableParameter(partIndex).getIndex(), operator);
		}
		
		ConditionalOperator<Object, Object> giveOperator(int index) {
			return argumentsPerIndex.get(index);
//			return Iterables.find(arguments.entrySet(), entry -> entry.getKey().getIndex() == index).getValue();
		}
		
		private <O> ConditionalOperator<O, ?> operator(Part part) {
			switch (part.getType()) {
				case BETWEEN:
					break;
				case IS_NOT_NULL:
					break;
				case IS_NULL:
					break;
				case LESS_THAN:
					break;
				case LESS_THAN_EQUAL:
					break;
				case GREATER_THAN:
					break;
				case GREATER_THAN_EQUAL:
					break;
				case BEFORE:
					break;
				case AFTER:
					break;
				case NOT_LIKE:
					break;
				case LIKE:
					return (ConditionalOperator<O, ?>) new Like(true, true);
				case STARTING_WITH:
					break;
				case ENDING_WITH:
					break;
				case IS_NOT_EMPTY:
					break;
				case IS_EMPTY:
					break;
				case NOT_CONTAINING:
					break;
				case CONTAINING:
					break;
				case NOT_IN:
					break;
				case IN:
					break;
				case NEAR:
					break;
				case WITHIN:
					break;
				case REGEX:
					break;
				case EXISTS:
					break;
				case TRUE:
					break;
				case FALSE:
					break;
				case NEGATING_SIMPLE_PROPERTY:
					break;
				case SIMPLE_PROPERTY:
					return new Equals<>();
			}
			return null;
		}
		
		private <O> AccessorChain<T, O> accessorChain(PropertyPath property) {
			return new AccessorChain<>(Accessors.accessor(property.getOwningType().getType(), property.getSegment()));
		}
	}
}
