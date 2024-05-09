package org.codefilarete.stalactite.sql.spring.repository.query;

import java.util.ArrayList;
import java.util.List;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.operator.Between;
import org.codefilarete.stalactite.query.model.operator.Equals;
import org.codefilarete.stalactite.query.model.operator.Greater;
import org.codefilarete.stalactite.query.model.operator.In;
import org.codefilarete.stalactite.query.model.operator.IsNull;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.stalactite.query.model.operator.Lower;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.ModifiableInt;
import org.springframework.data.jpa.repository.query.PartTreeJpaQuery;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.lang.Nullable;

/**
 * {@link RepositoryQuery} for Stalactite. Inspired by {@link PartTreeJpaQuery}.
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
			this.query = new Query<>(entityPersister, tree);
			
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
		query.criteriaChain.consume(parameters);
		return query.executableEntityQuery.execute(accumulator);
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}
	
	static class Query<T> {
		
		private final ExecutableEntityQuery<T> executableEntityQuery;
		private final EntityPersister<T, ?> entityPersister;
		private final CriteriaChain criteriaChain;

		Query(EntityPersister<T, ?> entityPersister, PartTree tree) {
			this.entityPersister = entityPersister;
			this.criteriaChain = new CriteriaChain(new ArrayList<>());
			Holder<ExecutableEntityQuery<T>> resultHolder = new Holder<>();
			ModifiableInt partIndex = new ModifiableInt();
			tree.forEach(orPart -> {
				orPart.forEach(part -> {
							append(part, resultHolder, partIndex.getValue());
							partIndex.increment();
						}
				);
			});
			executableEntityQuery = resultHolder.get();
		}
		
		private void append(Part part, Holder<ExecutableEntityQuery<T>> resultHolder, int partIndex) {
			Criterion criterion = operator(part);
			// entityPersister doesn't support the creation of a ExecutableEntityQuery from scratch and requires to
			// create it from entityPersister.selectWhere : we call it for first part
			if (partIndex == 0) {
				ExecutableEntityQuery<T> criteriaHook = entityPersister.selectWhere(accessorChain(part.getProperty()), criterion.operator);
				resultHolder.set(criteriaHook);
			} else {
				resultHolder.get().and(accessorChain(part.getProperty()), criterion.operator);
			}
			this.criteriaChain.criteria.add(criterion);
		}
		
		private Criterion operator(Part part) {
			ConditionalOperator<?, ?> operator = null;
			switch (part.getType()) {
				case BETWEEN:
					operator = new Between<>();
					break;
				case IS_NOT_NULL:
					operator = new IsNull().not();
					break;
				case IS_NULL:
					operator = new IsNull();
					break;
				case LESS_THAN:
					operator = new Lower<>();
					break;
				case LESS_THAN_EQUAL:
					operator = new Lower<>().equals();
					break;
				case GREATER_THAN:
					operator = new Greater<>();
					break;
				case GREATER_THAN_EQUAL:
					operator = new Greater<>().equals();
					break;
				case BEFORE:
					break;
				case AFTER:
					break;
				case NOT_LIKE:
					operator = new Like(true, true).not();
					break;
				case LIKE:
					operator = new Like(true, true);
					break;
				case STARTING_WITH:
					operator = new Like(false, true);
					break;
				case ENDING_WITH:
					operator = new Like(true, false);
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
					operator = new In<>().not();
					break;
				case IN:
					operator = new In<>();
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
					operator = new Equals<>().not();
					break;
				case SIMPLE_PROPERTY:
					operator = new Equals<>();
			}
			if (operator == null) {
				throw new UnsupportedOperationException("Unsupported operator type: " + part.getType());
			} else {
				
				// Adapting result depending on kind of operator was instantiated
				if (operator instanceof Between) {
					return new Criterion(operator, 2) {
						@Override
						public Object convert(Object[] arguments, int argumentIndex) {
							return new Between.Interval<>(arguments[argumentIndex], arguments[argumentIndex + 1]);
						}
					};
				} else if (operator instanceof IsNull) {
					return new Criterion(operator, 0) {
						@Override
						public void setValue(Object[] arguments, int argumentIndex) {
							// IsNull doesn't set any value (at least it does nothing)
							// moreover default behavior will throw exception due to argument position computation:
							// findByNameIsNull() has no arg => arguments is empty making arguments[0] throws an IndexOutOfBoundsException 
						}
					};
				} else {
					return new Criterion(operator);
				}
			}
		}
		
		private <O> AccessorChain<T, O> accessorChain(PropertyPath property) {
			return new AccessorChain<>(Accessors.accessor(property.getOwningType().getType(), property.getSegment()));
		}
		
		/**
		 * Combination of an operator and its expected argument count.
		 * Helps to consume runtime arguments, see {@link CriteriaChain}
		 * @author Guillaume Mary
		 * @see CriteriaChain
		 */
		private static class Criterion {
			
			private final ConditionalOperator<Object, Object> operator;
			private final int argumentCount;
			
			private Criterion(ConditionalOperator<?, ?> operator) {
				this(operator, 1);
			}
			
			public Criterion(ConditionalOperator<?, ?> operator, int argumentCount) {
				this.operator = (ConditionalOperator<Object, Object>) operator;
				this.argumentCount = argumentCount;
			}
			
			public void setValue(Object[] arguments, int argumentIndex) {
				this.operator.setValue(convert(arguments, argumentIndex));
			}
			
			protected Object convert(Object[] arguments, int argumentIndex) {
				return arguments[argumentIndex];
			}
		}
		
		/**
		 * Service that consumes runtime arguments, see {@link #consume(Object[])}
		 * @author Guillaume Mary
		 * @see #consume(Object[])
		 */
		private static class CriteriaChain {
			
			private final List<Criterion> criteria;
			
			private CriteriaChain(List<Criterion> criteria) {
				this.criteria = criteria;
			}
			
			void consume(Object[] arguments) {
				int argumentIndex = 0;
				for (Criterion criterion : criteria) {
					criterion.setValue(arguments, argumentIndex);
					argumentIndex += criterion.argumentCount;
				}
			}
		}
	}
}
