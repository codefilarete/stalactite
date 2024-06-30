package org.codefilarete.stalactite.sql.spring.repository.query;

import java.util.ArrayList;
import java.util.List;

import org.codefilarete.reflection.Accessor;
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
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.ParameterOutOfBoundsException;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.Streamable;
import org.springframework.lang.Nullable;

/**
 * {@link RepositoryQuery} for Stalactite. Inspired by {@link PartTreeJpaQuery}.
 * The parsing of the {@link QueryMethod} is made by Spring {@link PartTree}, hence this class only iterates over parts
 * to create a Stalactite query.
 * 
 * @param <C> entity type
 * @author Guillaume Mary
 */
public class PartTreeStalactiteQuery<C> implements RepositoryQuery {
	
	private final QueryMethod method;
	private final Query<C> query;
	private final Accumulator<C, ?, Object> accumulator;
	
	public PartTreeStalactiteQuery(QueryMethod method, EntityPersister<C, ?> entityPersister) {
		this.method = method;
		this.accumulator = method.isCollectionQuery()
				? (Accumulator) Accumulators.toList()
				: (Accumulator) Accumulators.getFirstUnique();
		Parameters<?, ?> parameters = method.getParameters();
		
		Class<C> domainClass = entityPersister.getClassToPersist();
		
		boolean recreationRequired = parameters.hasDynamicProjection() || parameters.potentiallySortsDynamically();
		
		try {
			
			PartTree tree = new PartTree(method.getName(), domainClass);
			new QueryMethodValidator(tree, method).validate();
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
			Criterion criterion = convertToOperator(part.getType());
			// entityPersister doesn't support the creation of a ExecutableEntityQuery from scratch and requires to
			// create it from entityPersister.selectWhere : we call it for first part
			if (partIndex == 0) {
				ExecutableEntityQuery<T> criteriaHook = entityPersister.selectWhere(convertToAccessorChain(part.getProperty()), criterion.operator);
				resultHolder.set(criteriaHook);
			} else {
				resultHolder.get().and(convertToAccessorChain(part.getProperty()), criterion.operator);
			}
			this.criteriaChain.criteria.add(criterion);
		}
		
		private Criterion convertToOperator(Type type) {
			ConditionalOperator<?, ?> operator = null;
			switch (type) {
				case BETWEEN:
					operator = new Between<>();
					break;
				case IS_NOT_NULL:
					operator = new IsNull().not();
					break;
				case IS_NULL:
					operator = new IsNull();
					break;
				case BEFORE:
				case LESS_THAN:
					operator = new Lower<>();
					break;
				case LESS_THAN_EQUAL:
					operator = new Lower<>().equals();
					break;
				case AFTER:
				case GREATER_THAN:
					operator = new Greater<>();
					break;
				case GREATER_THAN_EQUAL:
					operator = new Greater<>().equals();
					break;
				case NOT_LIKE:
					operator = new Like(true, true).not();
					break;
				case LIKE:
					operator = new Like(true, true);
					break;
				case STARTING_WITH:
					operator = Like.startsWith();
					break;
				case ENDING_WITH:
					operator = Like.endsWith();
					break;
				case IS_NOT_EMPTY:
					break;
				case IS_EMPTY:
					break;
				case NOT_CONTAINING:
					operator = Like.contains().not();
					break;
				case CONTAINING:
					operator = Like.contains();
					break;
				case NOT_IN:
					operator = new In<>().not();
					break;
				case IN:
					operator = new In<>();
					break;
				case TRUE:
					operator = new Equals<>(true);
					break;
				case FALSE:
					operator = new Equals<>(false);
					break;
				case NEGATING_SIMPLE_PROPERTY:
					operator = new Equals<>().not();
					break;
				case SIMPLE_PROPERTY:
					operator = new Equals<>();
					break;
				// Hereafter operators are not supported for different reasons :
				// - JPA doesn't either,
				// - too database-specific,
				// - don't know on which operator to bind them (no javadoc)
				case NEAR:
				case WITHIN:
				case REGEX:
				case EXISTS:
			}
			if (operator == null) {
				throw new UnsupportedOperationException("Unsupported operator type: " + type);
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
				} else if (type == Type.TRUE || type == Type.FALSE) {
					return new Criterion(operator, 0) {
						@Override
						public void setValue(Object[] arguments, int argumentIndex) {
							// TRUE and FALSE don't set any value
							// moreover default behavior will throw exception due to argument position computation:
							// findByNameIsNull() has no arg => arguments is empty making arguments[0] throws an IndexOutOfBoundsException 
						}
					};
				} else {
					return new Criterion(operator);
				}
			}
		}
		
		private <O> AccessorChain<T, O> convertToAccessorChain(PropertyPath property) {
			List<Accessor<?, ?>> accessorChain = new ArrayList<>(); 
			property.forEach(path -> 
					accessorChain.add(Accessors.accessor(path.getOwningType().getType(), path.getSegment())));
			return new AccessorChain<>(accessorChain);
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
	
	/**
	 * Validator for query method. Basically, it ensures that arguments match length and types of properties.
	 * @author Guillaume Mary
	 * @see #validate() 
	 */
	public static class QueryMethodValidator {
		
		private final PartTree tree;
		private final QueryMethod method;
		
		public QueryMethodValidator(PartTree tree, QueryMethod method) {
			this.tree = tree;
			this.method = method;
		}
		
		public void validate() {
			int argCount = 0;
			Iterable<Part> parts = () -> tree.stream().flatMap(Streamable::stream).iterator();
			for (Part part : parts) {
				int numberOfArguments = part.getNumberOfArguments();
				for (int i = 0; i < numberOfArguments; i++) {
					throwExceptionOnArgumentMismatch(part, argCount);
					argCount++;
				}
			}
		}
		
		private void throwExceptionOnArgumentMismatch(Part part, int index) {
			Type type = part.getType();
			String property = part.getProperty().toDotPath();
			
			Parameter parameter;
			try {
				parameter = method.getParameters().getBindableParameter(index);
			} catch (ParameterOutOfBoundsException e) {
				throw new IllegalStateException(String.format(
						"Method %s expects at least %d arguments but only found %d. This leaves an operator of type %s for property %s unbound.",
						method.getName(), index + 1, index, type.name(), property));
			}
			
			if (expectsCollection(type) && !parameterIsCollectionLike(parameter)) {
				throw new IllegalStateException(wrongParameterTypeMessage(property, type, "Collection", parameter));
			} else if (!expectsCollection(type) && !parameterIsScalarLike(parameter)) {
				throw new IllegalStateException(wrongParameterTypeMessage(property, type, "scalar", parameter));
			}
		}
		
		private String wrongParameterTypeMessage(String property, Type operatorType, String expectedArgumentType, Parameter parameter) {
			
			return String.format("Operator %s on %s requires a %s argument, found %s in method %s.", operatorType.name(),
					property, expectedArgumentType, parameter.getType(), method.getName());
		}
		
		private boolean parameterIsCollectionLike(Parameter parameter) {
			return Iterable.class.isAssignableFrom(parameter.getType()) || parameter.getType().isArray();
		}
		
		/**
		 * Arrays are may be treated as collection like or in the case of binary data as scalar
		 */
		private boolean parameterIsScalarLike(Parameter parameter) {
			return !Iterable.class.isAssignableFrom(parameter.getType());
		}
		
		private boolean expectsCollection(Type type) {
			return type == Type.IN || type == Type.NOT_IN;
		}
	}
}
