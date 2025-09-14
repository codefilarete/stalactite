package org.codefilarete.stalactite.spring.repository.query;

import java.util.ArrayList;
import java.util.List;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.operator.Between;
import org.codefilarete.stalactite.query.model.operator.Equals;
import org.codefilarete.stalactite.query.model.operator.Greater;
import org.codefilarete.stalactite.query.model.operator.In;
import org.codefilarete.stalactite.query.model.operator.InIgnoreCase;
import org.codefilarete.stalactite.query.model.operator.IsNull;
import org.codefilarete.stalactite.query.model.operator.Lesser;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.tool.collection.Arrays;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.Part.Type;

/**
 * Base class to build a query from a method.
 * 
 * @param <T>
 * @author Guillaume Mary
 */
public abstract class AbstractDerivedQuery<T> {
	
	protected final List<Criterion> condition;
	
	private final Class<T> entityType;
	
	public AbstractDerivedQuery(Class<T> entityType) {
		this.entityType = entityType;
		this.condition = new ArrayList<>();
	}
	
	public Criterion append(Part part) {
		Criterion criterion = convertToCriterion(part.getType(), part.shouldIgnoreCase() != IgnoreCaseType.NEVER);
		this.condition.add(criterion);
		return criterion;
	}

	private Criterion convertToCriterion(Type type, boolean ignoreCase) {
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
				operator = new Lesser<>();
				break;
			case LESS_THAN_EQUAL:
				operator = new Lesser<>().equals();
				break;
			case AFTER:
			case GREATER_THAN:
				operator = new Greater<>();
				break;
			case GREATER_THAN_EQUAL:
				operator = new Greater<>().equals();
				break;
			case NOT_LIKE:
				operator = new Like<>(false, false).not();
				if (ignoreCase) {
					operator = ((Like) operator).ignoringCase();
				}
				break;
			case LIKE:
				operator = new Like<>(false, false);
				if (ignoreCase) {
					operator = ((Like) operator).ignoringCase();
				}
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
				if (ignoreCase) {
					operator = ((In) operator).ignoringCase();
				}
				break;
			case IN:
				operator = new In<>();
				if (ignoreCase) {
					operator = ((In) operator).ignoringCase();
				}
				break;
			case TRUE:
				operator = new Equals<>(true);
				break;
			case FALSE:
				operator = new Equals<>(false);
				break;
			case NEGATING_SIMPLE_PROPERTY:
				operator = new Equals<>().not();
				if (ignoreCase) {
					operator = ((Equals) operator).ignoringCase();
				}
				break;
			case SIMPLE_PROPERTY:
				operator = new Equals<>();
				if (ignoreCase) {
					operator = ((Equals) operator).ignoringCase();
				}
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
				return new BetweenCriterion((Between<?>) operator);
			} else if (operator instanceof In || operator instanceof InIgnoreCase) {
				return new InCriterion(operator);
			} else if (operator instanceof IsNull) {
				return new IsNullCriterion((IsNull) operator);
			} else if (type == Type.TRUE || type == Type.FALSE) {
				return new BooleanCriterion(operator);
			} else {
				return new Criterion(operator);
			}
		}
	}
	
	protected <O> AccessorChain<T, O> convertToAccessorChain(Order property) {
		return this.convertToAccessorChain(PropertyPath.from(property.getProperty(), entityType));
	}
	
	protected <O> AccessorChain<T, O> convertToAccessorChain(PropertyPath property) {
		List<Accessor<?, ?>> accessorChain = new ArrayList<>();
		property.forEach(path ->
				accessorChain.add(Accessors.accessor(path.getOwningType().getType(), path.getSegment())));
		return new AccessorChain<>(accessorChain);
	}
	
	public void consume(Object[] arguments) {
		int argumentIndex = 0;
		for (Criterion criterion : condition) {
			criterion.setValue(arguments, argumentIndex);
			argumentIndex += criterion.argumentCount;
		}
	}
	
	/**
	 * Combination of an operator and its expected argument count.
	 * Helps to consume runtime arguments, see {@link #consume(Object[])}
	 *
	 * @author Guillaume Mary
	 */
	public static class Criterion {
		
		protected final ConditionalOperator<Object, Object> condition;
		protected final int argumentCount;
		
		private Criterion(ConditionalOperator<?, ?> condition) {
			this(condition, 1);
		}
		
		public Criterion(ConditionalOperator<?, ?> condition, int argumentCount) {
			this.condition = (ConditionalOperator<Object, Object>) condition;
			this.argumentCount = argumentCount;
		}
		
		public void setValue(Object[] arguments, int argumentIndex) {
			this.condition.setValue(convert(arguments, argumentIndex));
		}
		
		protected Object convert(Object[] arguments, int argumentIndex) {
			return arguments[argumentIndex];
		}
	}
	
	private static class BetweenCriterion extends Criterion {
		
		public BetweenCriterion(Between<?> operator) {
			super(operator, 2);
		}

		@Override
		public Object convert(Object[] arguments, int argumentIndex) {
			return new Between.Interval<>(arguments[argumentIndex], arguments[argumentIndex + 1]);
		}
	}

	private static class IsNullCriterion extends Criterion {
		
		public IsNullCriterion(IsNull operator) {
			super(operator, 0);
		}

		@Override
		public void setValue(Object[] arguments, int argumentIndex) {
			// IsNull doesn't set any value (at least it does nothing)
			// moreover default behavior will throw exception due to argument position computation:
			// findByNameIsNull() has no arg => arguments is empty making arguments[0] throws an IndexOutOfBoundsException 
		}
	}

	private static class InCriterion extends Criterion {
		public InCriterion(ConditionalOperator<?, ?> operator) {
			super(operator, 2);
		}

		@Override
		public Object convert(Object[] arguments, int argumentIndex) {
			Object argument = arguments[argumentIndex];
			if (argument instanceof Object[]) {
				// converting varargs to an Iterable because it's what's expected by "In" class as value
				return Arrays.asList((Object[]) argument);
			} else {
				return argument;
			}
		}
	}

	private static class BooleanCriterion extends Criterion {
		public BooleanCriterion(ConditionalOperator<?, ?> operator) {
			super(operator, 0);
		}

		@Override
		public void setValue(Object[] arguments, int argumentIndex) {
			// TRUE and FALSE don't set any value
			// moreover default behavior will throw exception due to argument position computation:
			// findByNameIsNull() has no arg => arguments is empty making arguments[0] throws an IndexOutOfBoundsException 
		}
	}
}
