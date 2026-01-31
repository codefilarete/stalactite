package org.codefilarete.stalactite.engine.runtime.projection;

import org.codefilarete.reflection.*;
import org.codefilarete.stalactite.engine.EntityCriteria;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

import java.util.List;

/**
 * Simple class that stores options of the query
 *
 * @author Guillaume Mary
 */
public class ProjectionQueryPageSupport<C>
		implements EntityCriteria.OrderByChain<C, ProjectionQueryPageSupport<C>>, EntityCriteria.LimitAware<ProjectionQueryPageSupport<C>> {
	
	private boolean distinct;
	private Limit limit;
	private final KeepOrderSet<OrderByItem> orderBy = new KeepOrderSet<>();
	
	
	public boolean isDistinct() {
		return distinct;
	}
	
	void distinct() {
		distinct = true;
	}
	
	public Limit getLimit() {
		return limit;
	}
	
	public KeepOrderSet<OrderByItem> getOrderBy() {
		return orderBy;
	}
	
	@Override
	public ProjectionQueryPageSupport<C> limit(int count) {
		limit = new Limit(count);
		return this;
	}
	
	@Override
	public ProjectionQueryPageSupport<C> limit(int count, Integer offset) {
		limit = new Limit(count, offset);
		return this;
	}
	
	@Override
	public ProjectionQueryPageSupport<C> orderBy(SerializableFunction<C, ?> getter, Order order) {
		orderBy.add(new OrderByItem(Arrays.asList(new AccessorByMethodReference<>(getter)), order, false));
		return this;
	}
	
	@Override
	public ProjectionQueryPageSupport<C> orderBy(SerializableBiConsumer<C, ?> setter, Order order) {
		orderBy.add(new OrderByItem(Arrays.asList(new MutatorByMethodReference<>(setter)), order, false));
		return this;
	}
	
	@Override
	public ProjectionQueryPageSupport<C> orderBy(AccessorChain<C, ?> getter, Order order) {
		orderBy.add(new OrderByItem(getter.getAccessors(), order, false));
		return this;
	}
	
	@Override
	public ProjectionQueryPageSupport<C> orderBy(AccessorChain<C, ?> getter, Order order, boolean ignoreCase) {
		orderBy.add(new OrderByItem(getter.getAccessors(), order, ignoreCase));
		getter.getAccessors().forEach(accessor -> assertAccessorIsNotIterable(accessor, AccessorDefinition.giveDefinition(accessor).getMemberType()));
		return this;
	}
	
	private void assertAccessorIsNotIterable(ValueAccessPoint valueAccessPoint, Class memberType) {
		if (Iterable.class.isAssignableFrom(memberType)) {
			throw new IllegalArgumentException("OrderBy clause on a Collection property is unsupported due to eventual inconsistency"
					+ " with Collection nature : "
					+ (valueAccessPoint instanceof AbstractReflector
					? ((AbstractReflector<?>) valueAccessPoint).getDescription()
					: AccessorDefinition.giveDefinition(valueAccessPoint)).toString());
		}
	}
	
	/**
	 * Creates a copy of this instance by merging its options with another.
	 * Made to handle Spring Data's different ways of sorting (should have been put closer to its usage, but was too complex)
	 *
	 * @param other some other paging options
	 * @return a merge of this instance with given one
	 */
	ProjectionQueryPageSupport<C> merge(ProjectionQueryPageSupport<C> other) {
		ProjectionQueryPageSupport<C> duplicate = new ProjectionQueryPageSupport<>();
		// applying this instance's limit and orderBy options
		if (this.getLimit() != null) {
			duplicate.limit(this.getLimit().getCount(), this.getLimit().getOffset());
		}
		duplicate.orderBy.addAll(this.orderBy);
		// adding other instance's limit and orderBy options (may overwrite info, but that's user responsibility, we can't do anything smart here)
		if (other.getLimit() != null) {
			duplicate.limit(other.getLimit().getCount(), other.getLimit().getOffset());
		}
		duplicate.orderBy.addAll(other.orderBy);
		return duplicate;
	}
	
	public static class OrderByItem {
		
		private final List<? extends ValueAccessPoint<?>> property;
		private final Order direction;
		private final boolean ignoreCase;
		
		public OrderByItem(List<? extends ValueAccessPoint<?>> property, Order direction, boolean ignoreCase) {
			this.property = property;
			this.direction = direction;
			this.ignoreCase = ignoreCase;
		}
		
		public List<? extends ValueAccessPoint<?>> getProperty() {
			return property;
		}
		
		public Order getDirection() {
			return direction;
		}
		
		public boolean isIgnoreCase() {
			return ignoreCase;
		}
	}
}
