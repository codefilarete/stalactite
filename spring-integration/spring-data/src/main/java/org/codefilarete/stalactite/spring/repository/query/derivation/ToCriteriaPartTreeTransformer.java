package org.codefilarete.stalactite.spring.repository.query.derivation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.EntityPersister.LimitAware;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.query.model.LogicalOperator;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.repository.query.parser.PartTree.OrPart;

/**
 * Applies a {@link PartTree} to a {@link EntityCriteriaSupport} which is expected to come from either a
 * {@link ProjectionQueryCriteriaSupport} or a
 * {@link EntityQueryCriteriaSupport}
 *
 * @param <C>
 * @author Guillaume Mary
 */
public class ToCriteriaPartTreeTransformer<C> extends AbstractDerivedQuery<C> {
	
	private final PartTree partTree;
	
	public ToCriteriaPartTreeTransformer(PartTree partTree, Class<C> entityType) {
		super(entityType);
		this.partTree = partTree;
	}
	
	public Condition applyTo(EntityCriteriaSupport<C> entityCriteriaSupport, OrderByChain<C, ?> orderByChain, LimitAware<?> limitAware) {
		Processor processor = new Processor(entityCriteriaSupport, orderByChain);
		partTree.forEach(processor::append);
		if (partTree.getSort().isSorted()) {
			processor.appendSort(partTree);
		}
		if (partTree.getMaxResults() != null) {
			limitAware.limit(partTree.getMaxResults());
		}
		return new Condition(processor.conditions);
	}
	
	public static class Condition {
		
		private final List<Criterion> condition;
		
		public Condition(List<Criterion> condition) {
			this.condition = condition;
		}
		
		public void consume(Object[] arguments) {
			int argumentIndex = 0;
			for (Criterion criterion : condition) {
				criterion.setValue(arguments, argumentIndex);
				argumentIndex += criterion.argumentCount;
			}
		}
	}
	
	private class Processor {
		
		private EntityCriteriaSupport<C> currentSupport;
		private final OrderByChain<C, ?> orderByChain;
		private final List<Criterion> conditions = new ArrayList<>();
		
		public Processor(EntityCriteriaSupport<C> currentSupport, OrderByChain<C, ?> orderByChain) {
			this.orderByChain = orderByChain;
			this.currentSupport = currentSupport;
		}
		
		private void appendSort(PartTree tree) {
			tree.getSort().iterator().forEachRemaining(order -> {
				AccessorChain<C, Object> orderProperty = convertToAccessorChain(order);
				orderByChain.orderBy(orderProperty, order.getDirection() == Direction.ASC ? Order.ASC : Order.DESC, order.isIgnoreCase());
			});
		}
		
		private void append(OrPart part) {
			boolean nested = false;
			if (part.stream().count() > 1) {    // "if" made to avoid extra parenthesis (can be considered superfluous)
				nested = true;
				this.currentSupport = currentSupport.beginNested();
			}
			Iterator<Part> iterator = part.iterator();
			if (iterator.hasNext()) {
				append(iterator.next(), LogicalOperator.OR);
			}
			iterator.forEachRemaining(p -> this.append(p, LogicalOperator.AND));
			if (nested) {    // "if" made to avoid extra parenthesis (can be considered superfluous)
				this.currentSupport = currentSupport.endNested();
			}
		}
		
		private void append(Part part, LogicalOperator orOrAnd) {
			AccessorChain<C, Object> getter = convertToAccessorChain(part.getProperty());
			Criterion criterion = convertToCriterion(part);
			this.currentSupport.add(orOrAnd, getter.getAccessors(), criterion.condition);
			this.conditions.add(criterion);
		}
	}
}