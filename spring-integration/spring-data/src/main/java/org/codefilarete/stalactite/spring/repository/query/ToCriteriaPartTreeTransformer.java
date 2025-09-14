package org.codefilarete.stalactite.spring.repository.query;

import java.util.Iterator;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.EntityPersister.LimitAware;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.model.LogicalOperator;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.repository.query.parser.PartTree.OrPart;

/**
 * Applies a {@link PartTree} to a {@link EntityCriteriaSupport} which is expected to come from either a {@link ProjectionQueryCriteriaSupport}
 * or a {@link org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport}
 *
 * @param <C>
 * @author Guillaume Mary
 */
public class ToCriteriaPartTreeTransformer<C> extends AbstractDerivedQuery<C> {
	
	private final OrderByChain<C, ?> orderByChain;
	private EntityCriteriaSupport<C> currentSupport;
	
	public ToCriteriaPartTreeTransformer(PartTree tree, Class<C> entityType, EntityCriteriaSupport<C> entityCriteriaSupport, OrderByChain<C, ?> orderByChain, LimitAware<?> limitAware) {
		super(entityType);
		this.currentSupport = entityCriteriaSupport;
		this.orderByChain = orderByChain;
		tree.forEach(this::append);
		if (tree.getSort().isSorted()) {
			appendSort(tree);
		}
		if (tree.getMaxResults() != null) {
			limitAware.limit(tree.getMaxResults());
		}
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
		Criterion criterion = append(part);
		this.currentSupport.add(orOrAnd, getter.getAccessors(), criterion.condition);
	}
}
