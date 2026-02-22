package org.codefilarete.stalactite.query.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codefilarete.stalactite.query.api.CriteriaChain;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.tool.collection.Iterables;

/**
 * A basic implementation of {@link CriteriaChain}.
 * 
 * @author Guillaume Mary
 */
public class Criteria<SELF extends Criteria<SELF>> extends AbstractCriterion implements CriteriaChain<SELF> {
	
	/** Criteria, ClosedCriteria */
	protected List<AbstractCriterion> conditions = new ArrayList<>();

	public Criteria() {
	}

	public Criteria(Selectable<?> column, String condition) {
		add(new ColumnCriterion(column, condition));
	}
	
	public <O> Criteria(Selectable<O> column, ConditionalOperator<? super O, ?> condition) {
		add(new ColumnCriterion(column, condition));
	}
	
	public Criteria(Object ... columns) {
		add(new RawCriterion(columns));
	}

	public List<AbstractCriterion> getConditions() {
		return conditions;
	}
	
	/**
	 * Adds a criterion to this chain.
	 * 
	 * @param condition any criteria 
	 * @return this
	 */
	@Override
	public SELF add(AbstractCriterion condition) {
		this.conditions.add(condition);
		return (SELF) this;
	}
	
	@Override
	public SELF add(Iterable<AbstractCriterion> criteria) {
		criteria.forEach(this.conditions::add);
		return (SELF) this;
	}
	
	@Override
	public SELF add(LogicalOperator logicalOperator, Selectable<?> column, CharSequence condition) {
		return add(new ColumnCriterion(logicalOperator, column, condition));
	}
	
	@Override
	public SELF add(LogicalOperator logicalOperator, Selectable<?> column, ConditionalOperator<?, ?> condition) {
		return add(new ColumnCriterion(logicalOperator, column, condition));
	}
	
	@Override
	public SELF add(LogicalOperator logicalOperator, CriteriaChain<?> criteria) {
		Criteria toAdd;
		if (criteria instanceof Criteria) {
			toAdd = (Criteria) criteria;
		} else {
			toAdd = new Criteria(criteria);
		}
		toAdd.setOperator(logicalOperator);
		this.conditions.add(toAdd);
		return (SELF) this;
	}
	
	@Override
	public SELF add(LogicalOperator logicalOperator, Object... columns) {
		return add(new RawCriterion(logicalOperator, columns));
	}
	
	public Object remove(int index) {
		return this.conditions.remove(index);
	}
	
	public List<AbstractCriterion> clear() {
		List<AbstractCriterion> result = Iterables.copy(this.conditions);
		this.conditions.clear();
		return result;
	}
	
	@Override
	public Iterator<AbstractCriterion> iterator() {
		return this.conditions.iterator();
	}
}
