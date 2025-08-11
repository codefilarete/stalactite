package org.codefilarete.stalactite.query.model;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.collection.Iterables;

/**
 * A basic implementation of {@link CriteriaChain}.
 * 
 * @author Guillaume Mary
 */
public class Criteria<SELF extends Criteria<SELF>> extends AbstractCriterion implements CriteriaChain<SELF> {
	
	/**
	 * Replaces all {@link Column}s in given condition (source) by its match in <code>columnClones</code>.
	 * This is done by cloning {@link ColumnCriterion} in source, and then pushed them in <code>target</code>.
	 * 
	 * Made to handle {@link Column} clones and aliases at query time.
	 * 
	 * @param source container of {@link Column} that must be replaced by the ones in columnClones
	 * @param target container where to push the criteria clones
	 * @param columnClones mapping between use columns and the ones of the query (the one with aliases), usually a {@link IdentityHashMap}
	 */
	// TODO: to remove ? seems to be merely used or in cycle
	public static void copy(Iterable<AbstractCriterion> source, CriteriaChain target, Function<Selectable<?>, Selectable<?>> columnClones) {
		source.forEach(criterion -> {
			if (criterion instanceof ColumnCriterion) {
				ColumnCriterion columnCriterion = (ColumnCriterion) criterion;
				target.add(criterion.getOperator(), columnCriterion.copyFor(columnClones.apply(columnCriterion.getColumn())));
			} else if (criterion instanceof Criteria) {
				target.add(criterion.getOperator(), ((Criteria<?>) criterion).copyFor(columnClones));
			} else {
				target.add(criterion);
			}
		});
	}
	
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
			toAdd = new Criteria();
			copy(criteria, toAdd, Function.identity());
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
	
	public Criteria copyFor(Function<Selectable<?>, Selectable<?>> columnClones) {
		Criteria<SELF> result = new Criteria<>();
		result.setOperator(this.getOperator());
		copy(conditions, result, columnClones);
		return result;
	}
}
