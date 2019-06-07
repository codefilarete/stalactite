package org.gama.stalactite.persistence.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.MemberDefinition;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelectExecutor;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.query.ValueAccessPointCriterion.LogicalOperator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractOperator;
import org.gama.stalactite.query.model.Query;

/**
 * @author Guillaume Mary
 */
public class EntityCriteriaSupport<C> implements EntityCriteria<C> {
	
	
	private final JoinedStrategiesSelectExecutor<C, ?, ?> joinedStrategiesSelectExecutor;
	/** Criteria, ClosedCriteria */
	private List<ValueAccessPointCriterion> conditions = new ArrayList<>();
	
	private Query query = new Query();
	
	private ValueAccessPointMap<Column> propertyToColumn;
	private Column primaryKey;
	
	
	private EntityCriteriaSupport(JoinedStrategiesSelectExecutor<C, ?, ?> joinedStrategiesSelectExecutor) {
		this.joinedStrategiesSelectExecutor = joinedStrategiesSelectExecutor;
		propertyToColumn = new ValueAccessPointMap<>(this.joinedStrategiesSelectExecutor.getMappingStrategy().getPropertyToColumn());
		// we add the identifier and primary key because they are not in the property mapping 
		IdMappingStrategy<C, ?> idMappingStrategy = this.joinedStrategiesSelectExecutor.getMappingStrategy().getIdMappingStrategy();
		if (idMappingStrategy instanceof SimpleIdMappingStrategy) {
			primaryKey = ((SimpleIdentifierAssembler) idMappingStrategy.getIdentifierAssembler()).getColumn();
			propertyToColumn.put(
					((SimpleIdMappingStrategy<C, ?>) idMappingStrategy).getIdAccessor().getIdAccessor(),
					primaryKey);
		}
		query.from(getMappingStrategy().getTargetTable());
		// NB : select(Collection) is not implemented
		propertyToColumn.values().forEach(query::select);
	}
	
	public <O, I, T extends Table> EntityCriteriaSupport(JoinedStrategiesSelectExecutor<C, I, T> joinedStrategiesSelectExecutor, SerializableFunction<C, O> getter, AbstractOperator<O> operand) {
		this(joinedStrategiesSelectExecutor);
		add(null, getter, operand);
	}
	
	public <O> EntityCriteriaSupport(JoinedStrategiesSelectExecutor<C, ?, ?> joinedStrategiesSelectExecutor, SerializableBiConsumer<C, O> setter, AbstractOperator<O> operand) {
		this(joinedStrategiesSelectExecutor);
		add(null, setter, operand);
	}
	
	private ClassMappingStrategy<C, ?, ?> getMappingStrategy() {
		return this.joinedStrategiesSelectExecutor.getMappingStrategy();
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator operator, SerializableFunction<C, O> getter, AbstractOperator<O> operand) {
		query.where(getColumn(getter), operand);
		conditions.add(new ValueAccessPointCriterion(operator, new AccessorByMethodReference<>(getter), operand));
		return this;
	}
	
	private <O> Column getColumn(SerializableFunction<C, O> getter) {
		Column column = propertyToColumn.get(new AccessorByMethodReference<>(getter));
		if (column == null) {
			throw new IllegalArgumentException("No column found for " + MemberDefinition.toString(new AccessorByMethodReference<>(getter)));
		}
		return column;
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator operator, SerializableBiConsumer<C, O> setter, AbstractOperator<O> operand) {
		query.where(getColumn(setter), operand);
		conditions.add(new ValueAccessPointCriterion(operator, new MutatorByMethodReference<>(setter), operand));
		return this;
	}
	
	private <O> Column getColumn(SerializableBiConsumer<C, O> setter) {
		Column column = propertyToColumn.get(new MutatorByMethodReference<>(setter));
		if (column == null) {
			throw new IllegalArgumentException("No column found for " + MemberDefinition.toString(new MutatorByMethodReference<>(setter)));
		}
		return column;
	}
	
	@Override
	public <O> EntityCriteria<C> and(SerializableFunction<C, O> getter, AbstractOperator<O> operand) {
		return add(LogicalOperator.AND, getter, operand);
	}
	
	@Override
	public <O> EntityCriteria<C> and(SerializableBiConsumer<C, O> setter, AbstractOperator<O> operand) {
		return add(LogicalOperator.AND, setter, operand);
	}
	
	@Override
	public <O> EntityCriteria<C> or(SerializableFunction<C, O> getter, AbstractOperator<O> operand) {
		return add(LogicalOperator.OR, getter, operand);
	}
	
	@Override
	public <O> EntityCriteria<C> or(SerializableBiConsumer<C, O> setter, AbstractOperator<O> operand) {
		return add(LogicalOperator.OR, setter, operand);
	}
	
//	@Override
//	public <A, B> EntityCriteria<C> and(SerializableBiConsumer<C, A> setter, SerializableFunction<A, B> getter, AbstractOperator<B> operand) {
//		ValueAccessPointMap<IEmbeddedBeanMappingStrategy<Object, ?>> mappingStrategies =
//				new ValueAccessPointMap<>(getMappingStrategy().getMappingStrategies());
//		
//		query.getFrom().innerJoin(primaryKey, mappingStrategies).getTargetTable());
//		return add(LogicalOperator.AND, setter, operand);
//	}
//	
	@Override
	public Iterator<ValueAccessPointCriterion> iterator() {
		return this.conditions.iterator();
	}
	
	@Override
	public String toString() {
		return conditions.toString();
	}
	
	public Query getQuery() {
		return query;
	}
}
