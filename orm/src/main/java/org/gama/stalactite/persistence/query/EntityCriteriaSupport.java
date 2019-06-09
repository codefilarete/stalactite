package org.gama.stalactite.persistence.query;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.MemberDefinition;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.query.ValueAccessPointCriterion.LogicalOperator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.AbstractOperator;
import org.gama.stalactite.query.model.Criteria;
import org.gama.stalactite.query.model.CriteriaChain;

/**
 * @author Guillaume Mary
 */
public class EntityCriteriaSupport<C> implements EntityCriteria<C> {
	
	private Criteria query = new Criteria();
	
	private ValueAccessPointMap<Column> propertyToColumn;
	
	public <O> EntityCriteriaSupport(ClassMappingStrategy<C, ?, ?> mappingStrategy, SerializableFunction<C, O> getter, AbstractOperator<O> operand) {
		this(mappingStrategy);
		add(null, getter, operand);
	}
	
	public <O> EntityCriteriaSupport(ClassMappingStrategy<C, ?, ?> mappingStrategy, SerializableBiConsumer<C, O> setter, AbstractOperator<O> operand) {
		this(mappingStrategy);
		add(null, setter, operand);
	}
	
	private EntityCriteriaSupport(ClassMappingStrategy<C, ?, ?> mappingStrategy) {
		propertyToColumn = new ValueAccessPointMap<>(mappingStrategy.getPropertyToColumn());
		// we add the identifier and primary key because they are not in the property mapping 
		IdMappingStrategy<C, ?> idMappingStrategy = mappingStrategy.getIdMappingStrategy();
		if (idMappingStrategy instanceof SimpleIdMappingStrategy) {
			Column primaryKey = ((SimpleIdentifierAssembler) idMappingStrategy.getIdentifierAssembler()).getColumn();
			propertyToColumn.put(((SimpleIdMappingStrategy<C, ?>) idMappingStrategy).getIdAccessor().getIdAccessor(), primaryKey);
		}
		mappingStrategy.getMappingStrategies().forEach((k, v) -> {
			v.getPropertyToColumn().forEach((p, c) -> {
				propertyToColumn.put(new AccessorChain<>(k, p), c);
			});
		});
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator operator, SerializableFunction<C, O> getter, AbstractOperator<O> operand) {
		if (operator == LogicalOperator.OR) {
			query.or(getColumn(getter), operand);
		} else {
			query.and(getColumn(getter), operand);
		}
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
		if (operator == LogicalOperator.OR) {
			query.or(getColumn(setter), operand);
		} else {
			query.and(getColumn(setter), operand);
		}
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
	
	@Override
	public <A, B> EntityCriteria<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, AbstractOperator<B> operand) {
		query.and(propertyToColumn.get(new AccessorChain<>(new AccessorByMethodReference<>(getter1), new AccessorByMethodReference<>(getter2))), operand);
		return this;
	}
	
	public CriteriaChain getQuery() {
		return query;
	}
}
