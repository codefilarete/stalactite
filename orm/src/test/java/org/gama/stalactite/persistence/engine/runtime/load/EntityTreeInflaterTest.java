package org.gama.stalactite.persistence.engine.runtime.load;

import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.runtime.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.gama.stalactite.persistence.engine.runtime.load.RelationJoinNode.BasicEntityCache;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IRowTransformer;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntityTreeInflaterTest {
	
	@Test
	void transform_doesntGoDeeperIfRelatedBeanIdIsNull() {
		
		Table leftTable = new Table("leftTable");
		Column leftTablePk = leftTable.addColumn("pk", long.class);
		
		Table rightTable = new Table("rightTable");
		Column rightTablePk = leftTable.addColumn("pk", long.class);
		Column rightTableFkToLeftTable = leftTable.addColumn("fkToLeftTable", long.class);
		
		Table rightMostTable = new Table("rightMostTable");
		Column rightMostTablePk = leftTable.addColumn("pk", long.class);
		Column rightMostTableFkToRightTable = leftTable.addColumn("fkToRightTable", long.class);
		
		EntityInflater leftEntityInflater = Mockito.mock(EntityInflater.class);
		// these lines are needed to trigger "main bean instance" creation
		when(leftEntityInflater.giveIdentifier(any(), any())).thenReturn(42);
		when(leftEntityInflater.getEntityType()).thenReturn(Object.class);
		IRowTransformer rightEntityBuilder = mock(IRowTransformer.class);
		when(rightEntityBuilder.transform(any())).thenReturn(new Object());
		when(leftEntityInflater.copyTransformerWithAliases(any())).thenReturn(rightEntityBuilder);
		
		// we create a second inflater to be related with first one, but we'll expect its transform() method not to be invoked
		EntityInflater rightEntityInflater = Mockito.mock(EntityInflater.class);
		when(rightEntityInflater.giveIdentifier(any(), any())).thenReturn(null);
		
		// relation fixer isn't expected to do anything because it won't be called (goal of the test)
		BeanRelationFixer relationFixer = Mockito.mock(BeanRelationFixer.class);
		
		// we create a last inflater to be related with the intermediary one (the right one),
		// but we'll expect none of its method to be invoked
		EntityInflater rightMostEntityInflater = Mockito.mock(EntityInflater.class);
		
		// composing entity tree : leftInflater gets a relation on rightInflater
		EntityJoinTree entityJoinTree = new EntityJoinTree<>(new JoinRoot<>(leftEntityInflater, leftTable));
		String joinName = entityJoinTree.addRelationJoin(
				EntityJoinTree.ROOT_STRATEGY_NAME,
				rightEntityInflater,
				leftTablePk,
				rightTableFkToLeftTable,
				JoinType.OUTER,
				relationFixer);
		entityJoinTree.addRelationJoin(
				joinName,
				rightMostEntityInflater,
				rightTablePk,
				rightMostTableFkToRightTable,
				JoinType.OUTER,
				Mockito.mock(BeanRelationFixer.class));
		
		EntityTreeQuery<Country> entityTreeQuery = new EntityTreeQueryBuilder<>(entityJoinTree).buildSelectQuery(new Dialect().getColumnBinderRegistry());
		EntityTreeInflater testInstance = new EntityTreeInflater<>(entityJoinTree, new ColumnedRow(entityTreeQuery.getColumnAliases()::get));
		
		
		Row databaseData = new Row()
				.add("leftTable_pk", 1)
				.add("rightTable_fkToLeftTable", null)
				.add("rightTable_pk", null)
				;
		testInstance.transform(databaseData, new BasicEntityCache());
		
		verify(rightEntityInflater, times(1)).giveIdentifier(any(), any());
		// because we returned null on giveIdentifier, the transformation algorithm shouldn't ask for relation appliance
		verify(relationFixer, times(0)).apply(any(), any());
		// and it shouldn't go deeper
		verify(rightMostEntityInflater, times(0)).giveIdentifier(any(), any());
	}
	
}