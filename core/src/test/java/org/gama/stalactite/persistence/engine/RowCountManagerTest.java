package org.gama.stalactite.persistence.engine;

import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.engine.RowCountManager.RowCounter;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.gama.stalactite.persistence.engine.RowCountManager.THROWING_ROW_COUNT_MANAGER;

/**
 * @author Guillaume Mary
 */
class RowCountManagerTest {
	
	@Test
	void checkRowCounter_add_duplicateProof() {
		Table toto = new Table("toto");
		Column colA = toto.addColumn("a", int.class);
		Column colB = toto.addColumn("b", int.class);
		
		RowCounter testInstance = new RowCounter();
		testInstance.add(Maps.forHashMap(Column.class, Object.class).add(colA, 1).add(colB, 2));
		testInstance.add(Maps.forHashMap(Column.class, Object.class).add(colA, 2).add(colB, 4));
		THROWING_ROW_COUNT_MANAGER.checkRowCount(2, testInstance);
		
		// we add a duplicate, row count should take it into account
		testInstance.add(Maps.forHashMap(Column.class, Object.class).add(colA, 2).add(colB, 4));
		THROWING_ROW_COUNT_MANAGER.checkRowCount(3, testInstance);
		
		// same with UpWhereColumn
		testInstance = new RowCounter();
		testInstance.add(Maps.forHashMap(UpwhereColumn.class, Object.class).add(new UpwhereColumn<>(colA, true), 1).add(new UpwhereColumn<>(colB, true), 2));
		testInstance.add(Maps.forHashMap(UpwhereColumn.class, Object.class).add(new UpwhereColumn<>(colA, true), 2).add(new UpwhereColumn<>(colB, true), 4));
		THROWING_ROW_COUNT_MANAGER.checkRowCount(2, testInstance);
		
		testInstance.add(Maps.forHashMap(UpwhereColumn.class, Object.class).add(new UpwhereColumn<>(colA, true), 2).add(new UpwhereColumn<>(colB, true), 4));
		THROWING_ROW_COUNT_MANAGER.checkRowCount(3, testInstance);
	}
}