package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A contract that allow to decide what happens when expected and effective row count doesn't match on update and delete statement.
 * Primarly thought to be used with optimistic lock, but may be used even on non versioned entities.
 * 
 * @author Guillaume Mary
 */
public interface RowCountManager {
	
	/** {@link RowCountManager} that will do nothing during check, created for testing purpose */
	RowCountManager NOOP_ROW_COUNT_MANAGER = (expectedRowCount, rowCount) -> { /* Nothing is done */ };
	
	/** {@link RowCountManager} that will throw a {@link StaleObjectExcepion} during check if expected and effective row count doesn't match */
	RowCountManager THROWING_ROW_COUNT_MANAGER = (expectedRowCount, rowCount) -> {
		if (expectedRowCount.size() != rowCount) {
			// row count miss => we throw an exception
			throw new StaleObjectExcepion(expectedRowCount.size(), rowCount);
		}
	};
	
	void checkRowCount(RowCounter expectedRowCount, int rowCount);
	
	/** A basic register for row update or delete */
	class RowCounter {
		
		/**
		 * All values of SQL statement.
		 * Not crucial but could be usefull for future features needing touched columns or debugging purpose.
		 */
		private final List<Map<? /* UpwhereColumn or Column */, Object>> rowValues = new ArrayList<>();
		
		public void add(Map<? /* UpwhereColumn or Column */, Object> updateValues) {
			this.rowValues.add(updateValues);
		}
		
		public int size() {
			return rowValues.size();
		}
	}
}
