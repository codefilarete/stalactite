package org.codefilarete.stalactite.engine.listener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codefilarete.tool.Duo;
import org.codefilarete.stalactite.mapping.Mapping;
import org.codefilarete.stalactite.mapping.Mapping.UpwhereColumn;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface UpdateListener<C> {
	
	default void beforeUpdate(Iterable<? extends Duo<? extends C, ? extends C>> payloads, boolean allColumnsStatement) {
		
	}
	
	default void afterUpdate(Iterable<? extends Duo<? extends C, ? extends C>> entities, boolean allColumnsStatement) {
		
	}
	
	default void onUpdateError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		
	}
	
	/**
	 * Payload for listener's methods. Carries modified + unmodified entities and the columns + values
	 * which must be updated.
	 * Not expected to be used elsewhere than the {@link UpdateListener} mechanism.
	 * 
	 * @param <C> entities type
	 * @param <T> target table type
	 */
	class UpdatePayload<C, T extends Table<T>> {
		private final Duo<? extends C, ? extends C> entities;
		
		private final Map<UpwhereColumn<T>, Object> values;
		
		public UpdatePayload(Duo<? extends C, ? extends C> entities, Map<UpwhereColumn<T>, Object> values) {
			this.entities = entities;
			this.values = values;
		}
		
		/**
		 * Gives the tuple of modified (left) and unmodified (right) instances
		 * 
		 * @return constructor argument
		 */
		public Duo<C, C> getEntities() {
			return (Duo<C, C>) entities;
		}
		
		/**
		 * Gives the map of columns and values which must be updated in database
		 * 
		 * @return constructor argument
		 */
		public Map<UpwhereColumn<T>, Object> getValues() {
			return values;
		}
	}
	
	/**
	 * Default behavior for giving {@link UpdatePayload} from some entities and a {@link Mapping}.
	 * 
	 * @param entities modified + unmodified entities
	 * @param allColumns indicates if all columns must be updated or not
	 * @param mapping the strategy that will compute updatable columns and values from modified + unmodified entities 
	 * @param <C> entities type
	 * @param <T> target table type
	 * @return arguments wrapped into an {@link UpdatePayload}, enhanced with updatable columns and values
	 */
	static <C, T extends Table<T>> Iterable<UpdatePayload<C, T>> computePayloads(Iterable<? extends Duo<C, C>> entities,
																				 boolean allColumns,
																				 Mapping<C, T> mapping) {
		return computePayloads(entities, allColumns, mapping::getUpdateValues);
	}
	
	static <C, T extends Table<T>> Iterable<UpdatePayload<C, T>> computePayloads(Iterable<? extends Duo<C, C>> entities,
																				 boolean allColumns,
																				 UpdateValuesProvider<C, T> mappingStrategy) {
		List<UpdatePayload<C, T>> result = new ArrayList<>();
		for (Duo<? extends C, ? extends C> next : entities) {
			C modified = next.getLeft();
			C unmodified = next.getRight();
			// finding differences between modified instances and unmodified ones
			Map<UpwhereColumn<T>, Object> updateValues = mappingStrategy.getUpdateValues(modified, unmodified, allColumns);
			UpdatePayload<C, T> payload = new UpdatePayload<>(next, updateValues);
			result.add(payload);
		}
		return result;
	}
	
	interface UpdateValuesProvider<C, T extends Table<T>> {
		Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns);
	}
	
}
