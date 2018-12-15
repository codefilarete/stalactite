package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.bean.Objects;
import org.gama.reflection.IMutator;
import org.gama.reflection.IReversibleAccessor;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class EmbeddedBeanMappingStrategy<C, T extends Table> implements IEmbeddedBeanMappingStrategy<C, T> {
	
	private final Map<IReversibleAccessor<C, Object>, Column<T, Object>> propertyToColumn;
	
	private final Set<Column<T, Object>> columns;
	
	private final ToBeanRowTransformer<C> rowTransformer;
	
	/**
	 * Columns (and their value provider) which are not officially mapped by a bean property.
	 * Those are for insertion time.
	 */
	private final List<Duo<Column<T, ?>, Function<C, ?>>> silentInsertedColumns = new ArrayList<>();
	
	/**
	 * Columns (and their value provider) which are not officially mapped by a bean property.
	 * Those are for update time.
	 */
	private final List<Duo<Column<T, ?>, Function<C, ?>>> silentUpdatedColumns = new ArrayList<>();
	
	/**
	 * Build a EmbeddedBeanMappingStrategy from a mapping between Field and Column.
	 * Fields are expected to be from same class.
	 * Columns are expected to be from same table.
	 * No control is done about that, caller must be aware of it.
	 * First entry of {@code propertyToColumn} is used to pick up persisted class and target table.
	 *
	 * @param targetClass the class to persist
	 * @param propertyToColumn a mapping between Field and Column, expected to be coherent (fields of same class, column of same table)
	 */
	public EmbeddedBeanMappingStrategy(Class<C> targetClass, Map<? extends IReversibleAccessor<C, Object>, Column<T, Object>> propertyToColumn) {
		this.propertyToColumn = new HashMap<>(propertyToColumn.size());
		Map<Column, IMutator> columnToField = new HashMap<>();
		for (Entry<? extends IReversibleAccessor<C, Object>, ? extends Column<T, Object>> fieldColumnEntry : propertyToColumn.entrySet()) {
			Column<T, Object> column = fieldColumnEntry.getValue();
			IReversibleAccessor<C, Object> accessorByField = fieldColumnEntry.getKey();
			// autoincrement and primary key columns mustn't be written (inserted nor updated) by this class
			if (!column.isAutoGenerated() && !column.isPrimaryKey()) {
				this.propertyToColumn.put(accessorByField, column);
			}
			columnToField.put(column, accessorByField.toMutator());
		}
		this.rowTransformer = new ToBeanRowTransformer<>(targetClass, columnToField);
		this.columns = new LinkedHashSet<>(propertyToColumn.values());
	}
	
	public Map<IReversibleAccessor<C, Object>, Column<T, Object>> getPropertyToColumn() {
		return propertyToColumn;
	}
	
	/**
	 * Gives mapped columns (can be a subset of the target table)
	 * @return target mapped columns
	 */
	@Nonnull
	@Override
	public Set<Column<T, Object>> getColumns() {
		return columns;
	}
	
	public ToBeanRowTransformer<C> getRowTransformer() {
		return rowTransformer;
	}
	
	@Override
	public <O> void addSilentColumnInserter(Column<T, O> column, Function<C, O> valueProvider) {
		silentInsertedColumns.add(new Duo<>(column, valueProvider));
	}
	
	@Override
	public <O> void addSilentColumnUpdater(Column<T, O> column, Function<C, O> valueProvider) {
		silentUpdatedColumns.add(new Duo<>(column, valueProvider));
	}
	
	@Nonnull
	@Override
	public Map<Column<T, Object>, Object> getInsertValues(C c) {
		return foreachField(new FieldVisitor<Column<T, Object>>() {
			@Override
			protected void visitField(Entry<IReversibleAccessor<C, Object>, Column<T, Object>> fieldColumnEntry) {
				toReturn.put(fieldColumnEntry.getValue(), fieldColumnEntry.getKey().get(c));
				silentInsertedColumns.forEach(columnFunctionDuo ->
						toReturn.put((Column<T, Object>) columnFunctionDuo.getLeft(), columnFunctionDuo.getRight().apply(c)));
			}
		});
	}
	
	@Nonnull
	@Override
	public Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<Column<T, Object>, Object> unmodifiedColumns = new HashMap<>();
		// getting differences
		Map<UpwhereColumn<T>, Object> modifiedFields = foreachField(new FieldVisitor<UpwhereColumn<T>>() {
			@Override
			protected void visitField(Entry<IReversibleAccessor<C, Object>, Column<T, Object>> fieldColumnEntry) {
				IReversibleAccessor<C, Object> accessor = fieldColumnEntry.getKey();
				Object modifiedValue = accessor.get(modified);
				Object unmodifiedValue = unmodified == null ? null : accessor.get(unmodified);
				Column<T, Object> fieldColumn = fieldColumnEntry.getValue();
				if (!Objects.equalsWithNull(modifiedValue, unmodifiedValue)
						// OR is here to take cases where getUpdateValues(..) gets only "modified" parameter (such as for updateById) and
						// some modified properties are null, without this OR such properties won't be updated (set to null)
						// and, overall, if they are all null, modifiedFields is empty then causing a statement without values 
						|| unmodified == null) {
					toReturn.put(new UpwhereColumn<>(fieldColumn, true), modifiedValue);
				} else {
					unmodifiedColumns.put(fieldColumn, modifiedValue);
				}
			}
		});
		
		// adding complementary columns if necessary
		if (!modifiedFields.isEmpty() && allColumns) {
			for (Entry<Column<T, Object>, Object> unmodifiedField : unmodifiedColumns.entrySet()) {
				modifiedFields.put(new UpwhereColumn<>(unmodifiedField.getKey(), true), unmodifiedField.getValue());
			}
		}
		// getting values for silent columns
		silentUpdatedColumns.forEach(columnFunctionDuo -> {
			Object modifiedValue = columnFunctionDuo.getRight().apply(modified);
			modifiedFields.put(new UpwhereColumn<>(columnFunctionDuo.getLeft(), true), modifiedValue);
		});
		return modifiedFields;
	}
	
	private <K> Map<K, Object> foreachField(FieldVisitor<K> visitor) {
		this.propertyToColumn.entrySet().forEach(visitor::visitField);
		return visitor.toReturn;
	}
	
	@Override
	public C transform(Row row) {
		return this.rowTransformer.transform(row);
	}
	
	private abstract class FieldVisitor<K> implements Consumer<Entry<IReversibleAccessor<C, Object>, Column<T, Object>>> {
		
		protected Map<K, Object> toReturn = new HashMap<>();
		
		@Override
		public final void accept(Entry<IReversibleAccessor<C, Object>, Column<T, Object>> fieldColumnEntry) {
			visitField(fieldColumnEntry);
		}

		protected abstract void visitField(Entry<IReversibleAccessor<C, Object>, Column<T, Object>> fieldColumnEntry);
	}
	
}
