package org.stalactite.persistence.mapping;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import org.stalactite.lang.Reflections;
import org.stalactite.lang.bean.Objects;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.Finder;
import org.stalactite.lang.collection.Iterables.ForEach;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.sql.result.Row;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.reflection.AccessorByField;

/**
 * @author mary
 */
public class FieldMappingStrategy<T> implements IMappingStrategy<T> {
	
	private final Class<T> classToPersist;
	
	private final Map<AccessorByField, Column> fieldToColumn;
	
	private final AccessorByField<T, Serializable> primaryKeyField;
	
	private final Table targetTable;
	
	private final Set<Column> columns;
	
	private final ToBeanRowTransformer<T> rowTransformer;
	
	/**
	 * Build a FieldMappingStrategy from a mapping between Field and Column.
	 * Fields are expected to be from same class.
	 * Columns are expected to be from same table.
	 * No control is done about that, caller must be aware of it.
	 * First entry of <tt>fieldToColumn</tt> is used to pick up persisted class and target table.
	 * 
	 * @param fieldToColumn a mapping between Field and Column, expected to be coherent (fields of same class, column of same table)
	 */
	public FieldMappingStrategy(@Nonnull Map<Field, Column> fieldToColumn) {
		this.fieldToColumn = new LinkedHashMap<>(fieldToColumn.size());
		Map<String, AccessorByField> columnToField = new HashMap<>();
		for (Entry<Field, Column> fieldColumnEntry : fieldToColumn.entrySet()) {
			Column column = fieldColumnEntry.getValue();
			AccessorByField accessorByField = new AccessorByField(fieldColumnEntry.getKey());
			this.fieldToColumn.put(accessorByField, column);
			columnToField.put(column.getName(), accessorByField);
		}
		Entry<Field, Column> firstEntry = Iterables.first(fieldToColumn);
		this.targetTable = firstEntry.getValue().getTable();
		this.classToPersist = (Class<T>) firstEntry.getKey().getDeclaringClass();
		this.rowTransformer = new ToBeanRowTransformer<>(Reflections.getDefaultConstructor(classToPersist), columnToField);
		
		Entry<AccessorByField, Column> primaryKeyEntry = Iterables.filter(this.fieldToColumn.entrySet(), new Finder<Entry<AccessorByField, Column>>() {
			@Override
			public boolean accept(Entry<AccessorByField, Column> fieldColumnEntry) {
				return fieldColumnEntry.getValue().isPrimaryKey();
			}
		});
		if (primaryKeyEntry != null) {
			this.primaryKeyField = primaryKeyEntry.getKey();
		} else {
			throw new UnsupportedOperationException("No primary key field for " + targetTable.getName());
		}
		this.columns = new LinkedHashSet<>(fieldToColumn.values());
	}
	
	@Override
	public Table getTargetTable() {
		return targetTable;
	}
	
	@Override
	public Set<Column> getColumns() {
		return columns;
	}
	
	@Override
	public PersistentValues getInsertValues(@Nonnull final T t) {
		return foreachField(new FieldVisitor() {
			@Override
			protected void visitField(Entry<AccessorByField, Column> fieldColumnEntry) throws IllegalAccessException {
				toReturn.putUpsertValue(fieldColumnEntry.getValue(), fieldColumnEntry.getKey().get(t));
			}
		}, true);	// primary key must be inserted (not if DB gives it but it's not implemented yet)
	}
	
	@Override
	public PersistentValues getUpdateValues(@Nonnull final T modified, final T unmodified, final boolean allColumns) {
		final Map<Column, Object> unmodifiedColumns = new LinkedHashMap<>();
		// getting differences
		PersistentValues toReturn = foreachField(new FieldVisitor() {
			@Override
			protected void visitField(Entry<AccessorByField, Column> fieldColumnEntry) throws IllegalAccessException {
				AccessorByField<T, Object> field = fieldColumnEntry.getKey();
				Object modifiedValue = field.get(modified);
				Object unmodifiedValue = unmodified == null ? null : field.get(unmodified);
				Column fieldColumn = fieldColumnEntry.getValue();
				if (!Objects.equalsWithNull(modifiedValue, unmodifiedValue)) {
					toReturn.putUpsertValue(fieldColumn, modifiedValue);
				} else {
					unmodifiedColumns.put(fieldColumn, modifiedValue);
				}
			}
		}, false);	// primary key mustn't be updated
		
		// adding complementary columns if necessary
		if (allColumns && !toReturn.getUpsertValues().isEmpty()) {
			for (Entry<Column, Object> unmodifiedField : unmodifiedColumns.entrySet()) {
				toReturn.putUpsertValue(unmodifiedField.getKey(), unmodifiedField.getValue());
			}
		}
		
		putVersionedKeyValues(modified, toReturn);
		return toReturn;
	}
	
	@Override
	public PersistentValues getDeleteValues(@Nonnull T t) {
		PersistentValues toReturn = new PersistentValues();
		putVersionedKeyValues(t, toReturn);
		return toReturn;
	}
	
	@Override
	public PersistentValues getSelectValues(@Nonnull Serializable id) {
		PersistentValues toReturn = new PersistentValues();
		toReturn.putWhereValue(this.targetTable.getPrimaryKey(), id);
		return toReturn;
	}
	
	@Override
	public PersistentValues getVersionedKeyValues(@Nonnull T t) {
		PersistentValues toReturn = new PersistentValues();
		putVersionedKeyValues(t, toReturn);
		return toReturn;
	}
	
	@Override
	public Serializable getId(T t) {
		try {
			return primaryKeyField.get(t);
		} catch (IllegalAccessException e) {
			Exceptions.throwAsRuntimeException(e);
			// unjoinable code
			return null;
		}
	}
	
	@Override
	public void setId(T t, Serializable identifier) {
		try {
			primaryKeyField.set(t, identifier);
		} catch (IllegalAccessException e) {
			Exceptions.throwAsRuntimeException(e);
		}
	}
	
	private PersistentValues foreachField(final FieldVisitor visitor, boolean withPK) {
		Map<AccessorByField, Column> fieldsTobeVisited = new LinkedHashMap<>(this.fieldToColumn);
		if (!withPK) {
			fieldsTobeVisited.remove(this.primaryKeyField);
		}
		Iterables.visit(fieldsTobeVisited.entrySet(), visitor);
		return visitor.toReturn;
	}
	
	protected void putVersionedKeyValues(T t, PersistentValues toReturn) {
		toReturn.putWhereValue(this.targetTable.getPrimaryKey(), getId(t));
	}
	
	@Override
	public T transform(Row row) {
		return this.rowTransformer.transform(row);
	}
	
	private static abstract class FieldVisitor extends ForEach<Entry<AccessorByField, Column>, Void> {
		
		protected PersistentValues toReturn = new PersistentValues();
		
		@Override
		public final Void visit(Entry<AccessorByField, Column> fieldColumnEntry) {
			try {
				visitField(fieldColumnEntry);
			} catch (IllegalAccessException e) {
				Exceptions.throwAsRuntimeException(e);
			}
			return null;
		}
		
		protected abstract void visitField(Entry<AccessorByField, Column> fieldColumnEntry) throws IllegalAccessException;
	}
}
