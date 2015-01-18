package org.stalactite.persistence.mapping;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class FieldMappingStrategy<T> implements IMappingStrategy<T> {
	
	private Map<Field, Column> fieldToColumn;
	
	private final Field primaryKeyField;
	
	private final Table targetTable;
	
	private final Set<Column> columns;
	
	public FieldMappingStrategy(@Nonnull Map<Field, Column> fieldToColumn) {
		this.fieldToColumn = fieldToColumn;
		ensureFieldsAreAccessible();
		this.targetTable = Iterables.firstValue(fieldToColumn).getTable();
		Entry<Field, Column> primaryKeyEntry = Iterables.filter(fieldToColumn.entrySet(), new Finder<Entry<Field, Column>>() {
			@Override
			public boolean accept(Entry<Field, Column> fieldColumnEntry) {
				return fieldColumnEntry.getValue().isPrimaryKey();
			}
		});
		if (primaryKeyEntry != null) {
			this.primaryKeyField = primaryKeyEntry.getKey();
		} else {
			throw new UnsupportedOperationException("No primary key field for " + targetTable.getName());
		}
		this.columns = new HashSet<>(fieldToColumn.values());
	}
	
	@Override
	public Table getTargetTable() {
		return targetTable;
	}
	
	@Override
	public Set<Column> getColumns() {
		return columns;
	}
	
	private void ensureFieldsAreAccessible() {
		for (Field field : fieldToColumn.keySet()) {
			Reflections.ensureAccessible(field);
		}
	}
	
	@Override
	public PersistentValues getInsertValues(@Nonnull final T t) {
		return foreachField(new FieldVisitor() {
			@Override
			protected void visitField(Entry<Field, Column> fieldColumnEntry) throws IllegalAccessException {
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
			protected void visitField(Entry<Field, Column> fieldColumnEntry) throws IllegalAccessException {
				Field field = fieldColumnEntry.getKey();
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
	public PersistentValues getSelectValues(@Nonnull T t) {
		PersistentValues toReturn = new PersistentValues();
		putVersionedKeyValues(t, toReturn);
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
			return (Serializable) primaryKeyField.get(t);
		} catch (IllegalAccessException e) {
			Exceptions.throwAsRuntimeException(e);
			// unjoinable code
			return null;
		}
	}
	
	private PersistentValues foreachField(final FieldVisitor visitor, boolean withPK) {
		Map<Field, Column> fieldsTobeVisited = new LinkedHashMap<>(this.fieldToColumn);
		if (!withPK) {
			fieldsTobeVisited.remove(this.primaryKeyField);
		}
		Iterables.visit(fieldsTobeVisited.entrySet(), visitor);
		return visitor.toReturn;
	}
	
	protected void putVersionedKeyValues(T t, PersistentValues toReturn) {
		toReturn.putWhereValue(this.targetTable.getPrimaryKey(), getId(t));
	}
	
	private static abstract class FieldVisitor extends ForEach<Entry<Field, Column>, Void> {
		
		protected PersistentValues toReturn = new PersistentValues();
		
		@Override
		public final Void visit(Entry<Field, Column> fieldColumnEntry) {
			try {
				visitField(fieldColumnEntry);
			} catch (IllegalAccessException e) {
				Exceptions.throwAsRuntimeException(e);
			}
			return null;
		}
		
		protected abstract void visitField(Entry<Field, Column> fieldColumnEntry) throws IllegalAccessException;
	}
}
