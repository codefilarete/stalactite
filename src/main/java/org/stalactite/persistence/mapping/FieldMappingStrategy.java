package org.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

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
	
	private Field primaryKeyField;
	private Table targetTable;
	
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
			throw new UnsupportedOperationException("No primary key found for " + targetTable.getName());
		}
	}
	
	public Table getTargetTable() {
		return targetTable;
	}
	
	private void ensureFieldsAreAccessible() {
		for (Field field : fieldToColumn.keySet()) {
			field.setAccessible(true);
		}
	}
	
	@Override
	public PersistentValues getInsertValues(@Nonnull final T t) {
		return foreachField(new FieldVisitor() {
			@Override
			protected void visitField(Entry<Field, Column> fieldColumnEntry) throws IllegalAccessException {
				toReturn.putUpsertValue(fieldColumnEntry.getValue(), fieldColumnEntry.getKey().get(t));
			}
		});
	}
	
	@Override
	public PersistentValues getUpdateValues(@Nonnull final T modified, @Nonnull final T unmodified) {
		return foreachField(new FieldVisitor() {
			@Override
			protected void visitField(Entry<Field, Column> fieldColumnEntry) throws IllegalAccessException {
				Object modifiedValue = fieldColumnEntry.getKey().get(modified);
				Object unmodifiedValue = fieldColumnEntry.getKey().get(unmodified);
				if (!Objects.equalsWithNull(modifiedValue, unmodifiedValue)) {
					toReturn.putUpsertValue(fieldColumnEntry.getValue(), modifiedValue);
				}
				putVersionedKeyValues(unmodified, toReturn);
			}
		});
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
	
	private PersistentValues foreachField(final FieldVisitor visitor) {
		Iterables.visit(this.fieldToColumn.entrySet(), visitor);
		return visitor.toReturn;
	}
	
	protected void putVersionedKeyValues(T t, PersistentValues toReturn) {
		try {
			toReturn.putWhereValue(this.targetTable.getPrimaryKey(), primaryKeyField.get(t));
		} catch (IllegalAccessException e) {
			Exceptions.throwAsRuntimeException(e);
		}
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
