package org.codefilarete.stalactite.sql.ddl.structure;

import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Representation of a database key such as Primary Key and Foreign Key, or columns of a join.
 * Can be composed of a single column or multiple ones as in composite key.
 * 
 * @param <T> table owning the key, can also be a sub-query
 * @param <ID> object type composed by key columns, left unused here because no method refers to id
 * @author Guillaume Mary
 */
public interface Key<T extends Fromable, ID /* unused in this class, left for clarity in all code using Key */> {
	
	static <T extends Fromable, ID> KeyBuilder<T, ID> from(T table) {
		return new KeyBuilder<>(table);
	}
	
	static <T extends Fromable, ID> Key<T, ID> ofSingleColumn(JoinLink<T, ID> column) {
		return new KeySupport<T, ID>(column.getOwner(), new KeepOrderSet<>(column)) {
			@Override
			public boolean isComposed() {
				return false;
			}
		};
	}
	
	T getTable();
	
	KeepOrderSet<? extends JoinLink<T, ?>> getColumns();
	
	boolean isComposed();
	
	class KeyBuilder<T extends Fromable, ID> {
		
		private final KeySupport<T, ID> keySupport;
		
		private KeyBuilder(T table) {
			keySupport = new KeySupport<>(table);
		}
		
		public KeyBuilder<T, ID> addColumn(JoinLink<T, ?> column) {
			this.keySupport.addColumn(column);
			return this;
		}
		
		public Key<T, ID> build() {
			return keySupport;
		}
	}
	
	class KeySupport<T extends Fromable, ID> implements Key<T, ID> {
		
		private final T table;
		private final KeepOrderSet<JoinLink<T, ?>> columns;
		
		private KeySupport(T table) {
			this(table, new KeepOrderSet<>());
		}
		
		private KeySupport(T table, KeepOrderSet<? extends JoinLink<T, ?>> columns) {
			this.table = table;
			this.columns = (KeepOrderSet<JoinLink<T, ?>>) columns;
		}
		
		@Override
		public T getTable() {
			return table;
		}
		
		@Override
		public KeepOrderSet<JoinLink<T, ?>> getColumns() {
			return columns;
		}
		
		private void addColumn(JoinLink<T, ?> column) {
			this.columns.add((JoinLink<T, Object>) column);
		}
		
		@Override
		public boolean isComposed() {
			return columns.size() == 1;
		}
	}
}
