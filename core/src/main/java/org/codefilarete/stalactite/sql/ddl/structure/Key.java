package org.codefilarete.stalactite.sql.ddl.structure;

import java.util.Collection;

import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Representation of a database key such as Primary Key and Foreign Key, or columns of a join.
 * Can be composed of a single column or multiple ones as in a composite key.
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
	
	<J extends JoinLink<T, ?>> KeepOrderSet<J> getColumns();
	
	boolean isComposed();
	
	default <TARGETTABLE extends Fromable> KeyMapping<T, TARGETTABLE, ID> reference(Key<TARGETTABLE, ID> targetKey) {
		return new KeyMapping<>(this, targetKey);
	}
	
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
		
		public void addAllColumns(Collection<? extends JoinLink<T, ?>> columns) {
			this.keySupport.addAllColumns(columns);
		}
	}
	
	class KeySupport<T extends Fromable, ID> implements Key<T, ID> {
		
		private final T table;
		private final KeepOrderSet<JoinLink<T, ?>> columns;
		
		// left private (as addColumn(..) and addAllColumns(..)) to make it only available from the builder
		private KeySupport(T table) {
			this(table, new KeepOrderSet<>());
		}
		
		public KeySupport(T table, KeepOrderSet<? extends JoinLink<T, ?>> columns) {
			this.table = table;
			this.columns = (KeepOrderSet<JoinLink<T, ?>>) columns;
		}
		
		public KeySupport(KeepOrderSet<? extends JoinLink<T, ?>> columns) {
			this(first(columns).getOwner(), columns);
		}
		
		@Override
		public T getTable() {
			return table;
		}
		
		@Override
		public KeepOrderSet<JoinLink<T, ?>> getColumns() {
			return columns;
		}
		
		// left private (as constructor with Table argument) to make it only available from the builder
		private void addColumn(JoinLink<T, ?> column) {
			this.columns.add(column);
		}
		
		// left private (as constructor with Table argument) to make it only available from the builder
		private void addAllColumns(Collection<? extends JoinLink<T, ?>> columns) {
			this.columns.addAll(columns);
		}
		
		@Override
		public boolean isComposed() {
			return columns.size() > 1;
		}
	}
}
