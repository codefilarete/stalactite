package org.codefilarete.stalactite.sql.ddl.structure;

import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Foreign key between tables
 * 
 * @param <T> the table owning this foreign key
 * @param <U> the table referenced by this foreign key
 * @param <ID> the type of both keys
 * @author Guillaume Mary
 */
public class ForeignKey<T extends Table<T>, U extends Table<U>, ID> extends KeyMapping<T, U, ID> {
	
	private final String name;
	
	public <I> ForeignKey(String name, Column<T, I> column, Column<U, I> targetColumn) {
		this(name, new KeepOrderSet<>(column), new KeepOrderSet<>(targetColumn));
	}
	
	@SuppressWarnings("squid:S1319")	// wanted : we want to show that we must keep set order
	public ForeignKey(String name, KeepOrderSet<Column<T, ?>> columns, KeepOrderSet<? extends Column<U, ?>> targetColumns) {
		this(name, new KeySupport<>(first(columns).getTable(), columns), new KeySupport<>(first(targetColumns).getTable(), targetColumns));
	}
	
	public ForeignKey(String name, Key<T, ID> columns, Key<U, ID> targetColumns) {
		super(columns, targetColumns);
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	/**
	 * Returns the source columns of this foreign key.
	 * Same as {@link #getLeftColumns()}
	 * 
	 * @return the source columns of this foreign key
	 */
	@Override
	public KeepOrderSet<Column<T, ?>> getColumns() {
		return getLeftColumns();
	}
	
	/**
	 * Returns the target table of this foreign key.
	 * Same as {@link #getRightKey()}
	 * 
	 * @return the target table of this foreign key
	 */
	public U getTargetTable() {
		return getRightKey().getTable();
	}
	
	/**
	 * Returns the target columns of this foreign key.
	 * Same as {@link #getRightColumns()}
	 * 
	 * @return the target columns of this foreign key
	 */
	public KeepOrderSet<Column<U, ?>> getTargetColumns() {
		return getRightColumns();
	}
}
