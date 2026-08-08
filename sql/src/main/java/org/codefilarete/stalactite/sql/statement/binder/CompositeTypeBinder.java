package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.ResultSet;

/**
 * Contract for handling composite type that needs to be dispatched to several columns on a {@link java.sql.PreparedStatement}.
 * This class was designed to fulfill the need of passing a composite type to an "in" operator, aka a tuple-in.
 * It implements {@link ParameterBinder} to be pushed to a {@link ColumnBinderRegistry}, but actually doesn't
 * support reading from a {@link ResultSet} because it would require reading several columns, whereas {@link #get(ResultSet, String)}
 * allows only one to read from.
 *
 * @param <T> the mapped type
 * @author Guillaume Mary
 */
public interface CompositeTypeBinder<T> extends ParameterBinder<T> {
	
	int getComponentTypeSize();
}
