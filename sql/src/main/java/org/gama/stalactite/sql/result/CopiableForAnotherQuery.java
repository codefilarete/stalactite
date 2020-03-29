package org.gama.stalactite.sql.result;

import java.util.Map;
import java.util.function.Function;

import org.gama.lang.exception.NotImplementedException;

/**
 * @author Guillaume Mary
 */
public interface CopiableForAnotherQuery<T> {
	
	/**
	 * Expected to make a copy of this instance with column translation to reuse this instance on another kind of {@link java.sql.ResultSet} on which
	 * columns differ by their column names. Optional operation (not necessary if query is not expected to be reused in such a context).
	 * <strong>This implementation throws an {@link NotImplementedException}</strong>
	 *
	 * @param columnMapping a {@link Function} that gives a new column name for a asked one
	 * 						Can be implemented with a switch/case, a prefix/suffix concatenation, etc
	 * @return a new instance, kind of clone of this
	 */
	default CopiableForAnotherQuery<T> copyWithAliases(Function<String, String> columnMapping) {
		throw new NotImplementedException("This instance doesn't support copy, please implement it if you wish to reuse its mapping for another query");
	}
	
	
	/**
	 * Same as {@link #copyWithAliases(Function)} but with a concrete mapping through a {@link Map}
	 * 
	 * @param columnMapping the mapping between column names and new ones
	 * @return a new instance, kind of clone of this
	 */
	default CopiableForAnotherQuery<T> copyWithAliases(Map<String, String> columnMapping) {
		return copyWithAliases(columnMapping::get);
	}
}
