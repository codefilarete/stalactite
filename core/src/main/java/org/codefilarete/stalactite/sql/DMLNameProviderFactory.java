package org.codefilarete.stalactite.sql;

import java.util.Map;
import java.util.function.Function;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.api.Fromable;

/**
 * A factory of {@link DMLNameProvider} to let one decides which concrete implementation he wants to provide.
 * Typical usage is to provide an instance that protect against database vendor keywords (by quoting them).
 * 
 * @author Guillaume Mary
 */
public interface DMLNameProviderFactory {
	
	/**
	 * Expected to create a {@link DMLNameProvider} with given aliasing function.
	 * 
	 * @param tableAliaser a {@link Function} that provides the alias of tables
	 * @return a {@link DMLNameProvider} that takes its aliases from given {@link Function}
	 */
	DMLNameProvider build(Function<Fromable, String> tableAliaser);
	
	/**
	 * Expected to create a {@link DMLNameProvider} with given aliases.
	 *
	 * @param tableAliases a {@link Map} that provides the alias of tables
	 * @return a {@link DMLNameProvider} that takes its aliases from given {@link Map}
	 */
	default DMLNameProvider build(Map<? extends Fromable, String> tableAliases) {
		return build(tableAliases::get);
	}
}
