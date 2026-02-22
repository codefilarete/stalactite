package org.codefilarete.stalactite.query.builder;

import java.util.function.Function;

import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.Selectable;

/**
 * A {@link DMLNameProvider} that add back-quotes to all identifiers.
 * Quoting names make the database keep lower/upper-case of the table/column.
 * For some database vendors, sometimes combined with some OS, respecting table/column definition case is mandatory if you want the SQL operation
 * to find the table/column you target.
 * 
 * @author Guillaume Mary
 */
public class QuotingDMLNameProvider extends DMLNameProvider {
	
	private final char quoteCharacter;
	
	public QuotingDMLNameProvider(Function<Fromable, String> tableAliaser) {
		this(tableAliaser, '`');
	}
	
	public QuotingDMLNameProvider(Function<Fromable, String> tableAliaser, char quoteCharacter) {
		super(tableAliaser);
		this.quoteCharacter = quoteCharacter;
	}
	
	@Override
	public String getSimpleName(Selectable<?> column) {
		return quoteCharacter + super.getSimpleName(column) + quoteCharacter;
	}
	
	@Override
	public String getName(Fromable table) {
		return quoteCharacter + super.getName(table) + quoteCharacter;
	}
}
