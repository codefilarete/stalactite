package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.tool.collection.Arrays;

/**
 * Implementation of <code>date_format</code> SQL function
 * 
 * @author Guillaume Mary
 */
public class DateFormat extends SQLFunction<Iterable<?>, String> {
	
	/**
	 * 
	 * @param selectable any selectable representing a date (Date, LocalDate, LocalDateTime, ZonedDateTime, etc)
	 * @param format the pattern accepted by the <code>date_format</code> SQL function
	 */
	public DateFormat(Selectable<?> selectable, String format) {
		super("date_format", String.class, Arrays.asList(selectable, format));
	}
}