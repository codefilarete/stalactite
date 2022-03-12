package org.codefilarete.stalactite.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Month;

/**
 * {@link ParameterBinder} dedicated to {@link LocalTime} : uses {@link ResultSet#getTimestamp(String)} and {@link PreparedStatement#setTimestamp(int, Timestamp)}
 *
 * @author Guillaume Mary
 */
public class LocalTimeBinder implements ParameterBinder<LocalTime> {
	
	/**
	 * Default {@link LocalDate} used to build stored Timestamp, totally arbitrary, but don't use {@link LocalDate#MIN} because it's too "low"
	 * for {@link Timestamp#valueOf(String)} which doesn't convert it correctly.
	 */
	public static final LocalDate DEFAULT_TIMESTAMP_REFERENCE_DATE = LocalDate.of(2000, Month.JANUARY, 1);
	
	private final LocalDate timestampReferenceDate;
	
	public LocalTimeBinder() {
		this(DEFAULT_TIMESTAMP_REFERENCE_DATE);
	}
	
	public LocalTimeBinder(LocalDate timestampReferenceDate) {
		this.timestampReferenceDate = timestampReferenceDate;
	}
	
	@Override
	public LocalTime doGet(ResultSet resultSet, String columnName) throws SQLException {
		return resultSet.getTimestamp(columnName).toLocalDateTime().toLocalTime();
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, LocalTime value) throws SQLException {
		// LocalDateTime is stored as Timestamp (not found any other compatible and cross-database SQL type), which is built from a LocalDateTime
		// from given LocalTime, but it requires a LocalDate to be fulfilled : for our case it can be arbitrary since doGet(..) doesn't uses it.
		statement.setTimestamp(valueIndex, Timestamp.valueOf(value.atDate(timestampReferenceDate)));
	}
}
