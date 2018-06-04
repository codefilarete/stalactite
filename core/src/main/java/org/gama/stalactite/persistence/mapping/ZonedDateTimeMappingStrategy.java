package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.gama.lang.Reflections;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * A mapping strategy to persist a {@link ZonedDateTime} : requires 2 columns, one for the date-time part, another for the timezone.
 * Columns must respectively have a Java type of :
 * <ul>
 * <li>{@link LocalDateTime}</li>
 * <li>{@link ZoneId}</li>
 * </ul>
 * Thus, the {@link org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping} and {@link org.gama.sql.binder.ParameterBinderRegistry}
 * of your {@link org.gama.stalactite.persistence.sql.Dialect} must have them registered (which is done by default).
 * 
 * @author Guillaume Mary
 */
public class ZonedDateTimeMappingStrategy<T extends Table> implements IEmbeddedBeanMapper<ZonedDateTime, T> {
	
	private final Column<T, LocalDateTime> dateTimeColumn;
	private final Column<T, ZoneId> zoneColumn;
	private final UpwhereColumn<T> dateTimeUpdateColumn;
	private final UpwhereColumn<T> zoneUpdateColumn;
	private final Set<Column<T, ?>> columns;
	
	/**
	 * Build a EmbeddedBeanMappingStrategy from a mapping between Field and Column.
	 * Fields are expected to be from same class.
	 * Columns are expected to be from same table.
	 * No control is done about that, caller must be aware of it.
	 * First entry of <tt>propertyToColumn</tt> is used to pick up persisted class and target table.
	 *
	 * @param dateTimeColumn the column containing date and time part of final {@link ZonedDateTime}
	 * @param zoneColumn the column containing the zone part of final {@link ZonedDateTime}
	 * @throws IllegalArgumentException if dateTimeColumn is not of type {@link LocalDateTime} or zoneColumn of type {@link ZoneId}
	 */
	public ZonedDateTimeMappingStrategy(Column<T, LocalDateTime> dateTimeColumn, Column<T, ZoneId> zoneColumn) {
		if (!LocalDateTime.class.isAssignableFrom(dateTimeColumn.getJavaType())) {
			throw new IllegalArgumentException("Only column whose type is " + Reflections.toString(LocalDateTime.class) + " are supported");
		}
		if (!ZoneId.class.isAssignableFrom(zoneColumn.getJavaType())) {
			throw new IllegalArgumentException("Only column whose type is " + Reflections.toString(ZoneId.class) + " are supported");
		}
		this.dateTimeColumn = dateTimeColumn;
		this.zoneColumn = zoneColumn;
		this.dateTimeUpdateColumn = new UpwhereColumn<>(dateTimeColumn, true);
		this.zoneUpdateColumn = new UpwhereColumn<>(zoneColumn, true);
		this.columns = Collections.unmodifiableSet(Arrays.asHashSet(dateTimeColumn, zoneColumn));
	}
	
	@Nonnull
	@Override
	public Set<Column<T, Object>> getColumns() {
		return (Set) columns;
	}
	
	@Nonnull
	@Override
	public Map<Column<T, Object>, Object> getInsertValues(ZonedDateTime zonedDateTime) {
		Map<Column<T, ?>, Object> result = new HashMap<>();
		result.put(dateTimeColumn, zonedDateTime.toLocalDateTime());
		result.put(zoneColumn, zonedDateTime.getZone());
		return (Map) result;
	}
	
	@Nonnull
	@Override
	public Map<UpwhereColumn<T>, Object> getUpdateValues(ZonedDateTime modified, ZonedDateTime unmodified, boolean allColumns) {
		Map<Column<T, ?>, Object> unmodifiedColumns = new HashMap<>();
		Map<UpwhereColumn<T>, Object> toReturn = new HashMap<>();
		// getting differences side by side
		if (modified != null) {
			LocalDateTime modifiedDateTime = unmodified == null ? null : unmodified.toLocalDateTime();
			if (!Objects.equalsWithNull(modified.toLocalDateTime(), modifiedDateTime)) {
				toReturn.put(dateTimeUpdateColumn, modified.toLocalDateTime());
			} else {
				unmodifiedColumns.put(dateTimeColumn, modifiedDateTime);
			}
			ZoneId modifiedZone = unmodified == null ? null : unmodified.getZone();
			if (!Objects.equalsWithNull(modified.getZone(), modifiedZone)) {
				toReturn.put(zoneUpdateColumn, modified.getZone());
			} else {
				unmodifiedColumns.put(zoneColumn, modifiedZone);
			}
		} else {
			toReturn.put(dateTimeUpdateColumn, null);
			toReturn.put(zoneUpdateColumn, null);
		}

		// adding complementary columns if necessary
		if (!toReturn.isEmpty() && allColumns) {
			for (Entry<Column<T, ?>, Object> unmodifiedField : unmodifiedColumns.entrySet()) {
				toReturn.put(new UpwhereColumn<>(unmodifiedField.getKey(), true), unmodifiedField.getValue());
			}
		}
		return toReturn;
	}
	
	
	@Override
	public ZonedDateTime transform(Row row) {
		if (row.get(dateTimeColumn.getName()) == null || row.get(zoneColumn.getName()) == null) {
			return null;
		} else {
			return ZonedDateTime.of((LocalDateTime) row.get(dateTimeColumn.getName()), (ZoneId) row.get(zoneColumn.getName()));
		}
	}
}
