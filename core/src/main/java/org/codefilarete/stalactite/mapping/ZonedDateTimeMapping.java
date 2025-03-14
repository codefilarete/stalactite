package org.codefilarete.stalactite.mapping;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.tool.function.Predicates;

/**
 * A mapping strategy to persist a {@link ZonedDateTime} : requires 2 columns, one for the date-time part, another for the timezone.
 * Columns must respectively have a Java type of :
 * <ul>
 * <li>{@link LocalDateTime}</li>
 * <li>{@link ZoneId}</li>
 * </ul>
 * Thus, the {@link SqlTypeRegistry} and {@link ParameterBinderRegistry}
 * of your {@link Dialect} must have them registered (which is done by default).
 * 
 * @author Guillaume Mary
 */
public class ZonedDateTimeMapping<T extends Table<T>> implements EmbeddedBeanMapping<ZonedDateTime, T> {
	
	private final Column<T, LocalDateTime> dateTimeColumn;
	private final Column<T, ZoneId> zoneColumn;
	private final UpwhereColumn<T> dateTimeUpdateColumn;
	private final UpwhereColumn<T> zoneUpdateColumn;
	private final Set<Column<T, Object>> columns;
	private final ZonedDateTimeToBeanRowTransformer zonedDateTimeRowTransformer;
	
	/**
	 * Builds a {@link LocalDateTime} and {@link ZoneId} embedded mapping by specifying repective {@link Column}s.
	 * {@link Column}s are expected to be from same table, no strong control is made about that except generic type, caller must be aware of it.
	 * 
	 * @param dateTimeColumn the column containing date and time part of final {@link ZonedDateTime}
	 * @param zoneColumn the column containing the zone part of final {@link ZonedDateTime}
	 * @throws IllegalArgumentException if dateTimeColumn is not of type {@link LocalDateTime} or zoneColumn of type {@link ZoneId}
	 */
	public ZonedDateTimeMapping(Column<T, LocalDateTime> dateTimeColumn, Column<T, ZoneId> zoneColumn) {
		if (!LocalDateTime.class.isAssignableFrom(dateTimeColumn.getJavaType())) {
			throw new IllegalArgumentException("Only columns with type " + Reflections.toString(LocalDateTime.class) + " are supported");
		}
		if (!ZoneId.class.isAssignableFrom(zoneColumn.getJavaType())) {
			throw new IllegalArgumentException("Only columns with type " + Reflections.toString(ZoneId.class) + " are supported");
		}
		this.dateTimeColumn = dateTimeColumn;
		this.zoneColumn = zoneColumn;
		this.dateTimeUpdateColumn = new UpwhereColumn<>(dateTimeColumn, true);
		this.zoneUpdateColumn = new UpwhereColumn<>(zoneColumn, true);
		this.columns = (Set<Column<T, Object>>) (Set) Collections.unmodifiableSet(Arrays.asHashSet(dateTimeColumn, zoneColumn));
		this.zonedDateTimeRowTransformer = new ZonedDateTimeToBeanRowTransformer();
	}
	
	@Override
	public Set<Column<T, Object>> getColumns() {
		return (Set) columns;
	}
	
	@Override
	public void addPropertySetByConstructor(ValueAccessPoint<ZonedDateTime> accessor) {
		// this class doesn't support bean factory so it can't support properties set by constructor
	}
	
	@Override
	public Map<Column<T, ?>, Object> getInsertValues(ZonedDateTime zonedDateTime) {
		Map<Column<T, ?>, Object> result = new HashMap<>();
		result.put(dateTimeColumn, zonedDateTime.toLocalDateTime());
		result.put(zoneColumn, zonedDateTime.getZone());
		return result;
	}
	
	@Override
	public Map<UpwhereColumn<T>, Object> getUpdateValues(ZonedDateTime modified, ZonedDateTime unmodified, boolean allColumns) {
		Map<Column<T, ?>, Object> unmodifiedColumns = new HashMap<>();
		Map<UpwhereColumn<T>, Object> toReturn = new HashMap<>();
		// getting differences side by side
		if (modified != null) {
			LocalDateTime modifiedDateTime = unmodified == null ? null : unmodified.toLocalDateTime();
			if (!Predicates.equalOrNull(modified.toLocalDateTime(), modifiedDateTime)) {
				toReturn.put(dateTimeUpdateColumn, modified.toLocalDateTime());
			} else {
				unmodifiedColumns.put(dateTimeColumn, modifiedDateTime);
			}
			ZoneId modifiedZone = unmodified == null ? null : unmodified.getZone();
			if (!Predicates.equalOrNull(modified.getZone(), modifiedZone)) {
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
		return zonedDateTimeRowTransformer.transform(row);
	}
	
	@Override
	public Map<ReversibleAccessor<ZonedDateTime, Object>, Column<T, Object>> getPropertyToColumn() {
		throw new NotImplementedException(Reflections.toString(ZonedDateTimeMapping.class) + " can't export a mapping between some accessors and their columns"
				+ " because properties of " + Reflections.toString(ZonedDateTime.class) + " can't be set");
	}
	
	@Override
	public Map<ReversibleAccessor<ZonedDateTime, Object>, Column<T, Object>> getReadonlyPropertyToColumn() {
		throw new NotImplementedException(Reflections.toString(ZonedDateTimeMapping.class) + " can't export a mapping between some accessors and their columns"
				+ " because properties of " + Reflections.toString(ZonedDateTime.class) + " can't be set");
	}
	
	@Override
	public Set<Column<T, Object>> getWritableColumns() {
		return this.columns;
	}
	
	@Override
	public Set<Column<T, Object>> getReadonlyColumns() {
		return java.util.Collections.emptySet();
	}
	
	@Override
	public ZonedDateTimeToBeanRowTransformer copyTransformerWithAliases(ColumnedRow columnedRow) {
		return this.zonedDateTimeRowTransformer.copyWithAliases(columnedRow);
	}
	
	@Nullable
	private ZonedDateTime buildZonedDateTime(ColumnedRow columnedRow, Row row) {
		return buildZonedDateTime(columnedRow.getValue(dateTimeColumn, row), columnedRow.getValue(zoneColumn, row));
	}
	
	@Nullable
	private ZonedDateTime buildZonedDateTime(LocalDateTime dateTimeColumnName, ZoneId zoneColumnName) {
		if (dateTimeColumnName == null || zoneColumnName == null) {
			return null;
		} else {
			return ZonedDateTime.of(dateTimeColumnName, zoneColumnName);
		}
	}
	
	class ZonedDateTimeToBeanRowTransformer extends ToBeanRowTransformer<ZonedDateTime> {
		
		public ZonedDateTimeToBeanRowTransformer() {
			super(ZonedDateTime.class, Collections.emptyMap());
		}
		
		@Nullable
		@Override
		public ZonedDateTime newBeanInstance(Row row) {
			return buildZonedDateTime(new ColumnedRow(), row);
		}
		
		@Override
		public ZonedDateTimeToBeanRowTransformer copyWithAliases(ColumnedRow columnedRow) {
			return new ZonedDateTimeToBeanRowTransformer() {
				@Nullable
				@Override
				public ZonedDateTime newBeanInstance(Row row) {
					return buildZonedDateTime(columnedRow, row);
				}
			};
		}
	}
}
