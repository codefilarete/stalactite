package org.codefilarete.stalactite.mapping;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

import org.codefilarete.stalactite.mapping.Mapping.UpwhereColumn;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.MapBasedColumnedRow;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class ZonedDateTimeMappingTest {
	
	private static Table targetTable;
	private static Column<Table, LocalDateTime> colA;
	private static Column<Table, ZoneId> colB;
	
	@BeforeAll
	public static void setUpClass() {
		targetTable = new Table<>("Toto");
		colA = targetTable.addColumn("a", LocalDateTime.class);
		colB = targetTable.addColumn("b", ZoneId.class);
	}
	
	private ZonedDateTimeMapping testInstance;
	
	@BeforeEach
	public void setUp() {
		testInstance = new ZonedDateTimeMapping<>(colA, colB);
	}
	
	@Test
	void getInsertValue() {
		// Preparing insertion of a ZonedDateTime
		ZonedDateTime toBePersisted = ZonedDateTime.of(2018, Month.MAY.ordinal(), 16, 17, 53, 44, 00, ZoneId.of("America/Guadeloupe"));
		Map<Column, Object> insertValues = testInstance.getInsertValues(toBePersisted);
		assertThat(insertValues).isEqualTo(Maps.forHashMap(Column.class, Object.class)
			.add(colA, toBePersisted.toLocalDateTime())
			.add(colB, toBePersisted.getZone()));
	}
	
	@Test
	void transform() {
		// selecting it to see if it is correctly read
		ZonedDateTime toBePersisted = ZonedDateTime.of(2018, Month.MAY.ordinal(), 16, 17, 53, 44, 00, ZoneId.of("America/Guadeloupe"));
		MapBasedColumnedRow row = new MapBasedColumnedRow();
		row.put(colA, toBePersisted.toLocalDateTime());
		row.put(colB, toBePersisted.getZone());
		ZonedDateTime hydratedZonedDateTime = testInstance.transform(row);
		assertThat(toBePersisted).isEqualTo(hydratedZonedDateTime);
	}
	
	@Test
	void transform_null() {
		MapBasedColumnedRow row = new MapBasedColumnedRow();
		row.put(colA, null);
		row.put(colB, null);
		ZonedDateTime hydratedZonedDateTime = testInstance.transform(row);
		assertThat(hydratedZonedDateTime).isNull();
	}
	
	@Test
	void getUpdateValues() {
		ZonedDateTime hydratedZonedDateTime = ZonedDateTime.of(2018, Month.MAY.ordinal(), 16, 17, 53, 44, 00, ZoneId.of("America/Guadeloupe"));
		
		// Date time update
		ZonedDateTime modifiedZonedDateTime = hydratedZonedDateTime.minusHours(4);
		Map<UpwhereColumn, Object> updateValues = testInstance.getUpdateValues(modifiedZonedDateTime, hydratedZonedDateTime, false);
		assertThat(updateValues).isEqualTo(Maps.asMap(new UpwhereColumn(colA, true), modifiedZonedDateTime.toLocalDateTime()));
		
		// Date time update, all columns
		modifiedZonedDateTime = hydratedZonedDateTime.minusHours(4);
		updateValues = testInstance.getUpdateValues(modifiedZonedDateTime, hydratedZonedDateTime, true);
		assertThat(updateValues).isEqualTo(Maps.asMap(new UpwhereColumn(colA, true), (Object) modifiedZonedDateTime.toLocalDateTime())
			.add(new UpwhereColumn(colB, true), modifiedZonedDateTime.getZone()));
		
		// Zone update
		modifiedZonedDateTime = hydratedZonedDateTime.withZoneSameLocal(ZoneId.of("Europe/Paris"));
		updateValues = testInstance.getUpdateValues(modifiedZonedDateTime, hydratedZonedDateTime, false);
		assertThat(updateValues).isEqualTo(Maps.asMap(new UpwhereColumn(colB, true), modifiedZonedDateTime.getZone()));
		
		// Date time and zone update
		modifiedZonedDateTime = hydratedZonedDateTime.withZoneSameInstant(ZoneId.of("Europe/Paris"));
		updateValues = testInstance.getUpdateValues(modifiedZonedDateTime, hydratedZonedDateTime, false);
		assertThat(updateValues).isEqualTo(Maps.asMap(new UpwhereColumn(colA, true), (Object) modifiedZonedDateTime.toLocalDateTime())
			.add(new UpwhereColumn(colB, true), modifiedZonedDateTime.getZone()));
		
		// setting to null
		updateValues = testInstance.getUpdateValues(null, hydratedZonedDateTime, true);
		assertThat(updateValues).isEqualTo(Maps.forHashMap(UpwhereColumn.class, Object.class)
			.add(new UpwhereColumn(colA, true), null)
			.add(new UpwhereColumn(colB, true), null));
	}
}