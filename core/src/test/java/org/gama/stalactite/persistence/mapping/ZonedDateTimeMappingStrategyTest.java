package org.gama.stalactite.persistence.mapping;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

import org.codefilarete.tool.collection.Maps;
import org.gama.stalactite.persistence.mapping.MappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class ZonedDateTimeMappingStrategyTest {
	
	private static Table targetTable;
	private static Column<Table, LocalDateTime> colA;
	private static Column<Table, ZoneId> colB;
	
	@BeforeAll
	public static void setUpClass() {
		targetTable = new Table<>("Toto");
		colA = targetTable.addColumn("a", LocalDateTime.class);
		colB = targetTable.addColumn("b", ZoneId.class);
	}
	
	private ZonedDateTimeMappingStrategy testInstance;
	
	@BeforeEach
	public void setUp() {
		testInstance = new ZonedDateTimeMappingStrategy<>(colA, colB);
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
		Row row = new Row()
			.add(colA.getName(), toBePersisted.toLocalDateTime())
			.add(colB.getName(), toBePersisted.getZone());
		ZonedDateTime hydratedZonedDateTime = testInstance.transform(row);
		assertThat(toBePersisted).isEqualTo(hydratedZonedDateTime);
	}
	
	@Test
	void transform_null() {
		Row row = new Row()
			.add(colA.getName(), null)
			.add(colB.getName(), null);
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