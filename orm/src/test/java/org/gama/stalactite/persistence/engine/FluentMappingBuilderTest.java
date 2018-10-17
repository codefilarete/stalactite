package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;

import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilderTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	
	@BeforeAll
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
	}
	
	@Test
	public void testAdd_withoutName_targetedPropertyNameIsTaken() {
		ClassMappingStrategy<Toto, StatefullIdentifier, Table> mappingStrategy = FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getName)
				.build(DIALECT);
		
		// column should be correctly created
		assertEquals("Toto", mappingStrategy.getTargetTable().getName());
		Column columnForProperty = (Column) mappingStrategy.getTargetTable().mapColumnsOnName().get("name");
		assertNotNull(columnForProperty);
		assertEquals(String.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testAdd_withColumn_columnIsTaken() {
		Table toto = new Table("Toto");
		Column<Table, String> titleColumn = toto.addColumn("title", String.class);
		ClassMappingStrategy<Toto, StatefullIdentifier, Table> mappingStrategy = FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getName, titleColumn)
				.build(DIALECT);
		
		// column should not have been created
		Column columnForProperty = (Column) mappingStrategy.getTargetTable().mapColumnsOnName().get("name");
		assertNull(columnForProperty);
	}
	
	@Test
	public void testAdd_definedAsIdentifier_columnBecomesPrimaryKey() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
			.add(Toto::getName).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
			.build(DIALECT)
		;
		// column should be correctly created
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("name");
		assertTrue(columnForProperty.isPrimaryKey());
	}
	
	@Test
	public void testAdd_identifierDefinedTwice_throwsException() {
		Table toto = new Table("Toto");
		assertThrows(IllegalArgumentException.class, () -> FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
			.add(Toto::getName, "tata").identifier(IdentifierPolicy.ALREADY_ASSIGNED)
			.add(Toto::getFirstName).identifier(IdentifierPolicy.ALREADY_ASSIGNED),
			"Identifier is already defined by getName");
	}
	
	@Test
	public void testAdd_mappingDefinedTwiceByMethod_throwsException() {
		Table toto = new Table("Toto");
		assertThrows(IllegalArgumentException.class, () -> FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getName)
				.add(Toto::setName),
				"Mapping is already defined by the method"
				);
	}
	
	@Test
	public void testAdd_mappingDefinedTwiceByColumn_throwsException() {
		Table toto = new Table("Toto");
		assertThrows(IllegalArgumentException.class, () -> FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getName, "xyz")
				.add(Toto::getFirstName, "xyz"),
				"Mapping is already defined for xyz");
	}
	
	@Test
	public void testAdd_methodHasNoMatchingField_configurationIsStillValid() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getNoMatchingField)
				.build(DIALECT);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("noMatchingField");
		assertNotNull(columnForProperty);
		assertEquals(Long.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testAdd_methodIsASetter_configurationIsStillValid() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::setId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.build(DIALECT);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("id");
		assertNotNull(columnForProperty);
		assertEquals(Identifier.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testEmbed_definedByGetter() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
				.build(new PersistenceContext(null, DIALECT));
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("creationDate");
		assertNotNull(columnForProperty);
		assertEquals(Date.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testEmbed_definedBySetter() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::setTimestamp)
				.build(new PersistenceContext(null, DIALECT));
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("creationDate");
		assertNotNull(columnForProperty);
		assertEquals(Date.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testEmbed_withOverridenColumnName() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.build(new PersistenceContext(null, DIALECT));
		
		Map<String, Column> columnsByName = toto.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertNull(columnsByName.get("creationDate"));
		assertNull(columnsByName.get("modificationDate"));

		Column overridenColumn;
		// Columns with good name must be present
		overridenColumn = columnsByName.get("modifiedAt");
		assertNotNull(overridenColumn);
		assertEquals(Date.class, overridenColumn.getJavaType());
		overridenColumn = columnsByName.get("createdAt");
		assertNotNull(overridenColumn);
		assertEquals(Date.class, overridenColumn.getJavaType());
	}
	
	@Test
	public void testEmbed_withOverridenColumn() throws SQLException {
		Table toto = new Table("Toto");
		Column<Table, Date> createdAt = toto.addColumn("createdAt", Date.class);
		Column<Table, Date> modifiedAt = toto.addColumn("modifiedAt", Date.class);
		
		// Preparation of column mapping check thought insert statement generation
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
		when(preparedStatementMock.executeBatch()).thenReturn(new int[]{ 1 });
		ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
		
		Persister<Toto, StatefullIdentifier, ?> persister = FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
				.override(Timestamp::getCreationDate, createdAt)
				.override(Timestamp::getModificationDate, modifiedAt)
				.build(new PersistenceContext(new SimpleConnectionProvider(connectionMock), DIALECT));
		
		Map<String, Column> columnsByName = toto.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertNull(columnsByName.get("creationDate"));
		assertNull(columnsByName.get("modificationDate"));
		
		// checking that overriden column are well mapped: we use the insert command for that
		Toto dummyInstance = new Toto();
		dummyInstance.setTimestamp(new Timestamp());
		persister.insert(dummyInstance);
		verify(connectionMock).prepareStatement(argumentCaptor.capture());
		// the insert SQL must contains overriden columns
		assertEquals("insert into Toto(id, createdAt, modifiedAt) values (?, ?, ?)", argumentCaptor.getValue());
	}
	
	protected static class Toto implements Identified<Integer> {
		
		private String name;
		
		private String firstName;
		
		private Timestamp timestamp;
		
		public Toto() {
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public String getFirstName() {
			return name;
		}
		
		public Long getNoMatchingField() {
			return null;
		}
		
		public void setNoMatchingField(Long s) {
		}
		
		public long getNoMatchingFieldPrimitive() {
			return 0;
		}
		
		public void setNoMatchingFieldPrimitive(long s) {
		}
		
		@Override
		public Identifier<Integer> getId() {
			return null;
		}
		
		public void setId(Identifier<Integer> id) {
			
		}
		
		public Timestamp getTimestamp() {
			return timestamp;
		}
		
		public void setTimestamp(Timestamp timestamp) {
			this.timestamp = timestamp;
		}
	}
	
	protected static class Timestamp {
		
		private Date creationDate;
		
		private Date modificationDate;
		
		public Date getCreationDate() {
			return creationDate;
		}
		
		public Date getModificationDate() {
			return modificationDate;
		}
	}
}
