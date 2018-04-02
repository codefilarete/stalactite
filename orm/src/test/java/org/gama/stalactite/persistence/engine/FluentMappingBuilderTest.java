package org.gama.stalactite.persistence.engine;

import java.util.Date;
import java.util.Map;

import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilderTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	
	@BeforeClass
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
	}
	
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
	@Test
	public void testAdd_withoutName_targettedPropertyNameIsTaken() {
		ClassMappingStrategy<Toto, StatefullIdentifier> mappingStrategy = FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getName)
				.build(DIALECT);
		
		// column should be correctly created
		assertEquals("Toto", mappingStrategy.getTargetTable().getName());
		Column columnForProperty = mappingStrategy.getTargetTable().mapColumnsOnName().get("name");
		assertNotNull(columnForProperty);
		assertEquals(String.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testAdd_definedAsIdentifier_columnBecomesPrimaryKey() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
			.add(Toto::getName).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
			.build(DIALECT)
		;
		// column should be correctly created
		Column columnForProperty = toto.mapColumnsOnName().get("name");
		assertTrue(columnForProperty.isPrimaryKey());
	}
	
	@Test
	public void testAdd_identifierDefinedTwice_throwsException() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Identifier is already defined by");
		expectedException.expectMessage("getName");
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
			.add(Toto::getName, "tata").identifier(IdentifierPolicy.ALREADY_ASSIGNED)
			.add(Toto::getFirstName).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
		;
	}
	
	@Test
	public void testAdd_mappingDefinedTwiceByMethod_throwsException() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Mapping is already defined by the method");
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getName)
				.add(Toto::setName)
		;
	}
	
	@Test
	public void testAdd_mappingDefinedTwiceByColumn_throwsException() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Mapping is already defined for xyz");
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getName, "xyz")
				.add(Toto::getFirstName, "xyz")
		;
	}
	
	@Test
	public void testAdd_methodHasNoMatchingField_configurationIsStillValid() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getNoMatchingField)
				.build(DIALECT);
		
		Column columnForProperty = toto.mapColumnsOnName().get("noMatchingField");
		assertNotNull(columnForProperty);
		assertEquals(Long.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testAdd_methodIsASetter_configurationIsStillValid() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::setId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.build(DIALECT);
		
		Column columnForProperty = toto.mapColumnsOnName().get("id");
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
		
		Column columnForProperty = toto.mapColumnsOnName().get("creationDate");
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
		
		Column columnForProperty = toto.mapColumnsOnName().get("creationDate");
		assertNotNull(columnForProperty);
		assertEquals(Date.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testEmbed_withOverridenColumn() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.build(new PersistenceContext(null, DIALECT));
		
		Map<String, Column> columnsByName = toto.mapColumnsOnName();
		
		// original columns must be absent (hard to test: can be absent for many reasons !)
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
		
		@Override
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
