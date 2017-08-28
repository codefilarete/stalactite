package org.gama.stalactite.persistence.engine;

import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilderTest {
	
	protected static class Toto implements Identified<Integer> {
		
		private String name;
		
		private String firstName;
		
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
	}
	
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
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getName)
				.build(DIALECT);
		
		// column should be correctly created
		Column columnForProperty = toto.mapColumnsOnName().get("name");
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
				.add(Toto::setNoMatchingField)
				.add(Toto::setNoMatchingFieldPrimitive)
		;
	}
	
}
