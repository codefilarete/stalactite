package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.PersistenceMapper.IdentifierPolicy;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Guillaume Mary
 */
public class PersistenceMapperTest {
	
	protected static class Toto {
		
		private String name;
		
		private String firstName;
		
		public Toto() {
		}
		
		public String getName() {
			return name;
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
	}
	
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
	@Test
	public void testAdd_withoutName_targettedPropertyNameIsTook() {
		Table toto = new Table("Toto");
		PersistenceMapper.with(Toto.class, toto)
				.add(Toto::getName);
		
		// column sould be correctly created
		Column columnForProperty = toto.mapColumnsOnName().get("name");
		assertNotNull(columnForProperty);
		assertEquals(String.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testAdd_definedAsIdentifier_columnBecomesPrimaryKey() {
		Table toto = new Table("Toto");
		PersistenceMapper.with(Toto.class, toto)
			.add(Toto::getName).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
		;
		// column sould be correctly created
		Column columnForProperty = toto.mapColumnsOnName().get("name");
		assertTrue(columnForProperty.isPrimaryKey());
	}
	
	@Test
	public void testAdd_identifierDefinedTwice_throwsException() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Identifier is already defined by");
		expectedException.expectMessage("getName");
		Table toto = new Table("Toto");
		PersistenceMapper.with(Toto.class, toto)
			.add(Toto::getName, "tata").identifier(IdentifierPolicy.ALREADY_ASSIGNED)
			.add(Toto::getFirstName).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
		;
	}
	
	@Test
	public void testAdd_methodHasNoMatchingField_configurationIsStillValid() {
		Table toto = new Table("Toto");
		PersistenceMapper.with(Toto.class, toto)
				.add(Toto::getNoMatchingField);
		
		Column columnForProperty = toto.mapColumnsOnName().get("noMatchingField");
		assertNotNull(columnForProperty);
		assertEquals(Long.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testAdd_methodIsASetter_configurationIsStillValid() {
		Table toto = new Table("Toto");
		PersistenceMapper.with(Toto.class, toto)
				.add(Toto::setNoMatchingField)
				.add(Toto::setNoMatchingFieldPrimitive)
		;
	}
	
}
