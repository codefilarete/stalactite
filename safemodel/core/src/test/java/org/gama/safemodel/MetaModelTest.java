package org.gama.safemodel;

import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume
 */
public class MetaModelTest {
	
	@Test
	public void testFixFieldsOwner() throws Exception {
		FixFieldsOwnerTestMetaModel testInstance = new FixFieldsOwnerTestMetaModel();
		
		assertEquals(testInstance, testInstance.field1.getOwner());
		assertEquals(testInstance, testInstance.protectedField.getOwner());
	}
	
	@Test
	public void testFixFieldsOwner_inheritance() throws Exception {
		FixFieldsOwnerTestMetaModel_extended testInstance = new FixFieldsOwnerTestMetaModel_extended();
		testInstance.fixFieldsOwner();
		
		assertEquals(testInstance, testInstance.field1.getOwner());
		assertEquals(testInstance, testInstance.protectedField.getOwner());
		assertEquals(testInstance, testInstance.field3.getOwner());
	}
	
	@Test(expectedExceptions = Exception.class)	// setting owner on private field in not expected to work
	public void testFixFieldsOwner_privateField() throws Exception {
		FixFieldsOwnerTestMetaModel_privateField testInstance = new FixFieldsOwnerTestMetaModel_privateField();
	}
	
	private static class FixFieldsOwnerTestMetaModel extends MetaModel {
		
		public MetaModel field1 = new MetaModel();
		
		protected MetaModel protectedField = new MetaModel();
		
		FixFieldsOwnerTestMetaModel() {
			fixFieldsOwner();
		}
	}
	
	private static class FixFieldsOwnerTestMetaModel_extended extends FixFieldsOwnerTestMetaModel {
		
		public MetaModel field3 = new MetaModel();
		
		FixFieldsOwnerTestMetaModel_extended() {
			super();
			fixFieldsOwner();
		}
	}
	
	private static class FixFieldsOwnerTestMetaModel_privateField extends MetaModel {
		
		private MetaModel privateField = new MetaModel();
		
		FixFieldsOwnerTestMetaModel_privateField() {
			fixFieldsOwner();
		}
	}
	
}