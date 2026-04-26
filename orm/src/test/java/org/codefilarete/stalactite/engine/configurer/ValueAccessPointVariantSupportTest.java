package org.codefilarete.stalactite.engine.configurer;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.DefaultReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ReadWriteAccessorChain;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ValueAccessPointVariantSupportTest {
	
	@Test
	void init_byAccessor_accessPointIsByMethodReferenceAndField() {
		ValueAccessPointVariantSupport<DummyClass, Integer> testInstance = new ValueAccessPointVariantSupport<>(DummyClass::getReadonlyProp);
		
		assertThat(testInstance.getAccessor()).isEqualTo(
				Accessors.readWriteAccessPoint(DummyClass::getReadonlyProp));
		assertThat(testInstance.getAccessor().getReader()).isEqualTo(
				Accessors.accessorByMethodReference(DummyClass::getReadonlyProp));
		assertThat(testInstance.getAccessor().getWriter()).isEqualTo(
				Accessors.mutatorByField(DummyClass.class, "readonlyProp"));
	}
	
	@Test
	void init_byAccessor_fieldIsOverridden_accessPointIsByMethodReferenceAndField() {
		ValueAccessPointVariantSupport<DummyClass, Integer> testInstance = new ValueAccessPointVariantSupport<>(DummyClass::getNonJavaBeanCompliantProperty);
		testInstance.setField(DummyClass.class, "readonlyProp");
		
		assertThat(testInstance.getAccessor().getReader()).isEqualTo(
				Accessors.accessorByMethodReference(DummyClass::getNonJavaBeanCompliantProperty));
		assertThat(testInstance.getAccessor().getWriter()).isEqualTo(
				Accessors.mutatorByField(DummyClass.class, "readonlyProp"));
	}
	
	@Test
	void init_byMutator_fieldIsOverridden_accessPointIsByMethodReferenceAndField() {
		ValueAccessPointVariantSupport<DummyClass, Integer> testInstance = new ValueAccessPointVariantSupport<>(DummyClass::setNonJavaBeanCompliantProperty);
		testInstance.setField(DummyClass.class, "writeOnlyProp");
		
		assertThat(testInstance.getAccessor().getReader()).isEqualTo(
				Accessors.accessorByField(DummyClass.class, "writeOnlyProp"));
		assertThat(testInstance.getAccessor().getWriter()).isEqualTo(
				Accessors.mutatorByMethodReference(DummyClass::setNonJavaBeanCompliantProperty));
	}
	
	@Test
	void init_byMutator_accessPointIsByMethodReferenceAndField() {
		ValueAccessPointVariantSupport<DummyClass, Integer> testInstance = new ValueAccessPointVariantSupport<>(DummyClass::setWriteOnlyProp);
		
		assertThat(testInstance.getAccessor()).isEqualTo(
				Accessors.readWriteAccessPoint(DummyClass::setWriteOnlyProp));
		assertThat(testInstance.getAccessor().getReader()).isEqualTo(
				Accessors.accessorByField(DummyClass.class, "writeOnlyProp"));
		assertThat(testInstance.getAccessor().getWriter()).isEqualTo(
				Accessors.mutatorByMethodReference(DummyClass::setWriteOnlyProp));
	}
	
	@Test
	void init_byField_accessPointIsByField() {
		ValueAccessPointVariantSupport<DummyClass, Integer> testInstance = new ValueAccessPointVariantSupport<>(DummyClass.class, "privateProp");
		
		assertThat(testInstance.getAccessor()).isEqualTo(
				new DefaultReadWritePropertyAccessPoint<>(Accessors.accessorByField(DummyClass.class, "privateProp")));
		assertThat(testInstance.getAccessor().getReader()).isEqualTo(
				Accessors.accessorByField(DummyClass.class, "privateProp"));
		assertThat(testInstance.getAccessor().getWriter()).isEqualTo(
				Accessors.mutatorByField(DummyClass.class, "privateProp"));
	}
	
	@Test
	void init_byField_accessPointIsByField_evenIfPublicAccessorExists() {
		ValueAccessPointVariantSupport<DummyClass, Integer> testInstance = new ValueAccessPointVariantSupport<>(DummyClass.class, "readonlyProp");
		
		assertThat(testInstance.getAccessor()).isEqualTo(
				new DefaultReadWritePropertyAccessPoint<>(Accessors.accessorByField(DummyClass.class, "readonlyProp")));
		assertThat(testInstance.getAccessor().getReader()).isEqualTo(
				Accessors.accessorByField(DummyClass.class, "readonlyProp"));
		assertThat(testInstance.getAccessor().getWriter()).isEqualTo(
				Accessors.mutatorByField(DummyClass.class, "readonlyProp"));
	}
	
	@Test
	void shift() {
		ValueAccessPointVariantSupport<DummyClass, Integer> testInstance = new ValueAccessPointVariantSupport<>(DummyClass.class, "readonlyProp");
		ReadWritePropertyAccessPoint<DummyClassProvider, DummyClass> dummyClassAccessor = Accessors.readWriteAccessPoint(DummyClassProvider::getDummyClass);
		ReadWritePropertyAccessPoint<DummyClassProvider, Integer> shiftedInstance = testInstance.shift(dummyClassAccessor);
		
		assertThat(shiftedInstance).isEqualTo(
				new ReadWriteAccessorChain<>(AccessorChain.fromAccessorsWithNullSafe(Arrays.asList(dummyClassAccessor, new DefaultReadWritePropertyAccessPoint<>(Accessors.accessorByField(DummyClass.class, "readonlyProp"))))));
		assertThat(shiftedInstance.getReader()).isEqualTo(
				AccessorChain.fromAccessorsWithNullSafe(Arrays.asList(dummyClassAccessor, new DefaultReadWritePropertyAccessPoint<>(Accessors.accessorByField(DummyClass.class, "readonlyProp")))));
		// Note that shiftedInstance.getWriter() is not testable because it's a lambda, see ReadWriteAccessorChain.mutator
	}
	
	private static class DummyClass {
		
		private int readonlyProp;
		
		private int writeOnlyProp;
		
		private int privateProp;
		
		public int getReadonlyProp() {
			return readonlyProp;
		}
		
		public void setWriteOnlyProp(int writeOnlyProp) {
			this.writeOnlyProp = writeOnlyProp;
		}
		
		public int getNonJavaBeanCompliantProperty() {
			return 0;
		}
		
		public void setNonJavaBeanCompliantProperty(int value) {
			
		}
	}
	
	private static class DummyClassProvider {
		
		private DummyClass dummyClass;
		
		public DummyClass getDummyClass() {
			return dummyClass;
		}
	}
	
}
