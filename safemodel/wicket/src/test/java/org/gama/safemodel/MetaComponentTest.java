package org.gama.safemodel;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.wicket.Page;
import org.apache.wicket.markup.html.GenericWebPage;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.protocol.http.WebApplication;
import org.apache.wicket.util.tester.WicketTester;
import org.gama.lang.exception.Exceptions;
import org.gama.safemodel.component.PhoneComponent;
import org.gama.safemodel.metamodel.MetaPhoneComponent;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.gama.safemodel.MetaComponentTest.SingleComponentPage.COMPONENT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class MetaComponentTest {
	
	@Test
	@UseDataProvider(value = "testTransformData", location = MetaModelPathComponentBuilderTest.class)
	public void testGivePathFromTop(MetaComponent metaModel, String expected) throws Exception {
		assertEquals(expected, metaModel.givePathFromRoot());
	}
	
	/**
	 * A test that's not really one: just to show how a MetaComponent can be tested.
	 * Of course there are some traps: in our test the MetaComponent is expected to add the tested one during construction (bad pratice)
	 * or onInitialize() phase.
	 * 
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	@Test
	public void testHierarchyChecker() throws IllegalAccessException, InstantiationException {
		// Creation of the Wicket test container, with a default (really basic) application
		WicketTester tester = new WicketTester(new WebApplication() {
			@Override
			public Class<? extends Page> getHomePage() {
				// we don't care about the Home page since we'll ask for the page we want during test
				return null;
			}
		});
		
		// creation of a basic component hierarchy with our tested component
		MetaPhoneComponent<MetaModel, Page> metaModelMetaPhoneComponent = new MetaPhoneComponent<>();
		MetaComponent<MetaPhoneComponent, PhoneComponent, Label> tested = metaModelMetaPhoneComponent.number;
		
		PhoneComponent testInstanciation = new ClassDecorator<>(tested.getDescription().getDeclaringContainer()).newInstance(COMPONENT_ID);
		SingleComponentPage dummyPage = new SingleComponentPage();
		dummyPage.add(testInstanciation);
		
		// start and render the test page
		tester.startPage(dummyPage);
		// assert rendered page class
		tester.assertRenderedPage(SingleComponentPage.class);
		// assert the page contains the component
		// WARN: the component is supposed to be added and visible via the constructor/addition to the page
		tester.assertComponent(COMPONENT_ID + ":" + tested.givePathFromRoot(), tested.getDescription().getType());
		Label label = tested.get(testInstanciation);
		assertNotNull(label);
	}
	
	/**
	 * A Page that contains only one component. Used to put a component and tests its presence.
	 */
	static class SingleComponentPage extends GenericWebPage {
		
		static final String COMPONENT_ID = "dummyId";
		
	}
	
	/**
	 * Little class to ease {@link Class} usage. To be shared ?
	 * @param <C>
	 */
	private static class ClassDecorator<C> {
		
		private final Class<C> decorated;
		
		public ClassDecorator(Class<C> decorated) {
			this.decorated = decorated;
		}
		
		public C newInstance() {
			try {
				return decorated.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw Exceptions.asRuntimeException(e);
			}
		}
		
		public C newInstance(Object ... args) {
			if (args.length == 0) {
				return newInstance();
			} else {
				Class[] parametersTypes = new Class[args.length];
				for (int i = 0; i < args.length; i++) {
					parametersTypes[i] = args[i].getClass();
				}
				try {
					Constructor<C> constructor = decorated.getConstructor(parametersTypes);
					return constructor.newInstance(args);
				} catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
					throw Exceptions.asRuntimeException(e);
				}
			}
		}
	}
}
