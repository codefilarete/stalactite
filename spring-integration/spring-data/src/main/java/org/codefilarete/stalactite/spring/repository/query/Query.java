package org.codefilarete.stalactite.spring.repository.query;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.QueryAnnotation;

/**
 * Annotation to declare a SQL query on repository methods.
 *
 * @author Guillaume Mary
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@QueryAnnotation
@Documented
public @interface Query {
	
	/**
	 * Defines the SQL query to be executed when the annotated method is called.
	 */
	String value() default "";
}
