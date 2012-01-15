package com.senseidb.indexing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Text {
	String name() default "";
	String store() default "NO";
	String index() default "NOT_ANALYZED_NO_NORMS";
	String termVector() default "NO";
}
