package com.sensei.search.client.json;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CustomJsonHandler {
    Class<? extends JsonHandler<?>> value();
    boolean flatten() default false;
    boolean overrideColumnName() default false;
}
