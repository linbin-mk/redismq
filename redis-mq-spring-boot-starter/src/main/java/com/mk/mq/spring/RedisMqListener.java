package com.mk.mq.spring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 标记返回值为 void 的同步处理方法；第二个参数可选，类型为 MessageContext。 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RedisMqListener {
    String queue();
    int concurrency() default 0;
}
