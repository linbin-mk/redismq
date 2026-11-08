package com.mk.mq.spring;

import com.mk.mq.ConsumerOptions;
import com.mk.mq.MessageContext;
import com.mk.mq.MqConsumer;
import com.mk.mq.RedisMq;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.Environment;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** 上下文启动后注册监听器，停止时先等待消费结束，再交还 Redis 生命周期。 */
public final class RedisMqListenerRegistry implements SmartLifecycle {
    private final RedisMq mq;
    private final RedisMqProperties properties;
    private final ConfigurableListableBeanFactory beans;
    private final Environment environment;
    private final List<MqConsumer<?>> consumers = new ArrayList<>();
    private volatile boolean running;

    RedisMqListenerRegistry(RedisMq mq, RedisMqProperties properties, ConfigurableListableBeanFactory beans, Environment environment) {
        this.mq = mq;
        this.properties = properties;
        this.beans = beans;
        this.environment = environment;
    }

    @Override
    public synchronized void start() {
        if (running) return;
        List<Registration> registrations = new ArrayList<>();
        // 仅检查已初始化单例，不为发现监听器而创建懒加载或多例业务对象。
        for (String name : beans.getSingletonNames()) {
            Object bean = beans.getSingleton(name);
            if (bean == null) continue;
            Map<Method, RedisMqListener> methods = MethodIntrospector.selectMethods(AopUtils.getTargetClass(bean),
                    (MethodIntrospector.MetadataLookup<RedisMqListener>) method -> AnnotatedElementUtils.findMergedAnnotation(method, RedisMqListener.class));
            methods.forEach((method, listener) -> registrations.add(validate(bean, method, listener)));
        }
        try {
            for (Registration registration : registrations) register(registration);
            running = true;
        } catch (RuntimeException e) {
            consumers.forEach(MqConsumer::close);
            consumers.clear();
            throw e;
        }
    }

    private Registration validate(Object bean, Method method, RedisMqListener listener) {
        Class<?>[] parameters = method.getParameterTypes();
        if (method.getReturnType() != void.class || parameters.length < 1 || parameters.length > 2
                || (parameters.length == 2 && parameters[1] != MessageContext.class)) {
            throw new IllegalArgumentException("MQ 监听方法必须返回 void，参数为消息体及可选的 MessageContext：" + method);
        }
        String queue = environment.resolveRequiredPlaceholders(listener.queue());
        mq.storageKeys(queue);
        Method invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
        ReflectionUtils.makeAccessible(invocable);
        return new Registration(bean, invocable, queue, parameters[0], properties.consumerOptions(listener.concurrency()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void register(Registration registration) {
        consumers.add(mq.consume(registration.queue(), (Class) registration.type(), (payload, context) -> {
            try {
                if (registration.method().getParameterCount() == 1) registration.method().invoke(registration.bean(), payload);
                else registration.method().invoke(registration.bean(), payload, context);
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof Exception exception) throw exception;
                if (e.getCause() instanceof Error error) throw error;
                throw new IllegalStateException("MQ 监听方法执行失败", e.getCause());
            }
        }, registration.options()));
    }

    @Override
    public synchronized void stop() {
        running = false;
        consumers.forEach(MqConsumer::close);
        consumers.clear();
    }
    @Override public boolean isRunning() { return running; }
    @Override public int getPhase() { return Integer.MAX_VALUE - 100; }
    private record Registration(Object bean, Method method, String queue, Class<?> type, ConsumerOptions options) { }
}
