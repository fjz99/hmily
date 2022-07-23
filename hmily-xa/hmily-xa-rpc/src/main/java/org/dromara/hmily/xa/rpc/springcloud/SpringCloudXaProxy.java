package org.dromara.hmily.xa.rpc.springcloud;


import feign.Client;
import feign.Request;
import feign.RequestTemplate;
import feign.Response;
import org.dromara.hmily.core.context.HmilyContextHolder;
import org.dromara.hmily.core.context.HmilyTransactionContext;
import org.dromara.hmily.core.context.XaParticipant;
import org.dromara.hmily.xa.rpc.RpcXaProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.lang.NonNull;

import java.util.Map;

public class SpringCloudXaProxy implements RpcXaProxy, BeanFactoryAware {
    private static final Logger LOGGER = LoggerFactory.getLogger (SpringCloudXaProxy.class);

    private final Client client = new Client.Default (null, null);
    //    private final Method method;
//    private final Object target;
//    private final Object[] args;
    private HmilyTransactionContext context;
    private RequestTemplate requestTemplate;
    private Request.Options options;

    @Override
    public Integer cmd(XaCmd cmd, Map<String, Object> params) {
        if (cmd == null) {
            LOGGER.warn ("cmd is null");
            return NO;
        }

        context.getXaParticipant ().setCmd (cmd.name ());
        HmilyContextHolder.set (context);
//        RpcMediator.getInstance ().transmit (requestTemplate::header, context);
        try {
//            method.invoke (target, args);

            Response response = client.execute (requestTemplate.request (), options);//会触发interceptor
            if (response.status () == 200) {
                return YES;
            }
        } catch (Throwable e) {
            LOGGER.error ("cmd {} err", cmd.name (), e);
        }
        return EXC;
    }

    @Override
    public int getTimeout() {
        return options.readTimeoutMillis ();
    }

    @Override
    public void init(XaParticipant participant) {
        context = new HmilyTransactionContext ();
        context.setXaParticipant (participant);
        HmilyContextHolder.set (context);
        FeignRequestInterceptor.registerXaProxy (context.getTransId (), this);
    }

    public void setRequestTemplate(RequestTemplate requestTemplate) {
        this.requestTemplate = requestTemplate;
    }

    @Override
    public boolean equals(RpcXaProxy xaProxy) {
        if (xaProxy instanceof SpringCloudXaProxy) {
            return ((SpringCloudXaProxy) xaProxy).requestTemplate.url ().equals (requestTemplate.url ());
        }
        return false;
    }

    @Override
    public void setBeanFactory(@NonNull BeanFactory beanFactory) throws BeansException {
        options = beanFactory.getBean (Request.Options.class);
    }
}
