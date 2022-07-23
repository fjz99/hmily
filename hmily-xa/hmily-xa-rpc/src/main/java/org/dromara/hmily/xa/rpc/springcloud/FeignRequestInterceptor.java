package org.dromara.hmily.xa.rpc.springcloud;


import feign.RequestInterceptor;
import feign.RequestTemplate;
import org.dromara.hmily.core.context.HmilyContextHolder;
import org.dromara.hmily.core.context.HmilyTransactionContext;
import org.dromara.hmily.core.mediator.RpcMediator;
import org.springframework.core.Ordered;

import java.util.HashMap;
import java.util.Map;

/**
 * 保证如果有context，就一定会设置
 * 保存的是远程rpc产生的，而有rpc则一定会commit或rollback
 */
class FeignRequestInterceptor implements RequestInterceptor, Ordered {
    /**
     * 可能出现多个线程并发Xa事务，但是每个线程内部的事务的prepare、commit、rollback都是单线程的
     */
    private static final ThreadLocal<Map<Long, SpringCloudXaProxy>> map = ThreadLocal.withInitial (HashMap::new);

    public static void main(String[] args) {
        RequestTemplate template = new RequestTemplate ();
        System.out.println (template.url ());
        template.insert (0, "http://xxx.com:8899/");
        System.out.println (template.url ());
        template.insert (0, "http://xxx.com:8899/");
        System.out.println (template.url ());
    }

    public static void registerXaProxy(Long xid, SpringCloudXaProxy xaProxy) {
        map.get ().put (xid, xaProxy);
    }

    @Override
    public void apply(RequestTemplate template) {
        HmilyTransactionContext context = HmilyContextHolder.get ();
        if (context == null)
            return;

        RpcMediator.getInstance ().transmit (template::header, context);

        //处理负载均衡,复制一份requestTemplate,保证路由到同一个服务器
        String cmd = context.getXaParticipant ().getCmd ();
        SpringCloudXaProxy proxy = map.get ().get (context.getTransId ());
        if (proxy != null && cmd != null) {
            proxy.setRequestTemplate (new RequestTemplate (template));
            map.get ().remove (context.getTransId ());
        }

    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

}
