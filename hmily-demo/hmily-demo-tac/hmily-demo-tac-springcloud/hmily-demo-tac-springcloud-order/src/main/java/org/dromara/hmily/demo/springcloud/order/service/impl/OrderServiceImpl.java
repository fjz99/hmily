/*
 * Copyright 2017-2021 Dromara.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dromara.hmily.demo.springcloud.order.service.impl;

import org.dromara.hmily.common.utils.IdWorkerUtils;
import org.dromara.hmily.demo.common.order.entity.Order;
import org.dromara.hmily.demo.common.order.enums.OrderStatusEnum;
import org.dromara.hmily.demo.common.order.mapper.OrderMapper;
import org.dromara.hmily.demo.springcloud.order.service.OrderService;
import org.dromara.hmily.demo.springcloud.order.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Date;


/**
 * OrderServiceImpl.
 *
 * @author zhangzhi
 */
@Service("orderService")
public class OrderServiceImpl implements OrderService {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderServiceImpl.class);

    private final OrderMapper orderMapper;

    private final PaymentService paymentService;

    @Autowired(required = false)
    public OrderServiceImpl(OrderMapper orderMapper, PaymentService paymentService) {
        this.orderMapper = orderMapper;
        this.paymentService = paymentService;
    }

    @Override
    public String orderPay(Integer count, BigDecimal amount) {
        Order order = saveOrder(count, amount);
        long start = System.currentTimeMillis();
        paymentService.makePayment(order);
        System.out.println("hmily-cloud分布式事务耗时：" + (System.currentTimeMillis() - start));
        return "success";
    }
    
    @Override
    public String testOrderPay(Integer count, BigDecimal amount) {
        Order order = saveOrder(count, amount);
        paymentService.testMakePayment(order);
        return "success";
    }
    
    @Override
    public String mockInventoryWithTryException(Integer count, BigDecimal amount) {
        Order order = saveOrder(count, amount);
        return paymentService.mockPaymentInventoryWithTryException(order);
    }
    
    @Override
    public String mockAccountWithTryException(Integer count, BigDecimal amount) {
        Order order = saveOrder(count, amount);
        return paymentService.mockPaymentAccountWithTryException(order);
    }
    
    /**
     * 模拟在订单支付操作中，库存在try阶段中的timeout
     *
     * @param count  购买数量
     * @param amount 支付金额
     * @return string
     */
    @Override
    public String mockInventoryWithTryTimeout(Integer count, BigDecimal amount) {
        Order order = saveOrder(count, amount);
        return paymentService.mockPaymentInventoryWithTryTimeout(order);
    }
    
    @Override
    public String mockAccountWithTryTimeout(Integer count, BigDecimal amount) {
        Order order = saveOrder(count, amount);
        return paymentService.mockPaymentAccountWithTryTimeout(order);
    }
    
    @Override
    public String orderPayWithNested(Integer count, BigDecimal amount) {
        Order order = saveOrder(count, amount);
        return paymentService.makePaymentWithNested(order);
    }
    
    @Override
    public String orderPayWithNestedException(Integer count, BigDecimal amount) {
        Order order = saveOrder(count, amount);
        return paymentService.makePaymentWithNestedException(order);
    }

    /**
     * 模拟在订单支付操作中，库存在Confirm阶段中的timeout
     *
     * @param count  购买数量
     * @param amount 支付金额
     * @return string
     */
    @Override
    public String mockInventoryWithConfirmTimeout(Integer count, BigDecimal amount) {
        Order order = saveOrder(count, amount);
        paymentService.mockPaymentInventoryWithConfirmTimeout(order);
        return "success";
    }

    @Override
    public void updateOrderStatus(Order order) {
        orderMapper.update(order);
    }

    @Override
    public String orderPayWithReadCommitted(Integer count, BigDecimal amount) {
        Order order = saveOrder(count, amount);
        long start = System.currentTimeMillis();
        paymentService.makePaymentWithReadCommitted(order);
        System.out.println("切面耗时：" + (System.currentTimeMillis() - start));
        return "success";
    }

    private Order saveOrder(Integer count, BigDecimal amount) {
        final Order order = buildOrder(count, amount);
        orderMapper.save(order);
        return order;
    }
    
    private Order buildOrder(Integer count, BigDecimal amount) {
        LOGGER.debug("构建订单对象");
        Order order = new Order();
        order.setCreateTime(new Date());
        order.setNumber(String.valueOf(IdWorkerUtils.getInstance().createUUID()));
        //demo中的表里只有商品id为 1的数据
        order.setProductId("1");
        order.setStatus(OrderStatusEnum.NOT_PAY.getCode());
        order.setTotalAmount(amount);
        order.setCount(count);
        //demo中 表里面存的用户id为10000
        order.setUserId("10000");
        return order;
    }
}
