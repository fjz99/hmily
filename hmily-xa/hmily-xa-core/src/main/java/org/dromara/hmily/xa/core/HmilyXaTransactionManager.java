/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.dromara.hmily.xa.core;

import org.dromara.hmily.core.context.HmilyContextHolder;
import org.dromara.hmily.core.context.HmilyTransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HmilyXaTransactionManager .
 *
 * @author sixh chenbin
 */
public class HmilyXaTransactionManager implements TransactionManager {

    private final Logger logger = LoggerFactory.getLogger(HmilyXaTransactionManager.class);

    /**
     * onveniently realize the processing of nested transactions.
     */
    private final ThreadLocal<Stack<Transaction>> tms = new ThreadLocal<>();

    /**
     * Store transId->Transaction.
     * Note that this map only stores the first Transaction instance if there has many thread local Transaction,
     * as calling that Transaction's rollback() or commit() will propagate cmd to other thread local Transaction.
     */
    private final Map<String, Transaction> transactionMap = new ConcurrentHashMap<>();

    /**
     * Instantiates a new Hmily xa transaction manager.
     */
    public HmilyXaTransactionManager() {

    }

    /**
     * Initialized hmily xa transaction manager.
     *
     * @return the hmily xa transaction manager
     */
    public static HmilyXaTransactionManager initialized() {
        return new HmilyXaTransactionManager();
    }

    /**
     * Gets transaction.
     *
     * @return the transaction
     */
    @Override
    public Transaction getTransaction() {
        return getThreadTransaction();
    }

    @Override
    public void resume(final Transaction transaction) throws InvalidTransactionException, IllegalStateException, SystemException {
        if (transaction == null) {
            this.clearTxTotr();
            return;
        }
        Transaction thm1 = this.getTransaction();
        if (thm1 != null) {
            if (thm1.equals(transaction)) {
                return;
            }
            logger.warn("The transaction of this thread is consistent with the one you passed in");
            throw new IllegalStateException("The transaction of this thread is consistent with the one you passed in");
        }
        boolean b = !(transaction instanceof TransactionImpl);
        if (b) {
            throw new InvalidTransactionException("transaction not TransactionImpl");
        }

        TransactionImpl inTx = (TransactionImpl) transaction;
        XaState xaState = XaState.valueOf(inTx.getStatus());
        switch (xaState) {
            case STATUS_ACTIVE:
            case STATUS_COMMITTING:
            case STATUS_PREPARING:
                break;
            default:
                throw new InvalidTransactionException("transaction state invalid");
        }
        setTxTotr(transaction);
        try {
            inTx.doEnList(null, XAResource.TMRESUME);
        } catch (RollbackException e) {
            throw new SystemException("RollbackException" + e.getMessage());
        }
    }

    /**
     * Gets thread transaction.
     *
     * @return the thread transaction
     */
    public Transaction getThreadTransaction() {
        synchronized (tms) {
            Stack<Transaction> stack = tms.get();
            if (stack == null) {
                return null;
            }
            return stack.peek();
        }
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {
        Transaction transaction = this.getTransaction();
        if (transaction == null) {
            throw new IllegalStateException("cannot get Transaction for setRollbackOnly");
        }
        transaction.setRollbackOnly();
    }

    @Override
    public void setTransactionTimeout(final int seconds) throws SystemException {

    }

    @Override
    public Transaction suspend() throws SystemException {
        Transaction transaction = this.getTransaction();
        if (transaction != null) {
            if (transaction instanceof TransactionImpl) {
                ((TransactionImpl) transaction).doDeList(XAResource.TMSUSPEND);
            }
            this.clearTxTotr();
        }
        return transaction;
    }

    @Override
    public void begin() {
        //开始一个事务.
        Transaction threadTransaction = getThreadTransaction();
        Transaction rct = threadTransaction;
        //xa;
        if (threadTransaction != null) {
            TransactionImpl tx = (TransactionImpl) rct;
            rct = tx.createSubTransaction();
            //线程本地的嵌套事务的话，会只有一个主协调者，然后list中存放每个从协调者，即使多级嵌套也是这样（即多级嵌套会把多层规整成一层）
            //根节点也有从协调者
            //最终事务提交的时候，会调用主协调者进行提交
            //这里不考虑事务的传播级别的问题，对于嵌套事务，spring会自动在最后提交
            //这就代表hmily的事务隔离级别不支持NETSED，只能用REQUIRED
        } else {
            boolean hasSuper = false;
            HmilyTransactionContext context = HmilyContextHolder.get();
            XidImpl xId;
            if (context != null && context.getXaParticipant() != null) {
                xId = new XidImpl(context.getXaParticipant().getGlobalId(),
                        context.getXaParticipant().getBranchId());
                hasSuper = true;
            } else {
                xId = new XidImpl();
            }
            //Main business coordinator
            rct = new TransactionImpl(xId, hasSuper);
            transactionMap.put(xId.getGlobalId(), rct);
        }
        setTxTotr(rct);
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        Transaction threadTransaction = getThreadTransaction();
        if (threadTransaction == null) {
            throw new IllegalStateException("Transaction is null,can not commit");
        }
        try {
            threadTransaction.commit();
        } finally {
            clearTxTotr();
        }
    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        Transaction transaction = this.getTransaction();
        if (transaction == null) {
            throw new IllegalStateException("Transaction is null,can not rollback");
        }
        try {
            transaction.rollback();
        } finally {
            clearTxTotr();
        }
    }

    @Override
    public int getStatus() throws SystemException {
        Transaction transaction = this.getTransaction();
        if (transaction == null) {
            throw new IllegalStateException("Transaction is null,can not getStatus");
        }
        return transaction.getStatus();
    }

    private boolean txCanRollback(final Transaction transaction) throws SystemException {
        if (transaction.getStatus() == Status
                .STATUS_MARKED_ROLLBACK) {
            transaction.rollback();
            return true;
        }
        return false;
    }

    /**
     * Gets state.
     *
     * @return the state
     * @throws SystemException the system exception
     */
    public Integer getState() throws SystemException {
        Transaction transaction = getTransaction();
        if (transaction == null) {
            return XaState.STATUS_NO_TRANSACTION.getState();
        }
        return transaction.getStatus();
    }

    private void clearTxTotr() {
        synchronized (tms) {
            Stack<Transaction> stack = tms.get();
            Transaction pop = stack.pop();
            if (stack.empty()) {
                tms.remove();
                transactionMap.forEach((k, v) -> {
                    if (v == pop) {
                        transactionMap.remove(k);
                    }
                });
            }
        }
    }

    /**
     * tx  to threadLocal.
     */
    private void setTxTotr(final Transaction transaction) {
        synchronized (tms) {
            Stack<Transaction> stack = tms.get();
            if (stack == null) {
                stack = new Stack<>();
                tms.set(stack);
            }
            stack.push(transaction);
        }
    }

    /**
     * 把事务标记为回滚状态.
     *
     * @param transId 事务id.
     */
    public void markTransactionRollback(final String transId) {
        if (transactionMap.containsKey(transId)) {
            logger.warn("When rollback cmd is received, the business logic of transaction {} isn't stopped, "
                    + "maybe there is a RPC timout.", transId);
            try {
                transactionMap.get(transId).setRollbackOnly();
            } catch (SystemException e) {
                logger.error("err setRollbackOnly", e);
            }
        }
    }
}
