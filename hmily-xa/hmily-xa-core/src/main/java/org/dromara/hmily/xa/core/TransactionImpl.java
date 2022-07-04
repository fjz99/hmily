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

import org.dromara.hmily.xa.core.timer.TimerRemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionRolledbackException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * TransactionImpl .
 * a thread only .
 *
 * @author sixh chenbin
 */
public class TransactionImpl implements Transaction, TimerRemovalListener<Resource> {

    private final Logger logger = LoggerFactory.getLogger(TransactionImpl.class);

    private final XidImpl xid;

    private SubCoordinator subCoordinator;//子协调器

    //所有的resource
    private final List<XAResource> enlistResourceList = Collections.synchronizedList(new ArrayList<>());

    private List<XAResource> delistResourceList;//用于保存挂起的？

    private final TransactionContext context;

    private final boolean hasSuper;

    /**
     * Instantiates a new Transaction.
     *
     * @param xId      the x id
     * @param hasSuper the has super
     */
    TransactionImpl(final XidImpl xId, final boolean hasSuper) {
        this.xid = xId;
        this.hasSuper = hasSuper;
        context = new TransactionContext(null, xId);
        //todo:这里还要设置超时器.
        subCoordinator(true, true);
    }

    /**
     * Coordinator
     * Instantiates a new Transaction.
     *
     * @param impl the
     */
    private TransactionImpl(final TransactionImpl impl) {
        this.xid = impl.getXid().newBranchId();
        context = impl.getContext();
        this.delistResourceList = null;
        hasSuper = impl.hasSuper;
        subCoordinator(true, true);
    }

    //需要支持不是XA的事务，比如本地直接提交了
    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        Finally oneFinally = context.getOneFinally();
        if (oneFinally != null) {//其实就是协调器
            try {
                if (hasSuper) {//如果有父事务，那就结束这个resource，让父事务调用
                    doDeList(XAResource.TMSUCCESS);
                } else {
                    oneFinally.commit();//最顶层父事务会调用commit，对应协调器的commit
                }
            } catch (TransactionRolledbackException e) {
                logger.error("error rollback", e);
                throw new RollbackException();
            } catch (RemoteException e) {
                logger.error("error", e);
                throw new SystemException(e.getMessage());
            }
            return;//return!!!!
        }
        //什么情况下会没有主coordinator？
        //我认为是无法触发的，因为构造器中会保证一定有主coordinator
        if (subCoordinator != null) {
            try {
                //第1阶段提交.
                //如果没有主coordinator，那就直接提交了
                subCoordinator.onePhaseCommit();
            } catch (Exception e) {
                logger.error("onePhaseCommit()");
                throw new RollbackException();
            }
        }
    }

    /**
     * Do en list.
     * rpc时会加入远程的RpcResource
     * 本地数据源也会加入
     *
     * @param xaResource the xa resource
     * @param flag       the flag
     * @throws SystemException   the system exception
     * @throws RollbackException the rollback exception
     */
    public void doEnList(final XAResource xaResource, final int flag) throws SystemException, RollbackException {
        //xaResource;
        if (flag == XAResource.TMJOIN
                || flag == XAResource.TMNOFLAGS) {
            //这里需要处理不同的xa事务数据.加入XA事务
            enlistResource(xaResource);
        } else if (flag == XAResource.TMRESUME) {
            //进行事务的恢复.
            //是恢复的话，就回复所有的挂起事务
            if (delistResourceList != null) {
                for (final XAResource resource : delistResourceList) {
                    this.enlistResource(resource);
                }
            }
        }
        delistResourceList = null;
    }

    /**
     * Do de list.
     * 先复制到de list，然后清除en list
     *
     * @param flag the flag
     * @throws SystemException the system exception
     */
    public void doDeList(final int flag) throws SystemException {
        delistResourceList = new ArrayList<>(enlistResourceList);
        for (XAResource resource : delistResourceList) {
            delistResource(resource, flag);
        }
    }

    @Override
    public boolean delistResource(final XAResource xaResource, final int flag) throws
            IllegalStateException, SystemException {
        if (!enlistResourceList.contains(xaResource)) {
            return false;
        }
        HmilyXaResource myResoure = (HmilyXaResource) xaResource;
        try {
            //flags - TMSUCCESS、TMFAIL 或 TMSUSPEND 之一。
            myResoure.end(flag);//end，代表提交前一个resource的结束
            enlistResourceList.remove(xaResource);
            return true;
        } catch (XAException e) {
            logger.info("xa resource end,{}", HmilyXaException.getMessage(e), e);
        }
        return false;
    }

    //一个事务关联一个coordinator，和一个子coordinator，子coordinator关联这个事务的所有resource
    @Override
    public boolean enlistResource(final XAResource xaResource) throws
            RollbackException, IllegalStateException, SystemException {
        //is null .
        if (subCoordinator == null) {
            subCoordinator(false, false);
            if (subCoordinator == null) {
                throw new SystemException("not create subCoordinator");
            }
        }
        XidImpl resId = this.subCoordinator.nextXid(this.xid);
        HmilyXaResource hmilyXaResource = new HmilyXaResource(resId, xaResource);
        boolean found = subCoordinator.addXaResource(hmilyXaResource);
        int flag = found ? XAResource.TMJOIN : XAResource.TMNOFLAGS;
        try {
            //如果加入了一个在挂起队列中的resource
            if (delistResourceList != null && delistResourceList.contains(hmilyXaResource)) {
                flag = XAResource.TMRESUME;
            }
            // TMNOFLAGS、TMJOIN 或 TMRESUME 之一。
            hmilyXaResource.start(flag);//resource生命周期启动callback
        } catch (XAException e) {
            logger.error("{}", HmilyXaException.getMessage(e), e);
            throw new IllegalStateException(e);
        }
        //不会重复添加resource
        if (!enlistResourceList.contains(hmilyXaResource)) {
            enlistResourceList.add(hmilyXaResource);
        }
        return true;
    }

    @Override
    public int getStatus() throws SystemException {
        if (context.getCoordinator() != null) {
            return context.getCoordinator().getState().getState();
        }
        return subCoordinator.getState().getState();
    }

    @Override
    public void registerSynchronization(final Synchronization synchronization) throws
            RollbackException, IllegalStateException, SystemException {
        if (synchronization == null) {
            return;
        }
        if (subCoordinator == null) {
            subCoordinator(false, true);
        }
        subCoordinator.addSynchronization(synchronization);
    }

    @Override
    public void rollback() throws IllegalStateException, SystemException {
        Finally oneFinally = context.getOneFinally();
        if (oneFinally != null) {
            if (hasSuper) {
                doDeList(XAResource.TMSUCCESS);
            } else {
                try {
                    oneFinally.rollback();
                } catch (RemoteException e) {
                    logger.error("rollback error {}", e.getMessage(), e);
                    throw new SystemException();
                }
            }
            return;
        }
        if (subCoordinator != null) {
            try {
                subCoordinator.rollback();
            } catch (RemoteException e) {
                logger.error("rollback error {}", e.getMessage(), e);
                throw new SystemException();
            }
        }
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {
        Coordinator coordinator = context.getCoordinator();
        if (coordinator != null) {
            coordinator.setRollbackOnly();
        }
        if (subCoordinator == null) {
            subCoordinator(false, false);
        }
        subCoordinator.setRollbackOnly();
    }

    /**
     * 只有在构造器里创建才会创建父coordinator
     * 父coordinator可能为null，假如使用了默认的无参构造器
     * @param newCoordinator 是否允许创建协调器
     */
    private void subCoordinator(final boolean newCoordinator, final boolean stateActive) {
        try {
            subCoordinator = new SubCoordinator(this, this.hasSuper);
            if (!stateActive) {
                return;
            }
            Coordinator coordinator = context.getCoordinator();
            if (newCoordinator && coordinator == null) {
                //懒加载
                coordinator = new Coordinator(xid, this.hasSuper);
                context.setCoordinator(coordinator);
                if (context.getOneFinally() == null) {
                    context.setOneFinally(coordinator);
                }
                //事务超时时间30000s
                coordinator.getTimer().put(coordinator, 30000, TimeUnit.SECONDS);
                coordinator.getTimer().addRemovalListener(this);
            }
            coordinator.addCoordinators(subCoordinator);
        } catch (Exception ex) {
            logger.error("build SubCoordinator error");
        }
    }

    /**
     * Gets xid.
     *
     * @return the xid
     */
    public XidImpl getXid() {
        return xid;
    }

    /**
     * Gets context.
     *
     * @return the context
     */
    public TransactionContext getContext() {
        return context;
    }

    /**
     * 创建一个子任务.
     *
     * @return the transaction
     */
    public TransactionImpl createSubTransaction() {
        return new TransactionImpl(this);
    }

    @Override
    public void onRemoval(final Resource value, final Long expire, final Long elapsed) {
    }
}
