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

package org.dromara.hmily.xa.rpc.springcloud.parameter;

import org.dromara.hmily.common.utils.LogUtil;
import org.dromara.hmily.core.context.HmilyTransactionContext;
import org.dromara.hmily.core.mediator.RpcMediator;
import org.dromara.hmily.core.mediator.RpcParameterLoader;
import org.dromara.hmily.spi.HmilySPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * The type springCloud parameter loader.
 *
 * @author xiaoyu
 */
@HmilySPI(value = "springCloud")
public class SpringCloudParameterLoader implements RpcParameterLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringCloudParameterLoader.class);

    @Override
    public HmilyTransactionContext load() {
        HmilyTransactionContext hmilyTransactionContext = null;
        try {
            final RequestAttributes requestAttributes = RequestContextHolder.currentRequestAttributes();
            hmilyTransactionContext = RpcMediator.getInstance().acquire(key -> ((ServletRequestAttributes) requestAttributes).getRequest().getHeader(key));
        } catch (IllegalStateException ex) {
            LogUtil.warn(LOGGER, () -> "can not acquire request info:" + ex.getLocalizedMessage());
        }
        return hmilyTransactionContext;
    }
}
