/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Base class for Visor tasks intended to execute job on coordinator node.
 */
public abstract class VisorCoordinatorNodeTask<A, R> extends VisorOneNodeTask<A, R> {
    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<A> arg) {
        ClusterNode crd = ignite.context().discovery().discoCache().oldestAliveServerNode();

        Collection<UUID> nids = new ArrayList<>(1);

        nids.add(crd == null ? ignite.localNode().id() : crd.id());

        return nids;
    }
}
