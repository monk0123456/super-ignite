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

package org.apache.ignite.internal.processors.security;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.security.SecurityUtils.nodeSecurityContext;

/**
 * Default {@code IgniteSecurity} implementation.
 * <p>
 * {@code IgniteSecurityProcessor} serves here as a facade with is exposed to Ignite internal code,
 * while {@code GridSecurityProcessor} is hidden and managed from {@code IgniteSecurityProcessor}.
 * <p>
 * This implementation of {@code IgniteSecurity} is responsible for:
 * <ul>
 *     <li>Keeping and propagating authenticated security contexts for cluster nodes;</li>
 *     <li>Delegating calls for all actions to {@code GridSecurityProcessor};</li>
 *     <li>Managing sandbox and proving point of entry to the internal sandbox API.</li>
 * </ul>
 */
public class IgniteSecurityProcessor implements IgniteSecurity, GridProcessor {
    /** Internal attribute name constant. */
    public static final String ATTR_GRID_SEC_PROC_CLASS = "grid.security.processor.class";

    /** Current security context. */
    private final ThreadLocal<SecurityContext> curSecCtx = ThreadLocal.withInitial(this::localSecurityContext);

    /** Grid kernal context. */
    private final GridKernalContext ctx;

    /** Security processor. */
    private final GridSecurityProcessor secPrc;

    /** Must use JDK marshaller for Security Subject. */
    private final JdkMarshaller marsh;

    /** Map of security contexts. Key is the node's id. */
    private final Map<UUID, SecurityContext> secCtxs = new ConcurrentHashMap<>();

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Grid kernal context.
     * @param secPrc Security processor.
     */
    public IgniteSecurityProcessor(GridKernalContext ctx, GridSecurityProcessor secPrc) {
        assert ctx != null;
        assert secPrc != null;

        this.ctx = ctx;
        this.log = ctx.log(IgniteSecurityProcessor.class);
        this.secPrc = secPrc;

        marsh = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(SecurityContext secCtx) {
        assert secCtx != null;

        secPrc.touch(secCtx);

        SecurityContext old = curSecCtx.get();

        curSecCtx.set(secCtx);

        return new OperationSecurityContext(this, old);
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(UUID subjId) {
        ClusterNode node = Optional.ofNullable(ctx.discovery().node(subjId))
            .orElseGet(() -> ctx.discovery().historicalNode(subjId));

        SecurityContext res = node != null ? secCtxs.computeIfAbsent(subjId,
            uuid -> nodeSecurityContext(marsh, U.resolveClassLoader(ctx.config()), node))
            : secPrc.securityContext(subjId);

        if (res == null)
            throw new IllegalStateException("Failed to find security context for subject with given ID : " + subjId);

        return withContext(res);
    }

    /** {@inheritDoc} */
    @Override public SecurityContext securityContext() {
        SecurityContext res = curSecCtx.get();

        assert res != null;

        return res;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred)
        throws IgniteCheckedException {
        return secPrc.authenticateNode(node, cred);
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return secPrc.isGlobalNodeAuthentication();
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        return secPrc.authenticate(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        return secPrc.authenticatedSubjects();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        return secPrc.authenticatedSubject(subjId);
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        secPrc.onSessionExpired(subjId);
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm) throws SecurityException {
        SecurityContext secCtx = curSecCtx.get();

        assert secCtx != null;

        secPrc.authorize(name, perm, secCtx);
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(ATTR_GRID_SEC_PROC_CLASS, secPrc.getClass().getName());

        secPrc.start();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        secPrc.stop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        ctx.event().addDiscoveryEventListener(
            (evt, discoCache) -> secCtxs.remove(evt.eventNode().id()), EVT_NODE_FAILED, EVT_NODE_LEFT
        );

        secPrc.onKernalStart(active);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        secPrc.onKernalStop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        secPrc.collectJoiningNodeData(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        secPrc.collectGridNodeData(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        secPrc.onGridDataReceived(data);
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        secPrc.onJoiningNodeDataReceived(data);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        secPrc.printMemoryStats();
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node) {
        IgniteNodeValidationResult res = validateSecProcClass(node);

        return res != null ? res : secPrc.validateNode(node);
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        IgniteNodeValidationResult res = validateSecProcClass(node);

        return res != null ? res : secPrc.validateNode(node, discoData);
    }

    /** {@inheritDoc} */
    @Override public @Nullable DiscoveryDataExchangeType discoveryDataType() {
        return secPrc.discoveryDataType();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        secPrc.onDisconnected(reconnectFut);
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteInternalFuture<?> onReconnected(
        boolean clusterRestarted) throws IgniteCheckedException {
        return secPrc.onReconnected(clusterRestarted);
    }

    /**
     * Getting local node's security context.
     *
     * @return Security context of local node.
     */
    private SecurityContext localSecurityContext() {
        return nodeSecurityContext(marsh, U.resolveClassLoader(ctx.config()), ctx.discovery().localNode());
    }

    /**
     * Validates that remote node's grid security processor class is the same as local one.
     *
     * @param node Joining node.
     * @return Validation result or {@code null} in case of success.
     */
    private IgniteNodeValidationResult validateSecProcClass(ClusterNode node) {
        String rmtCls = node.attribute(ATTR_GRID_SEC_PROC_CLASS);
        String locCls = secPrc.getClass().getName();

        boolean securityMsgSupported = IgniteFeatures.allNodesSupports(
            ctx,
            ctx.discovery().allNodes(),
            IgniteFeatures.IGNITE_SECURITY_PROCESSOR
        );

        if (securityMsgSupported && !F.eq(locCls, rmtCls)) {
            return new IgniteNodeValidationResult(node.id(),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, ctx.localNodeId(), node.id(), locCls, rmtCls),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, node.id(), ctx.localNodeId(), rmtCls, locCls));
        }

        return null;
    }

    /**
     * @return root security processor.
     */
    public GridSecurityProcessor gridSecurityProcessor() {
        return secPrc;
    }
}
