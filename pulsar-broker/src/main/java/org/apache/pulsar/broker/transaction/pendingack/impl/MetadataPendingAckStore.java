/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.transaction.pendingack.impl;

import static org.apache.bookkeeper.mledger.impl.AckSetStateUtil.createPositionWithAckSet;
import static org.apache.bookkeeper.mledger.impl.AckSetStateUtil.getAckSetArrayOrNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore.OpRecord;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnPaths;
import org.apache.pulsar.broker.transaction.metadata.TxnState;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.ScanConsumer;

/**
 * {@link PendingAckStore} for {@code segment://} subscriptions that reads truth from the
 * metadata-store transaction layout (PIP-473) rather than from a per-subscription log.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li><b>Ack</b> — {@link #appendIndividualAck} / {@link #appendCumulativeAck} write a
 *       {@link TxnOp} record under {@code /txn/op/<txnId>-<seq>} with
 *       {@code kind=ACK, segment, subscription, ledgerId, entryId, cumulative}, including the
 *       original batch size and acknowledgement mask. The associated
 *       {@code PendingAckHandle} keeps the in-memory state via the legacy interface.</li>
 *   <li><b>Commit / Abort marks</b> — normally no-ops because the TC owns the lifecycle: it
 *       CAS-updates the header and publishes subscription events. A direct commit remains fenced
 *       while an ACK append is uncertain.</li>
 *   <li><b>State transitions</b> — driven by {@code /txn/subscription-events/&lt;seg&gt;:&lt;sub&gt;-*}
 *       sequence events. The events are wake-ups; the truth is the header. On each notification
 *       we re-read headers for every currently-open txn this subscription is involved in and
 *       call {@code PendingAckHandleImpl.commitTxn} / {@code abortTxn} for those that have gone
 *       terminal. Committed recovery records remain until the persistent whole-entry cursor
 *       covers them; aborted records can be removed after the handle releases their reservations.</li>
 *   <li><b>ACK failures</b> — an uncertain append may leave no op for the TC to discover. After
 *       the handle retains its reservation, a failure-only retry chain reads the real decision.
 *       Only a confirmed abort releases that reservation; unknown or committed uncertainty stays
 *       fail-closed, preserving masks and any durable records for reconstruction.</li>
 *   <li><b>Recovery</b> (Option C) — on {@link #replayAsync}, subscribe to the event stream,
 *       scan {@code idx:acks-by-segment-subscription} for this {@code (segment, sub)}, group by
 *       {@code txnId}, fetch each header, and seed the in-memory open-txn set; then mark the
 *       handle ready and drain any events that fired during recovery.</li>
 * </ul>
 *
 * <p>Same TOCTOU / unbounded-cache caveats documented on {@code MetadataTransactionBuffer} apply
 * here; see that class's javadoc for the TC ordering contract.
 */
@CustomLog
public class MetadataPendingAckStore implements PendingAckStore {

    private static final long UNCERTAIN_ACK_RETRY_MILLIS = 1000;

    private final PersistentSubscription subscription;
    private final TxnMetadataStore txnStore;
    private final String segmentName;
    private final String subscriptionName;

    private final Object lock = new Object();
    /** Transactions awaiting a decision or durable cursor coverage — drained by reconcile. */
    private final Set<String> openTxns = new HashSet<>();
    /**
     * The ack appends in progress, which {@link #closeAsync()} waits for: the ack records of a deleted subscription
     * are deleted once it is closed, and none must land after that. Guarded by {@link #lock}, like the
     * {@link #closed} check of an append.
     */
    private final Set<CompletableFuture<Void>> appendsInProgress = new HashSet<>();
    /** Failed appends retain their exact recovery obligation until a durable decision can be applied. */
    private final Map<String, List<UncertainAppend>> uncertainAppends = new HashMap<>();
    /** Native append admission through the handle's queued reservation installation. */
    private final Map<String, Integer> pendingActivations = new HashMap<>();
    private final Map<String, Long> appendGenerations = new HashMap<>();
    private final Set<String> terminalApplications = new HashSet<>();
    private final Set<String> successfulRetries = new HashSet<>();
    private final Set<String> unknownSuccessfulAppends = new HashSet<>();
    private final Set<String> successfulTransactions = new HashSet<>();
    private final Map<String, List<ExecutorProvider.CleanupTask>> deferredAppendCompletions = new HashMap<>();
    private final Set<String> successfulReads = new HashSet<>();
    private final Set<String> dirtySuccessfulReads = new HashSet<>();
    private ScheduledFuture<?> uncertaintyRetry;
    private boolean uncertaintyRunning;

    private final CompletableFuture<Void> recoveryFuture = new CompletableFuture<>();
    private volatile AutoCloseable eventSubscription;
    private volatile PendingAckHandleImpl handle;
    private volatile ExecutorService handleExecutor;
    private volatile boolean closed;

    public MetadataPendingAckStore(PersistentSubscription subscription, TxnMetadataStore txnStore) {
        this.subscription = subscription;
        this.txnStore = txnStore;
        this.segmentName = subscription.getTopicName();
        this.subscriptionName = subscription.getName();
    }

    // ---- Replay / recovery -------------------------------------------------

    @Override
    public void replayAsync(PendingAckHandleImpl pendingAckHandle, ExecutorService executorService) {
        this.handle = pendingAckHandle;
        this.handleExecutor = executorService;
        try {
            this.eventSubscription = txnStore.subscribeSubscriptionEvents(
                    segmentName, subscriptionName, path -> triggerReconcile());
        } catch (MetadataStoreException e) {
            recoveryFuture.completeExceptionally(e);
            pendingAckHandle.exceptionHandleFuture(e);
            return;
        }

        // Keep the positions as well as transaction ids: a fresh handle must recover the actual
        // pending acknowledgements before it becomes Ready or applies a terminal decision.
        // The store delivers per-entry errors via this scan's returned future as well — recovery
        // still fails loudly via the terminal whenComplete below — but we log here so the cause
        // is captured with the segment/subscription context.
        Map<String, ConcurrentLinkedQueue<TxnOp>> opsByTxn = new ConcurrentHashMap<>();
        Map<String, TxnState> states = new ConcurrentHashMap<>();
        Map<String, ConcurrentLinkedQueue<RecoveredAck>> recoveredAcks = new ConcurrentHashMap<>();
        txnStore.listAcksBySegmentSubscription(segmentName, subscriptionName, new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                TxnOp op = TxnMetadataStore.fromJson(r.getValue(), TxnOp.class);
                if (op.getKind() != TxnOpKind.ACK) {
                    return;
                }
                String txnIdKey = TxnPaths.txnIdFromOpPath(r.getStat().getPath());
                if (txnIdKey != null) {
                    opsByTxn.computeIfAbsent(txnIdKey, __ -> new ConcurrentLinkedQueue<>()).add(op);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn().attr("segment", segmentName).attr("sub", subscriptionName)
                        .exception(throwable).log("PendingAckStore recovery scan errored");
            }

            @Override
            public void onCompleted() {
            }
        })
        .thenCompose(__ -> {
            List<CompletableFuture<Void>> reads = new ArrayList<>(opsByTxn.size());
            for (String txnIdKey : opsByTxn.keySet()) {
                reads.add(txnStore.getHeader(txnIdKey).thenAccept(opt -> {
                    TxnState state = opt.map(v -> v.value().getState()).orElse(TxnState.ABORTED);
                    states.put(txnIdKey, state);
                }));
            }
            return FutureUtil.waitForAll(reads);
        })
        .thenCompose(__ -> {
            List<CompletableFuture<Void>> positions = new ArrayList<>();
            opsByTxn.forEach((txnIdKey, ops) -> {
                // An abort never advances a cursor, so lost legacy batch masks need not prevent
                // rolling it back. Its fresh handle has no pending ACK state to release yet.
                if (states.get(txnIdKey) == TxnState.ABORTED) {
                    return;
                }
                for (TxnOp op : ops) {
                    positions.add(recoverPosition(op).thenAccept(position -> {
                        if (position != null) {
                            recoveredAcks.computeIfAbsent(txnIdKey, key -> new ConcurrentLinkedQueue<>())
                                    .add(new RecoveredAck(TxnIds.fromKey(txnIdKey), op, position));
                        }
                    }));
                }
            });
            return FutureUtil.waitForAll(positions);
        })
        .thenCompose(__ -> {
            // The handle has only one cumulative-ACK transaction slot. Finish each committed
            // transaction before hydrating another, and only then restore OPEN transactions.
            // A later cumulative position may be applied first: recovering an earlier position
            // then skips it because the cursor already covers it, without moving backward.
            CompletableFuture<Void> commits = CompletableFuture.completedFuture(null);
            for (Map.Entry<String, TxnState> entry : states.entrySet()) {
                if (entry.getValue() == TxnState.COMMITTED) {
                    String txnIdKey = entry.getKey();
                    commits = commits.thenComposeAsync(ignored -> {
                        hydrateAcks(pendingAckHandle, recoveredAcks.get(txnIdKey));
                        return pendingAckHandle.commitRecoveredTxn(TxnIds.fromKey(txnIdKey));
                    }, executorService);
                }
            }
            // A recovered committed ACK must already be applied before attaching consumers.
            return commits;
        })
        .thenRunAsync(() -> {
            states.forEach((txnIdKey, state) -> {
                if (state == TxnState.OPEN) {
                    hydrateAcks(pendingAckHandle, recoveredAcks.get(txnIdKey));
                }
            });
            synchronized (lock) {
                openTxns.addAll(states.keySet());
            }
            if (!pendingAckHandle.changeToReadyState()) {
                throw new CompletionException(new BrokerServiceException.ServiceUnitNotReadyException(
                        "Failed to change PendingAckHandle state to Ready"));
            }
            pendingAckHandle.completeHandleFuture();
            pendingAckHandle.handleCacheRequest();
            recoveryFuture.complete(null);
            states.forEach((txnIdKey, state) -> {
                if (state.isTerminal()) {
                    applyTerminal(txnIdKey, state);
                }
            });
            // All hydration, readiness and terminal scheduling share the handle's pinned executor.
            // Events arriving during recovery were gated by recoveryFuture and are re-read now.
            triggerReconcile();
        }, executorService)
        .whenComplete((__, err) -> {
            if (err != null) {
                log.error().attr("segment", segmentName).attr("sub", subscriptionName)
                        .exception(err).log("PendingAckStore recovery failed");
                closeSubscriptionQuietly();
                recoveryFuture.completeExceptionally(err);
                pendingAckHandle.exceptionHandleFuture(err);
            }
        });
    }

    private record RecoveredAck(TxnID txnId, TxnOp op, Position position) {
    }

    private static void hydrateAcks(PendingAckHandleImpl pendingAckHandle, Iterable<RecoveredAck> acks) {
        if (acks == null) {
            return;
        }
        for (RecoveredAck ack : acks) {
            if (Boolean.TRUE.equals(ack.op().getCumulative())) {
                pendingAckHandle.handleCumulativeAckRecover(ack.txnId(), ack.position());
            } else {
                int batchSize = ack.op().getBatchSize() == null ? 1 : ack.op().getBatchSize();
                // A deleted position must only skip that op, not the remaining ACKs of the txn.
                pendingAckHandle.handleIndividualAckRecover(ack.txnId(),
                        Collections.singletonList(MutablePair.of(ack.position(), batchSize)));
            }
        }
    }

    private CompletableFuture<Position> recoverPosition(TxnOp op) {
        return FutureUtil.supplySafely(() -> {
            Position position = PositionFactory.create(op.getLedgerId(), op.getEntryId());
            if (op.getBatchSize() != null) {
                long[] ackSet = op.getAckSet();
                return CompletableFuture.completedFuture(ackSet == null ? position
                        : createPositionWithAckSet(op.getLedgerId(), op.getEntryId(), ackSet.clone()));
            }
            // Older records omitted both batch size and ACK mask. A whole-entry ACK is safe only if
            // the original entry contained exactly one message (or the cursor durably deleted it).
            // A legacy partial multi-message batch cannot be reconstructed: retain its ops and fail
            // recovery rather than acknowledge messages the application never processed.
            if (subscription.getCursor().isMessageDeleted(position)) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Position> result = new CompletableFuture<>();
            try {
                subscription.getCursor().getManagedLedger().asyncReadEntry(position, new ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(Entry entry, Object ctx) {
                        try {
                            MessageMetadata metadata = Commands.parseMessageMetadata(entry.getDataBuffer().duplicate());
                            if (metadata.getNumMessagesInBatch() != 1) {
                                result.completeExceptionally(new MetadataStoreException(
                                        "Legacy transactional ACK lacks a batch acknowledgement mask at "
                                                + segmentName + "/" + position + "; operation records retained"));
                            } else {
                                result.complete(position);
                            }
                        } catch (Throwable error) {
                            result.completeExceptionally(error);
                        } finally {
                            entry.release();
                        }
                    }

                    @Override
                    public void readEntryFailed(ManagedLedgerException error, Object ctx) {
                        result.completeExceptionally(error);
                    }
                }, null);
            } catch (Throwable error) {
                result.completeExceptionally(error);
            }
            return result;
        });
    }

    // ---- Ack append --------------------------------------------------------

    @Override
    public CompletableFuture<Void> appendIndividualAck(TxnID txnID,
                                                       List<MutablePair<Position, Integer>> positions) {
        return appendAcks(txnID, positions, false);
    }

    @Override
    public CompletableFuture<Void> appendCumulativeAck(TxnID txnID, Position position) {
        return appendAcks(txnID, Collections.singletonList(MutablePair.of(position, 1)), true);
    }

    private CompletableFuture<Void> appendAcks(TxnID txnID, List<MutablePair<Position, Integer>> positions,
                                               boolean cumulative) {
        CompletableFuture<Void> appended = new CompletableFuture<>();
        String txnIdKey = TxnIds.toKey(txnID);
        synchronized (lock) {
            if (closed) {
                return FutureUtil.failedFuture(new IllegalStateException("PendingAckStore is closed"));
            }
            appendsInProgress.add(appended);
            pendingActivations.merge(txnIdKey, 1, Integer::sum);
            appendGenerations.merge(txnIdKey, 1L, Long::sum);
        }
        // Append one TxnOp per position. PendingAckHandleImpl already validates the txn isn't
        // terminal before calling us, so we don't repeat the header check here.
        CompletableFuture<?>[] appends = new CompletableFuture<?>[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            Position p = positions.get(i).getLeft();
            TxnOp op = new TxnOp(TxnOpKind.ACK, segmentName, subscriptionName,
                    p.getLedgerId(), p.getEntryId(), cumulative ? Boolean.TRUE : null);
            op.setBatchSize(positions.get(i).getRight());
            long[] ackSet = getAckSetArrayOrNull(p);
            op.setAckSet(ackSet == null ? null : ackSet.clone());
            appends[i] = FutureUtil.supplySafely(() -> txnStore.appendOp(txnIdKey, op));
        }
        CompletableFuture.allOf(appends).whenComplete((__, err) -> {
            if (err == null) {
                completeSuccessfulAppend(txnIdKey, appended);
            } else {
                completeUncertainAppend(txnIdKey, new UncertainAppend(), appended, err);
            }
        });
        return appended;
    }

    private void executeAppendCompletion(String txnIdKey, ExecutorProvider.CleanupTask completion) {
        ExecutorService executor = handleExecutor;
        if (executor == null) {
            completion.run();
            return;
        }
        try {
            executor.execute((ExecutorProvider.CleanupTask) () -> {
                synchronized (lock) {
                    if (!closed && terminalApplications.contains(txnIdKey)) {
                        // A commit's asynchronous cursor callback clears its captured handle map.
                        // Do not install later same-txn reservations before that callback finishes.
                        deferredAppendCompletions.computeIfAbsent(txnIdKey, ignored -> new ArrayList<>())
                                .add(completion);
                        return;
                    }
                }
                completion.run();
            });
        } catch (RejectedExecutionException rejected) {
            // Complete native persistence/drain truth; activation still fails closed on the stopped pin.
            completion.run();
        }
    }

    private void completeSuccessfulAppend(String txnIdKey, CompletableFuture<Void> appended) {
        ExecutorService executor = handleExecutor;
        if (executor == null) {
            appended.complete(null);
            synchronized (lock) {
                appendsInProgress.remove(appended);
                finishActivation(txnIdKey);
            }
            return;
        }
        executeAppendCompletion(txnIdKey, () -> {
            // Also for synchronous metadata completion, the handle attaches its async
            // installation callback before this pinned task completes the append future.
            appended.complete(null);
            synchronized (lock) {
                appendsInProgress.remove(appended);
            }
            try {
                executor.execute(() -> {
                    synchronized (lock) {
                        finishActivation(txnIdKey);
                        if (closed) {
                            return;
                        }
                        openTxns.add(txnIdKey);
                        successfulTransactions.add(txnIdKey);
                        unknownSuccessfulAppends.add(txnIdKey);
                    }
                    reconcileSuccessfulAppend(txnIdKey);
                });
            } catch (RejectedExecutionException rejected) {
                // Retain the activation fence and durable records if installation cannot run.
                log.debug().attr("segment", segmentName).exception(rejected)
                        .log("Handle executor stopped before successful ACK activation");
            }
        });
    }

    /** Called under lock, after the matching handle installation callback on its pin. */
    private void finishActivation(String txnIdKey) {
        pendingActivations.computeIfPresent(txnIdKey, (key, count) -> count == 1 ? null : count - 1);
    }

    private CompletableFuture<Void> reconcileSuccessfulAppend(String txnIdKey) {
        final long generation;
        synchronized (lock) {
            if (closed || pendingActivations.containsKey(txnIdKey) || terminalApplications.contains(txnIdKey)
                    || uncertainAppends.containsKey(txnIdKey) || !openTxns.contains(txnIdKey)) {
                return CompletableFuture.completedFuture(null);
            }
            if (!successfulReads.add(txnIdKey)) {
                dirtySuccessfulReads.add(txnIdKey);
                return CompletableFuture.completedFuture(null);
            }
            generation = appendGenerations.getOrDefault(txnIdKey, 0L);
        }
        return FutureUtil.supplySafely(() -> txnStore.getHeader(txnIdKey)).thenCompose(header -> {
            synchronized (lock) {
                if (closed) {
                    return CompletableFuture.completedFuture(null);
                }
                if (header.isEmpty()) {
                    successfulRetries.add(txnIdKey);
                } else if (generation == appendGenerations.getOrDefault(txnIdKey, 0L)) {
                    unknownSuccessfulAppends.remove(txnIdKey);
                    successfulRetries.remove(txnIdKey);
                }
            }
            if (header.isEmpty()) {
                scheduleUncertaintyRetry();
                return CompletableFuture.completedFuture(null);
            }
            return header.get().value().getState().isTerminal()
                    ? applyTerminalSnapshot(txnIdKey, header.get().value().getState(), null)
                    : CompletableFuture.completedFuture(null);
        }).exceptionally(error -> {
            synchronized (lock) {
                if (!closed && openTxns.contains(txnIdKey)) {
                    unknownSuccessfulAppends.add(txnIdKey);
                    successfulRetries.add(txnIdKey);
                }
            }
            scheduleUncertaintyRetry();
            log.debug().attr("segment", segmentName).attr("txn", txnIdKey).exception(error)
                    .log("Successful ACK decision read will retry");
            return null;
        }).whenComplete((__, error) -> {
            boolean dirty;
            synchronized (lock) {
                successfulReads.remove(txnIdKey);
                dirty = dirtySuccessfulReads.remove(txnIdKey);
                retireAppendGeneration(txnIdKey, generation);
            }
            if (dirty) {
                reconcileSuccessfulAppend(txnIdKey);
            }
        });
    }

    /** No later callback may confuse a retired generation with newly admitted work. Called under lock. */
    private void retireAppendGeneration(String txnIdKey, long generation) {
        if (generation == appendGenerations.getOrDefault(txnIdKey, 0L)
                && !pendingActivations.containsKey(txnIdKey) && !openTxns.contains(txnIdKey)
                && !successfulRetries.contains(txnIdKey) && !unknownSuccessfulAppends.contains(txnIdKey)
                && !uncertainAppends.containsKey(txnIdKey) && !terminalApplications.contains(txnIdKey)
                && !successfulReads.contains(txnIdKey) && !dirtySuccessfulReads.contains(txnIdKey)
                && !deferredAppendCompletions.containsKey(txnIdKey)) {
            successfulTransactions.remove(txnIdKey);
            appendGenerations.remove(txnIdKey);
        }
    }

    private static final class UncertainAppend {
        private boolean active;
    }

    private void completeUncertainAppend(String txnIdKey, UncertainAppend uncertain,
                                         CompletableFuture<Void> appended, Throwable error) {
        synchronized (lock) {
            if (!closed) {
                uncertainAppends.computeIfAbsent(txnIdKey, ignored -> new ArrayList<>()).add(uncertain);
            }
        }
        ExecutorService executor = handleExecutor;
        if (executor == null) {
            finishFailedAppend(appended, error);
            return;
        }
        executeAppendCompletion(txnIdKey, () -> {
            // Completing here queues the failed reservation callback before its activation.
            finishFailedAppend(appended, error);
            try {
                executor.execute(() -> {
                    synchronized (lock) {
                        if (closed || !uncertainAppends.getOrDefault(txnIdKey, List.of()).contains(uncertain)) {
                            return;
                        }
                        uncertain.active = true;
                        finishActivation(txnIdKey);
                    }
                    scheduleUncertaintyRetry();
                });
            } catch (RejectedExecutionException rejected) {
                log.debug().attr("segment", segmentName).exception(rejected)
                        .log("Handle executor stopped before uncertain ACK activation");
            }
        });
    }

    private void finishFailedAppend(CompletableFuture<Void> appended, Throwable error) {
        appended.completeExceptionally(error);
        synchronized (lock) {
            appendsInProgress.remove(appended);
        }
    }

    @Override
    public CompletableFuture<Void> appendCommitMark(TxnID txnID, AckType ackType) {
        if (txnID == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Transaction ID cannot be null"));
        }
        synchronized (lock) {
            // A failed append cannot be promoted to a successful ACK by a direct handle commit.
            // A fresh handle can recover an observed COMMITTED decision from retained durable ops.
            if (uncertainAppends.containsKey(TxnIds.toKey(txnID))) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Transaction ACK persistence is still uncertain"));
            }
        }
        // No-op for the metadata-driven store: v5 commits are driven by the TC writing the
        // /txn header CAS and publishing the subscription event, not by direct SPI calls.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> appendAbortMark(TxnID txnID, AckType ackType) {
        // No-op (see appendCommitMark).
        return CompletableFuture.completedFuture(null);
    }

    // ---- Reconcile (event-driven) -----------------------------------------

    private void scheduleUncertaintyRetry() {
        synchronized (lock) {
            if (closed || uncertaintyRunning || uncertaintyRetry != null || uncertainAppends.values().stream()
                    .flatMap(List::stream).noneMatch(uncertain -> uncertain.active) && successfulRetries.isEmpty()) {
                return;
            }
        }
        try {
            ScheduledExecutorService scheduler = subscription.getTopic().getBrokerService().getPulsar().getExecutor();
            synchronized (lock) {
                if (closed || uncertaintyRunning || uncertaintyRetry != null) {
                    return;
                }
                uncertaintyRetry = scheduler.schedule(this::reconcileUncertainAppends,
                        UNCERTAIN_ACK_RETRY_MILLIS, TimeUnit.MILLISECONDS);
            }
        } catch (RuntimeException unavailable) {
            // A shutting-down executor cannot repair uncertainty. Retain it until store close;
            // never report a successful ACK or discard its mask because scheduling failed.
            log.debug().attr("segment", segmentName).exception(unavailable)
                    .log("Could not schedule uncertain ACK reconciliation");
        }
    }

    private void reconcileUncertainAppends() {
        Map<String, List<UncertainAppend>> snapshot = new HashMap<>();
        Set<String> successfulSnapshot;
        synchronized (lock) {
            uncertaintyRetry = null;
            if (closed || uncertaintyRunning) {
                return;
            }
            uncertaintyRunning = true;
            successfulSnapshot = Set.copyOf(successfulRetries);
            successfulRetries.removeAll(successfulSnapshot);
            uncertainAppends.forEach((txn, appends) -> {
                List<UncertainAppend> active = appends.stream().filter(append -> append.active).toList();
                if (!active.isEmpty()) {
                    snapshot.put(txn, active);
                }
            });
        }
        // Only failed appends reach this path. One header/cleanup chain is in flight at a time,
        // and an outage never accumulates overlapping polls or changes the transaction outcome.
        CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
        for (Map.Entry<String, List<UncertainAppend>> entry : snapshot.entrySet()) {
            tail = tail.thenCompose(__ -> FutureUtil.supplySafely(() ->
                    reconcileUncertainTransaction(entry.getKey(), entry.getValue())).exceptionally(error -> {
                        log.debug().attr("segment", segmentName).attr("txn", entry.getKey()).exception(error)
                                .log("Uncertain ACK reconciliation will retry");
                        return null;
                    }));
        }
        for (String txnIdKey : successfulSnapshot) {
            tail = tail.thenCompose(__ -> reconcileSuccessfulAppend(txnIdKey));
        }
        tail.whenComplete((__, error) -> {
            synchronized (lock) {
                uncertaintyRunning = false;
            }
            scheduleUncertaintyRetry();
        });
    }

    private CompletableFuture<Void> reconcileUncertainTransaction(String txnIdKey, List<UncertainAppend> snapshot) {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        return txnStore.getHeader(txnIdKey).thenCompose(header -> {
            if (closed || header.isEmpty() || !header.get().value().getState().isTerminal()) {
                // Absence is UNKNOWN here: an ACK append error is not evidence of an abort.
                return CompletableFuture.completedFuture(null);
            }
            TxnState state = header.get().value().getState();
            if (state == TxnState.COMMITTED) {
                synchronized (lock) {
                    // The SDK aborts a transaction after an ACK error. An externally committed
                    // uncertain append cannot safely be materialized in this live handle: later
                    // reservations can race the cursor's asynchronous completion. Retain its masks
                    // and durable ops for reconstruction; a terminal decision cannot change again.
                    snapshot.forEach(uncertain -> uncertain.active = false);
                }
                log.debug().attr("segment", segmentName).attr("txn", txnIdKey)
                        .log("Committed transaction has uncertain ACK persistence; retaining recovery state");
                return CompletableFuture.completedFuture(null);
            }
            return applyTerminalSnapshot(txnIdKey, state, snapshot);
        });
    }

    private void triggerReconcile() {
        if (closed || !recoveryFuture.isDone() || recoveryFuture.isCompletedExceptionally()) {
            return;
        }
        Set<String> snapshot;
        Set<String> successful;
        synchronized (lock) {
            successful = Set.copyOf(successfulTransactions);
            snapshot = new HashSet<>(openTxns);
            snapshot.removeAll(uncertainAppends.keySet());
            snapshot.removeAll(pendingActivations.keySet());
            snapshot.removeAll(successfulTransactions);
        }
        // Locally appended transactions always use the safe reader, including event-read
        // errors after an earlier confirmed OPEN result. Held reads are dirtied and coalesced.
        successful.forEach(this::reconcileSuccessfulAppend);
        if (snapshot.isEmpty()) {
            return;
        }
        List<CompletableFuture<Void>> reads = new ArrayList<>(snapshot.size());
        for (String txnIdKey : snapshot) {
            reads.add(txnStore.getHeader(txnIdKey).thenAccept(opt -> {
                TxnState state = opt.map(v -> v.value().getState()).orElse(TxnState.ABORTED);
                if (state.isTerminal()) {
                    applyTerminal(txnIdKey, state);
                }
            }));
        }
        FutureUtil.waitForAll(reads).whenComplete((__, err) -> {
            if (err != null) {
                log.warn().attr("segment", segmentName).attr("sub", subscriptionName)
                        .exception(err).log("Reconcile encountered error");
            }
        });
    }

    /**
     * Apply a terminal state for {@code txnIdKey} — call the handle to advance / clear its state,
     * delete the matching op records, drop from {@link #openTxns}. Idempotent: if the handle
     * doesn't know the txn (already applied / was never on this sub), the calls are inexpensive.
     */
    private void applyTerminal(String txnIdKey, TxnState state) {
        applyTerminalSnapshot(txnIdKey, state, null).exceptionally(error -> {
            log.debug().attr("segment", segmentName).attr("txn", txnIdKey).exception(error)
                    .log("Terminal ACK application will retry on reconciliation");
            return null;
        });
    }

    /** Capture exact versions only after all admitted reservations are installed on the pin. */
    private CompletableFuture<Void> applyTerminalSnapshot(String txnIdKey, TxnState state,
                                                         List<UncertainAppend> uncertainSnapshot) {
        return CompletableFuture.completedFuture(null).thenComposeAsync(ignored -> {
            final long generation;
            synchronized (lock) {
                if (closed || pendingActivations.containsKey(txnIdKey)
                        || uncertainSnapshot == null && (uncertainAppends.containsKey(txnIdKey)
                        || !openTxns.contains(txnIdKey)) || !terminalApplications.add(txnIdKey)) {
                    return CompletableFuture.completedFuture(null);
                }
                generation = appendGenerations.getOrDefault(txnIdKey, 0L);
            }
            CompletableFuture<Void> applying = txnStore
                    .collectAckOpsForSegmentSubscription(segmentName, subscriptionName)
                    .thenComposeAsync(records -> {
                        List<OpRecord> exact = records.stream()
                                .filter(record -> txnIdKey.equals(record.txnId())).toList();
                        synchronized (lock) {
                            if (closed || pendingActivations.containsKey(txnIdKey)
                                    || generation != appendGenerations.getOrDefault(txnIdKey, 0L)) {
                                return CompletableFuture.completedFuture(null);
                            }
                            openTxns.remove(txnIdKey);
                        }
                        PendingAckHandleImpl current = handle;
                        CompletableFuture<Void> applied = current == null ? CompletableFuture.completedFuture(null)
                                : state == TxnState.COMMITTED ? current.commitTxn(
                                        TxnIds.fromKey(txnIdKey), Map.of(), 0L)
                                : current.abortTxn(TxnIds.fromKey(txnIdKey), null, 0L);
                        return applied.thenComposeAsync(__ -> {
                            if (closed) {
                                return CompletableFuture.completedFuture(null);
                            }
                            return state == TxnState.COMMITTED ? cleanupCommittedAcks(txnIdKey)
                                    : txnStore.deleteOps(exact).thenCompose(v ->
                                            txnStore.collectAckOpsForSegmentSubscription(segmentName, subscriptionName))
                                            .thenAccept(remaining -> {
                                                synchronized (lock) {
                                                    if (!closed && remaining.stream()
                                                            .anyMatch(record -> txnIdKey.equals(record.txnId()))) {
                                                        openTxns.add(txnIdKey);
                                                    }
                                                }
                                            });
                        }, handleExecutor).thenRun(() -> {
                            if (uncertainSnapshot != null) {
                                synchronized (lock) {
                                    List<UncertainAppend> currentAppends = uncertainAppends.get(txnIdKey);
                                    if (currentAppends != null) {
                                        currentAppends.removeAll(uncertainSnapshot);
                                        if (currentAppends.isEmpty()) {
                                            uncertainAppends.remove(txnIdKey);
                                        }
                                    }
                                }
                            }
                        });
                    }, handleExecutor);
            return applying.whenComplete((__, error) -> {
                boolean newer;
                List<ExecutorProvider.CleanupTask> deferred;
                synchronized (lock) {
                    terminalApplications.remove(txnIdKey);
                    deferred = deferredAppendCompletions.remove(txnIdKey);
                    if (error != null && !closed) {
                        openTxns.add(txnIdKey);
                    }
                    if (error == null && generation == appendGenerations.getOrDefault(txnIdKey, 0L)
                            && !pendingActivations.containsKey(txnIdKey)) {
                        unknownSuccessfulAppends.remove(txnIdKey);
                        successfulRetries.remove(txnIdKey);
                    }
                    retireAppendGeneration(txnIdKey, generation);
                    newer = !closed && appendGenerations.containsKey(txnIdKey)
                            && generation != appendGenerations.get(txnIdKey)
                            && !pendingActivations.containsKey(txnIdKey);
                }
                if (deferred != null) {
                    deferred.forEach(completion -> executeAppendCompletion(txnIdKey, completion));
                }
                if (newer) {
                    reconcileSuccessfulAppend(txnIdKey);
                }
            });
        }, handleExecutor);
    }

    private CompletableFuture<Void> cleanupCommittedAcks(String txnIdKey) {
        return FutureUtil.supplySafely(() -> {
            Position persisted = subscription.getCursor().getPersistentMarkDeletedPosition();
            ConcurrentLinkedQueue<OpRecord> covered = new ConcurrentLinkedQueue<>();
            AtomicBoolean retained = new AtomicBoolean();
            return txnStore.listOpsByTxn(txnIdKey, new ScanConsumer() {
                @Override
                public void onNext(GetResult record) {
                    TxnOp op = TxnMetadataStore.fromJson(record.getValue(), TxnOp.class);
                    String path = record.getStat().getPath();
                    if (!txnIdKey.equals(TxnPaths.txnIdFromOpPath(path)) || op.getKind() != TxnOpKind.ACK
                            || !segmentName.equals(op.getSegment()) || !subscriptionName.equals(op.getSubscription())) {
                        return;
                    }
                    Position position = PositionFactory.create(op.getLedgerId(), op.getEntryId());
                    if (persisted != null && position.compareTo(persisted) <= 0) {
                        covered.add(new OpRecord(path, txnIdKey, record.getStat().getVersion()));
                    } else {
                        retained.set(true);
                    }
                }

                @Override
                public void onError(Throwable error) {
                    // The returned scan future propagates the error and leaves recovery ops intact.
                }

                @Override
                public void onCompleted() {
                }
            }).thenCompose(__ -> txnStore.deleteOps(List.copyOf(covered))).thenCompose(__ -> {
                if (retained.get() || covered.isEmpty()) {
                    return CompletableFuture.completedFuture(retained.get());
                }
                // Versioned deletes can skip an op replaced at the same path. Keep tracking the
                // transaction if such a record still exists, so a later event can retry cleanup.
                AtomicBoolean remaining = new AtomicBoolean();
                return txnStore.listOpsByTxn(txnIdKey, new ScanConsumer() {
                    @Override
                    public void onNext(GetResult record) {
                        TxnOp op = TxnMetadataStore.fromJson(record.getValue(), TxnOp.class);
                        if (txnIdKey.equals(TxnPaths.txnIdFromOpPath(record.getStat().getPath()))
                                && op.getKind() == TxnOpKind.ACK && segmentName.equals(op.getSegment())
                                && subscriptionName.equals(op.getSubscription())) {
                            remaining.set(true);
                        }
                    }

                    @Override
                    public void onError(Throwable error) {
                        // The returned future preserves retry tracking on an error.
                    }

                    @Override
                    public void onCompleted() {
                    }
                }).thenApply(ignored -> remaining.get());
            }).thenAccept(needsRecovery -> {
                if (needsRecovery) {
                    synchronized (lock) {
                        openTxns.add(txnIdKey);
                    }
                }
            });
        });
    }

    // ---- Close -------------------------------------------------------------

    /**
     * Completes once the ack appends in progress are done; no append starts after it is called. A failed append
     * fails the ack it was for, not the close.
     */
    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<?>[] inProgress;
        Map<String, List<ExecutorProvider.CleanupTask>> deferred;
        synchronized (lock) {
            closed = true;
            if (uncertaintyRetry != null) {
                uncertaintyRetry.cancel(false);
                uncertaintyRetry = null;
            }
            uncertainAppends.clear();
            successfulRetries.clear();
            unknownSuccessfulAppends.clear();
            dirtySuccessfulReads.clear();
            successfulTransactions.clear();
            deferred = new HashMap<>(deferredAppendCompletions);
            deferredAppendCompletions.clear();
            inProgress = appendsInProgress.toArray(new CompletableFuture<?>[0]);
        }
        deferred.forEach((txn, completions) ->
                completions.forEach(completion -> executeAppendCompletion(txn, completion)));
        closeSubscriptionQuietly();
        return CompletableFuture.allOf(inProgress).exceptionally(__ -> null);
    }

    private void closeSubscriptionQuietly() {
        AutoCloseable h = eventSubscription;
        if (h == null) {
            return;
        }
        eventSubscription = null;
        try {
            h.close();
        } catch (Throwable t) {
            log.warn().attr("segment", segmentName).attr("sub", subscriptionName).exception(t)
                    .log("Failed to close event subscription");
        }
    }

    // ---- Test seams --------------------------------------------------------

    /** @return future that completes when initial recovery has finished. Intended for tests. */
    public CompletableFuture<Void> recoveryFuture() {
        return recoveryFuture;
    }
}
