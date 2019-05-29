/*
 * Copyright 2016-2019 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:JvmMultifileClass
@file:JvmName("FlowKt")

package kotlinx.coroutines.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.Channel.Factory.DEFAULT
import kotlinx.coroutines.channels.Channel.Factory.OPTIONAL_CHANNEL
import kotlinx.coroutines.internal.threadContextElements
import kotlin.coroutines.*
import kotlin.coroutines.intrinsics.*
import kotlin.jvm.*

/**
 * Buffers flow emissions via channel of a specified capacity and runs collector in a separate coroutine.
 *
 * todo
 *
 * Adjacent applications of [channelFlow], [flowOn], [buffer], [produceIn], and [broadcastIn] are
 * always fused so that only one properly configured channel is used for execution.
 *
 */
@FlowPreview
public fun <T> Flow<T>.buffer(capacity: Int = DEFAULT): Flow<T> {
    require(capacity >= 0 || capacity == DEFAULT || capacity == CONFLATED) {
        "Buffer size should be non-negative, DEFAULT, or CONFLATED, but was $capacity"
    }
    return if (this is ChannelFlow)
        update(capacity = capacity)
    else
        ChannelFlowOperator(this, capacity = capacity)
}

// todo: conflate would be a useful operator only when Channel.CONFLATE is changed to always deliver the last send value
//@FlowPreview
//public fun <T> Flow<T>.conflate(): Flow<T> = buffer(CONFLATED)

/**
 * The operator that changes the context where this flow is executed to the given [context].
 * This operator is composable and affects only preceding operators that do not have its own context.
 * This operator is context preserving: [context] **does not** leak into the downstream flow.
 *
 * For example:
 *
 * ```
 * withContext(Dispatchers.Main) {
 *     val singleValue = intFlow // will be executed on IO if context wasn't specified before
 *         .map { ... } // Will be executed in IO
 *         .flowOn(Dispatchers.IO)
 *         .filter { ... } // Will be executed in Default
 *         .flowOn(Dispatchers.Default)
 *         .single() // Will be executed in the Main
 * }
 * ```
 *
 * For more explanation of context preservation please refer to [Flow] documentation.
 *
 * This operators retains a sequential nature of flow if changing the context does not call for changing
 * the [dispatcher][CoroutineDispatcher]. Otherwise, if changing dispatcher is required, it collects
 * flow emissions in one coroutine that is run using a specified [context] and emits them from another coroutines
 * with the original collector's context using a channel with a [default][Channel.DEFAULT] buffer size
 * between two coroutines similarly to [buffer] operator, unless [buffer] operator is explicitly called
 * before or after `flowOn`, which requests buffering behavior and specifies channel size.
 *
 * Adjacent applications of [channelFlow], [flowOn], [buffer], [produceIn], and [broadcastIn] are
 * always fused so that only one properly configured channel is used for execution.
 *
 * @throws [IllegalArgumentException] if provided context contains [Job] instance.
 */
@FlowPreview
public fun <T> Flow<T>.flowOn(context: CoroutineContext): Flow<T> {
    checkFlowContext(context)
    return when {
        context == EmptyCoroutineContext -> this
        this is ChannelFlow -> update(context = context)
        else -> ChannelFlowOperator(this, context = context)
    }
}

/**
 * The operator that changes the context where all transformations applied to the given flow within a [builder] are executed.
 * This operator is context preserving and does not affect the context of the preceding and subsequent operations.
 *
 * Example:
 * ```
 * flow // not affected
 *     .map { ... } // Not affected
 *     .flowWith(Dispatchers.IO) {
 *         map { ... } // in IO
 *         .filter { ... } // in IO
 *     }
 *     .map { ... } // Not affected
 * ```
 * For more explanation of context preservation please refer to [Flow] documentation.
 *
 * todo:
 *
 * @throws [IllegalArgumentException] if provided context contains [Job] instance.
 */
@FlowPreview
public fun <T, R> Flow<T>.flowWith(
    context: CoroutineContext,
    builder: Flow<T>.() -> Flow<R>
): Flow<R> {
    checkFlowContext(context)
    val source = this
    return flow {
        /**
         * Here we should remove a Job instance from the context.
         * All builders are written using scoping and no global coroutines are launched, so it is safe not to provide explicit Job.
         * It is also necessary not to mess with cancellation if multiple flowWith are used.
         */
        val originalContext = coroutineContext.minusKey(Job)
        val prepared = source.flowOn(originalContext)
        builder(prepared).flowOn(context).collect { value ->
            return@collect emit(value)
        }
    }
}

// ChannelFlow implementation that operates on another flow before it
internal open class ChannelFlowOperator<S, T>(
    protected val flow: Flow<S>,
    context: CoroutineContext = EmptyCoroutineContext,
    capacity: Int = OPTIONAL_CHANNEL
) : ChannelFlow<T>(context, capacity) {
    override fun create(context: CoroutineContext, capacity: Int): ChannelFlow<T> =
        ChannelFlowOperator(flow, context, capacity)

    // It is overridden by concurrent merge operator
    @Suppress("UNCHECKED_CAST")
    protected open suspend fun flowCollect(collector: FlowCollector<T>) =
        (flow as Flow<T>).collect(collector)

    // Changes collecting context upstream to the specified newContext, while collecting in the original context
    private suspend fun collectWithContextUndispatched(collector: FlowCollector<T>, newContext: CoroutineContext) {
        val originalContextCollector = collector.withUndispatchedContextCollector(coroutineContext)
        // invoke flowCollect(originalContextCollector) in the newContext
        val flowCollectRef: suspend (FlowCollector<T>) -> Unit = { flowCollect(it) }
        return withContextUndispatched(newContext, block = flowCollectRef, value = originalContextCollector)
    }

    // Slow path when output channel is required
    protected override suspend fun collectTo(scope: ProducerScope<T>) =
        flowCollect(SendingCollector(scope))

    // Optimizations for fast-path when channel creation is optional
    override suspend fun collect(collector: FlowCollector<T>) {
        // Fast-path: When channel creation is optional (flowOn/flowWith operators without buffer/conflate)
        if (capacity == OPTIONAL_CHANNEL) {
            val collectContext = coroutineContext
            val newContext = collectContext + context // simulate resulting collect context
            // #1: If the resulting context happens to be the same as it was -- fallback to plain collect
            if (newContext == collectContext)
                return flowCollect(collector)
            // #2: If we don't need to change the dispatcher we can go without channels
            if (newContext[ContinuationInterceptor] == collectContext[ContinuationInterceptor])
                return collectWithContextUndispatched(collector, newContext)
        }
        // Slow-path: create the actual channel
        super.collect(collector)
    }

    // debug toString
    override fun toString(): String = "$flow -> ${super.toString()}"
}

internal class SendingCollector<T>(
    private val channel: SendChannel<T>
) : ConcurrentFlowCollector<T> {
    override suspend fun emit(value: T) = channel.send(value)
}

// Now if the underlying collector was accepting concurrent emits, then this one is too
// todo: we might need to generalize this pattern for "thread-safe" operators that can fuse with channels
private fun <T> FlowCollector<T>.withUndispatchedContextCollector(emitContext: CoroutineContext): FlowCollector<T> = when (this) {
    // SendingCollector does not care about the context at all so can be used as it
    is SendingCollector -> this
    // Original collector is concurrent, so wrap into UndispatchedContextCollector and tag the result with ConcurrentFlowCollector interface
    is ConcurrentFlowCollector ->
        object : UndispatchedContextCollector<T>(this, emitContext), ConcurrentFlowCollector<T> {}
    // Otherwise just wrap into UndispatchedContextCollector interface implementation
    else -> UndispatchedContextCollector(this, emitContext)
}

private open class UndispatchedContextCollector<T>(
    downstream: FlowCollector<T>,
    private val emitContext: CoroutineContext
) : FlowCollector<T> {
    private val countOrElement = threadContextElements(emitContext) // precompute for fast withContextUndispatched
    private val emitRef: suspend (T) -> Unit = { downstream.emit(it) } // allocate suspend function ref once on creation

    override suspend fun emit(value: T): Unit =
        withContextUndispatched(emitContext, countOrElement, emitRef, value)
}

// Efficiently computes block(value) in the newContext
private suspend fun <T, V> withContextUndispatched(
    newContext: CoroutineContext,
    countOrElement: Any = threadContextElements(newContext), // can be precomputed for speed
    block: suspend (V) -> T, value: V
): T =
    suspendCoroutineUninterceptedOrReturn sc@{ uCont ->
        withCoroutineContext(newContext, countOrElement) {
            block.startCoroutineUninterceptedOrReturn(value, Continuation(newContext) {
                uCont.resumeWith(it)
            })
        }
    }

private fun checkFlowContext(context: CoroutineContext) {
    require(context[Job] == null) {
        "Flow context cannot contain job in it. Had $context"
    }
}
