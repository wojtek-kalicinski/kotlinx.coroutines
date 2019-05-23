/*
 * Copyright 2016-2019 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.flow.internal

import kotlinx.coroutines.internal.*
import kotlin.jvm.*

// Note: it is conceptually the same as AbstractChannel.NULL_VALUE
// todo: consolidate, move this constant to "common" kotlinx.coroutines.internal, rename consistently with other symbols
@JvmField
@SharedImmutable
internal val NullSurrogate = Symbol("NullSurrogate")
