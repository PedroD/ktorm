package org.ktorm.support.postgresql

import org.ktorm.database.CachedRowSet
import java.util.*

public class CompositeCachedRowSet {
    private val resultSets = LinkedList<CachedRowSet>()

    public fun add(rs: CachedRowSet) {
        resultSets.add(rs)
    }

    public operator fun iterator(): Iterator<CachedRowSet> = object : Iterator<CachedRowSet> {
        private var cursor = 0
        private var hasNext: Boolean? = null

        override fun hasNext(): Boolean {
            val hasNext = (cursor < resultSets.size && resultSets[cursor].next()).also { hasNext = it }

            if (!hasNext) {
                return ++cursor < resultSets.size && hasNext()
            }

            return hasNext
        }

        override fun next(): CachedRowSet {
            return if (hasNext ?: hasNext()) resultSets[cursor].also { hasNext = null } else throw NoSuchElementException()
        }
    }

    public fun asIterable(): Iterable<CachedRowSet> {
        return Iterable { iterator() }
    }
}