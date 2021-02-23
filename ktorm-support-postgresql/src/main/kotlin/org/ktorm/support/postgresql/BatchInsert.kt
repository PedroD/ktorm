/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ktorm.support.postgresql

import org.ktorm.database.Database
import org.ktorm.dsl.AssignmentsBuilder
import org.ktorm.dsl.KtormDsl
import org.ktorm.dsl.batchInsert
import org.ktorm.expression.ColumnAssignmentExpression
import org.ktorm.expression.ColumnExpression
import org.ktorm.expression.SqlExpression
import org.ktorm.expression.TableExpression
import org.ktorm.schema.BaseTable
import org.ktorm.schema.Column

/**
 * Batch insert expression, represents a batch insert statement in PostgreSQL.
 *
 * For example:
 *
 * ```sql
 * insert into table (column1, column2) values (?, ?), (?, ?), (?, ?)...
 * on conflict (...) do update set ...`
 * ```
 *
 * @property table the table to be inserted.
 * @property assignments column assignments of the batch insert statement.
 * @property conflictColumns the index columns on which the conflict may happens.
 * @property updateAssignments the updated column assignments while key conflict exists.
 */
public data class BatchInsertExpression(
    val table: TableExpression,
    val assignments: List<ColumnAssignmentExpression<*>>,
    val conflictColumns: List<ColumnExpression<*>> = emptyList(),
    val updateAssignments: List<ColumnAssignmentExpression<*>> = emptyList(),
    val returningColumns: List<ColumnExpression<*>> = emptyList(),
    override val isLeafNode: Boolean = false,
    override val extraProperties: Map<String, Any> = emptyMap()
) : SqlExpression()

/**
 * Construct a batch insert expression in the given closure, then execute it and return the effected row count.
 *
 * The usage is almost the same as [batchInsert], but this function is implemented by generating a special SQL
 * using PostgreSQL's batch insert syntax, instead of based on JDBC batch operations. For this reason, its performance
 * is much better than [batchInsert].
 *
 * The generated SQL is like: `insert into table (column1, column2) values (?, ?), (?, ?), (?, ?)...`.
 *
 * Usage:
 *
 * ```kotlin
 * database.batchInsert(Employees) {
 *     item {
 *         set(it.name, "jerry")
 *         set(it.job, "trainee")
 *         set(it.managerId, 1)
 *         set(it.hireDate, LocalDate.now())
 *         set(it.salary, 50)
 *         set(it.departmentId, 1)
 *     }
 *     item {
 *         set(it.name, "linda")
 *         set(it.job, "assistant")
 *         set(it.managerId, 3)
 *         set(it.hireDate, LocalDate.now())
 *         set(it.salary, 100)
 *         set(it.departmentId, 2)
 *     }
 * }
 * ```
 *
 * @since 3.3.0
 * @param table the table to be inserted.
 * @param block the DSL block, extension function of [BatchInsertStatementBuilder], used to construct the expression.
 * @return the effected row count.
 * @see batchInsert
 */
public fun <T : BaseTable<*>> Database.batchInsert(
    table: T, block: BatchInsertStatementBuilder<T>.(T) -> Unit
): IntArray {
    val builder = BatchInsertStatementBuilder(table).apply { block(table) }

    if (builder.assignments.isEmpty()) return IntArray(0)

    val expressions = builder.assignments.map { assignment ->
        BatchInsertExpression(table.asExpression(), assignment)
    }

    return executeBatch(expressions)
}

/**
 * Batch insert records to the table, determining if there is a key conflict while inserting each of them,
 * and automatically performs updates if any conflict exists.
 *
 * Usage:
 *
 * ```kotlin
 * database.batchInsertOrUpdate(Employees) {
 *     item {
 *         set(it.id, 1)
 *         set(it.name, "vince")
 *         set(it.job, "engineer")
 *         set(it.salary, 1000)
 *         set(it.hireDate, LocalDate.now())
 *         set(it.departmentId, 1)
 *     }
 *     item {
 *         set(it.id, 5)
 *         set(it.name, "vince")
 *         set(it.job, "engineer")
 *         set(it.salary, 1000)
 *         set(it.hireDate, LocalDate.now())
 *         set(it.departmentId, 1)
 *     }
 *     onConflict {
 *         set(it.salary, it.salary + 900)
 *     }
 * }
 * ```
 *
 * Generated SQL:
 *
 * ```sql
 * insert into t_employee (id, name, job, salary, hire_date, department_id)
 * values (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)
 * on conflict (id) do update set salary = t_employee.salary + ?
 * ```
 *
 * @since 3.3.0
 * @param table the table to be inserted.
 * @param block the DSL block used to construct the expression.
 * @return the effected row count.
 * @see batchInsert
 */
public fun <T : BaseTable<*>> Database.batchInsertOrUpdate(
    table: T, block: BatchInsertOrUpdateStatementBuilder<T>.(T) -> Unit
): IntArray {
    val builder = BatchInsertOrUpdateStatementBuilder(table).apply { block(table) }

    if (builder.assignments.isEmpty()) return IntArray(0)

    val conflictColumns = builder.conflictColumns.ifEmpty { table.primaryKeys }
    if (conflictColumns.isEmpty()) {
        val msg =
            "Table '$table' doesn't have a primary key, " +
                    "you must specify the conflict columns when calling onConflict(col) { .. }"
        throw IllegalStateException(msg)
    }

    val expressions = builder.assignments.map { assignment ->
        BatchInsertExpression(
            table = table.asExpression(),
            assignments = assignment,
            conflictColumns = conflictColumns.map { it.asExpression() },
            updateAssignments = builder.updateAssignments
        )
    }

    return executeBatch(expressions)
}

/**
 * DSL builder for batch insert statements.
 */
@KtormDsl
public open class BatchInsertStatementBuilder<T : BaseTable<*>>(internal val table: T) {
    internal val assignments = ArrayList<List<ColumnAssignmentExpression<*>>>()

    /**
     * Add the assignments of a new row to the batch insert.
     */
    public fun item(block: AssignmentsBuilder.() -> Unit) {
        val builder = PostgreSqlAssignmentsBuilder().apply(block)

        if (assignments.isEmpty()
            || assignments[0].map { it.column.name } == builder.assignments.map { it.column.name }
        ) {
            assignments += builder.assignments
        } else {
            throw IllegalArgumentException("Every item in a batch operation must be the same.")
        }
    }
}

/**
 * DSL builder for batch insert or update statements.
 */
@KtormDsl
public class BatchInsertOrUpdateStatementBuilder<T : BaseTable<*>>(table: T) : BatchInsertStatementBuilder<T>(table) {
    internal val updateAssignments = ArrayList<ColumnAssignmentExpression<*>>()
    internal val conflictColumns = ArrayList<Column<*>>()

    /**
     * Specify the update assignments while any key conflict exists.
     */
    public fun onConflict(vararg columns: Column<*>, block: BatchInsertOrUpdateOnConflictClauseBuilder.() -> Unit) {
        val builder = BatchInsertOrUpdateOnConflictClauseBuilder().apply(block)
        updateAssignments += builder.assignments
        conflictColumns += columns
    }
}

/**
 * DSL builder for batch insert or update on conflict clause.
 */
@KtormDsl
public class BatchInsertOrUpdateOnConflictClauseBuilder : PostgreSqlAssignmentsBuilder() {

    /**
     * Reference the 'EXCLUDED' table in a ON CONFLICT clause.
     */
    public fun <T : Any> excluded(column: Column<T>): ColumnExpression<T> {
        // excluded.name
        return ColumnExpression(
            table = TableExpression(name = "excluded"),
            name = column.name,
            sqlType = column.sqlType
        )
    }
}
