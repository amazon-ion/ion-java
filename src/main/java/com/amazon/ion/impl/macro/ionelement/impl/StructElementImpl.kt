/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.ion.impl.macro.ionelement.impl

import com.amazon.ion.IonType
import com.amazon.ion.IonWriter
import com.amazon.ion.impl.macro.ionelement.api.*
import com.amazon.ion.impl.macro.ionelement.api.PersistentMetaContainer
import com.amazon.ion.impl.macro.ionelement.api.constraintError
import kotlinx.collections.immutable.PersistentCollection
import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.toPersistentList
import kotlinx.collections.immutable.toPersistentMap

internal class StructElementImpl(
    private val allFields: PersistentList<StructField>,
    override val annotations: PersistentList<String>,
    override val metas: PersistentMetaContainer
) : AnyElementBase(), StructElement {

    override val type: ElementType get() = ElementType.STRUCT
    override val size = allFields.size

    // Note that we are not using `by lazy` here because it requires 2 additional allocations and
    // has been demonstrated to significantly increase memory consumption!
    private var valuesBackingField: PersistentCollection<AnyElement>? = null
    override val values: Collection<AnyElement>
        get() {
            if (valuesBackingField == null) {
                valuesBackingField = fields.map { it.value }.toPersistentList()
            }
            return valuesBackingField!!
        }
    override val containerValues: Collection<AnyElement> get() = values
    override val structFields: Collection<StructField> get() = allFields
    override val fields: Collection<StructField> get() = allFields

    // Note that we are not using `by lazy` here because it requires 2 additional allocations and
    // has been demonstrated to significantly increase memory consumption!
    private var fieldsByNameBackingField: PersistentMap<String, PersistentList<AnyElement>>? = null

    /** Lazily calculated map of field names and lists of their values. */
    private val fieldsByName: Map<String, List<AnyElement>>
        get() {
            if (fieldsByNameBackingField == null) {
                fieldsByNameBackingField =
                    fields
                        .groupBy { it.name }
                        .map { structFieldGroup -> structFieldGroup.key to structFieldGroup.value.map { it.value }.toPersistentList() }
                        .toMap().toPersistentMap()
            }
            return fieldsByNameBackingField!!
        }

    override fun get(fieldName: String): AnyElement =
        fieldsByName[fieldName]?.firstOrNull() ?: constraintError(this, "Required struct field '$fieldName' missing")

    override fun getOptional(fieldName: String): AnyElement? =
        fieldsByName[fieldName]?.firstOrNull()

    override fun getAll(fieldName: String): Iterable<AnyElement> = fieldsByName[fieldName] ?: emptyList()

    override fun containsField(fieldName: String): Boolean = fieldsByName.containsKey(fieldName)

    override fun copy(annotations: List<String>, metas: MetaContainer): StructElementImpl =
        StructElementImpl(allFields, annotations.toPersistentList(), metas.toPersistentMap())

    override fun withAnnotations(vararg additionalAnnotations: String): StructElementImpl = _withAnnotations(*additionalAnnotations)
    override fun withAnnotations(additionalAnnotations: Iterable<String>): StructElementImpl = _withAnnotations(additionalAnnotations)
    override fun withoutAnnotations(): StructElementImpl = _withoutAnnotations()
    override fun withMetas(additionalMetas: MetaContainer): StructElementImpl = _withMetas(additionalMetas)
    override fun withMeta(key: String, value: Any): StructElementImpl = _withMeta(key, value)
    override fun withoutMetas(): StructElementImpl = _withoutMetas()

    override fun writeContentTo(writer: IonWriter) {
        writer.stepIn(IonType.STRUCT)
        fields.forEach {
            writer.setFieldName(it.name)
            it.value.writeTo(writer)
        }
        writer.stepOut()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is StructElementImpl) return false
        if (annotations != other.annotations) return false

        // We might avoid materializing fieldsByName by checking fields.size first
        if (this.size != other.size) return false
        if (this.fieldsByName.size != other.fieldsByName.size) return false

        // If we make it this far we can compare the list of field names in both
        if (this.fieldsByName.keys != other.fieldsByName.keys) return false

        // If we make it this far then we have to take the expensive approach of comparing the individual values in
        // [this] and [other].
        //
        // A field group is a list of fields with the same name. Within each field group we count the number of times
        // each value appears in both [this] and [other]. Each field group is equivalent if every value that appears n
        // times in one group also appears n times in the other group.

        this.fieldsByName.forEach { thisFieldGroup ->
            val thisSubGroup: Map<AnyElement, Int> = thisFieldGroup.value.groupingBy { it }.eachCount()

            // [otherGroup] should never be null due to the `if` statement above.
            val otherGroup = other.fieldsByName[thisFieldGroup.key]
                ?: error("unexpectedly missing other field named '${thisFieldGroup.key}'")

            val otherSubGroup: Map<AnyElement, Int> = otherGroup.groupingBy { it }.eachCount()

            // Simple equality should work from here
            if (thisSubGroup != otherSubGroup) {
                return false
            }
        }

        // Metas intentionally not included here.

        return true
    }

    // Note that we are not using `by lazy` here because it requires 2 additional allocations and
    // has been demonstrated to significantly increase memory consumption!
    private var cachedHashCode: Int? = null
    override fun hashCode(): Int {
        if (this.cachedHashCode == null) {
            // Sorting the hash codes of the individual fields makes their order irrelevant.
            var result = fields.map { it.hashCode() }.sorted().hashCode()

            result = 31 * result + annotations.hashCode()

            // Metas intentionally not included here.
            cachedHashCode = result
        }
        return this.cachedHashCode!!
    }
}
