/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

/**
 * A design pattern for optional extension interfaces, in a manner more
 * flexible than class inheritance.
 * <p>
 * A <em>facet</em> is an interface representing optional functionality that
 * may be supported by one or more subject types.  By itself, the facet looks
 * no different than any other interface.  What's different is how instances
 * of the facet are acquired from the subjects that support it.
 * <p>
 * Traditionally, a class supports optional functionality via subtypes, and
 * the user typically performs an {@code instanceof} operation before
 * downcasting to the optional type.
 * This works well for simple designs, but it runs into problems when combined
 * with design patterns like Decorator or Adaptor.
 * For example, a Decorator provides only modified functionality and can't be
 * responsible for implementing all possible extensions.
 * <p>
 * The Facet pattern resolves this problem by allowing the implementation of
 * the facet to be separate from the subject instance.  A Decorator can choose
 * to "pass-through" facets of the decorated subject, without implementing the
 * functionality itself.  This also allows different instances of the subject
 * to support different sets of facets, based on its particular state.
 * <p>
 * The central focus of the pattern is the
 * {@link software.amazon.ion.facet.Faceted#asFacet(Class)} method.
 * Subjects that wish to support facets implement it to allow users to request
 * the desired facet.
 * This method returns null when the subject doesn't support the facet.
 * <p>
 * The {@code asFacet()} method is used as a replacement
 * for the traditional {@code instanceof}/typecast idiom.  The subject is in
 * control of the result, and can do any number of things more sophisticated
 * than simply downcasting itself to the facet type.
 *
 *
 * <h2>Design Notes</h2>
 *
 * In general, facet interfaces should not extend the subject type.
 * This makes use of the facet a bit less convenient, since the user must
 * retain references to both the facet and its subject.
 * However, such extension can lead to challenging implementation problems,
 * especially when the subject is a decorator, adaptor, or similar wrapper
 * around the actual provider of the facet.
 * <p>
 * Given a concrete {@link software.amazon.ion.facet.Faceted} class, it may be that
 * some instances support a particular facet while others do not, depending on
 * the state of the subject or the way it was constructed. In such cases
 * {@link software.amazon.ion.facet.Faceted#asFacet asFacet} should choose whether
 * to return the facet based on the subject's state.
 * Such classes should <em>not</em> extend the facet interface (directly or
 * indirectly), since that allows clients to bypass {@code asfacet} and
 * simply downcast the subject to the facet,
 * causing problems for instances that can't support the facet.
 *
 *
 * <h2>Acknowledgements</h2>
 *
 * This is an adaptation of the Extension Objects pattern as written by
 * Erich Gamma.  It was primarily inspired by ISO C++ {@code locale} facets.
 *
 */
package software.amazon.ion.facet;
