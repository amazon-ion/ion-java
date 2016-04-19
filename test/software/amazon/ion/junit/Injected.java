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

package software.amazon.ion.junit;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkField;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;

/**
 * A JUnit 4 {@link Runner} that injects one or more JavaBeans properties of
 * the test fixture with a set of configured values.  This approach is similar
 * to {@link Parameterized} but utilizes setter injection instead of using
 * constructors, so its easier to reuse the injection via inheritance.
 * <p>
 * A test fixture may have no {@link Inject} parameters, in which case the
 * fixture runs as it would without the {@link Injected} runner.
 * This is useful for allowing a hierarchy of test fixtures to be declared
 * as {@link Injected} when only some of the sub-classes actually {@link Inject}
 * parameters.
 */
public class Injected
extends Suite
{

    /**
     * Annotation for a public static field which provides values to be
     * injected into the fixture. The {@code value} element of this annotation
     * must be the name of a writable property of the fixture.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public static @interface Inject
    {
        /**
         * The name of the property to inject.
         */
        String value();
    }


    private static final class Dimension
    {
        String property;
        PropertyDescriptor descriptor;
        Object[] values;
    }


    private static class InjectedRunner
    extends BlockJUnit4ClassRunner
    {
        private final Dimension[] myDimensions;
        private final int[] myValueIndices;
        private String myName;

        /**
         * @param klass
         * @throws InitializationError
         */
        public InjectedRunner(Class<?> klass,
                              Dimension[] dimensions,
                              int... indices)
        throws InitializationError
        {
            super(klass);

            assert dimensions.length == indices.length;

            myDimensions = dimensions;
            myValueIndices = indices;
        }

        @Override
        public Object createTest()
        throws Exception
        {
            Object test = getTestClass().getOnlyConstructor().newInstance();
            for (int i = 0; i < myDimensions.length; i++)
            {
                inject(test, myDimensions[i], myValueIndices[i]);
            }
            return test;
        }

        public void inject(Object target, Dimension dimension, int valueIndex)
        throws Exception
        {
            Method method = dimension.descriptor.getWriteMethod();
            Object value  = dimension.values[valueIndex];
            method.invoke(target, value);
        }

        @Override
        protected synchronized String getName()
        {
            if (myName == null)
            {
                StringBuilder buf = new StringBuilder("[");
                for (int i = 0; i < myDimensions.length; i++)
                {
                    if (i != 0) buf.append(',');

                    Dimension dim = myDimensions[i];
                    int valueIndex =  myValueIndices[i];
                    buf.append(dim.values[valueIndex]);
                }
                buf.append(']');
                myName = buf.toString();
            }
            return myName;
        }

        @Override
        protected String testName(FrameworkMethod method)
        {
            // Eclipse (Helios) can't display results properly if the names
            // are not unique.
            return method.getName() + getName();
        }

        @Override
        protected void validateConstructor(List<Throwable> errors)
        {
            validateOnlyOneConstructor(errors);
        }

        @Override
        protected Statement classBlock(RunNotifier notifier)
        {
            return childrenInvoker(notifier);
        }
    }



    /**
     * Only called reflectively. Do not use programmatically.
     */
    public Injected(Class<?> klass)
    throws Throwable
    {
        super(klass, fanout(klass));
    }

    private static List<Runner> fanout(Class<?> klass) throws Throwable
    {
        Dimension[] dimensions = findDimensions(new TestClass(klass));

        if (dimensions.length == 0)
        {
            // no dimensions in this test class--run with normal runner
            return Collections.<Runner>singletonList(new BlockJUnit4ClassRunner(klass));
        }
        return fanout(klass, new ArrayList<Runner>(), dimensions, new int[dimensions.length], 0);
    }

    private static List<Runner> fanout(Class<?> klass,
                                       List<Runner> runners,
                                       Dimension[] dimensions,
                                       int[] valueIndices,
                                       int dimensionIndex)
    throws InitializationError
    {
        assert dimensions.length == valueIndices.length;

        if (dimensionIndex == dimensions.length)
        {
            InjectedRunner runner =
                new InjectedRunner(klass, dimensions, valueIndices);
            runners.add(runner);
        }
        else
        {
            Dimension dim = dimensions[dimensionIndex];
            int width = dim.values.length;
            for (int i = 0; i < width; i++)
            {
                int[] childIndexes = valueIndices.clone();
                childIndexes[dimensionIndex] = i;
                fanout(klass, runners, dimensions, childIndexes, dimensionIndex + 1);
            }
        }
        return runners;
    }

    private static final Dimension[] EMPTY_DIMENSION_ARRAY = new Dimension[0];

    private static Dimension[] findDimensions(TestClass testClass)
    throws Throwable
    {
        List<FrameworkField> fields =
            testClass.getAnnotatedFields(Inject.class);
        if (fields.isEmpty())
        {
            return EMPTY_DIMENSION_ARRAY;
        }

        BeanInfo beanInfo = Introspector.getBeanInfo(testClass.getJavaClass());
        PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();

        Dimension[] dimensions = new Dimension[fields.size()];

        int i = 0;
        for (FrameworkField field : fields)
        {
            int modifiers = field.getField().getModifiers();
            if (! Modifier.isPublic(modifiers) || ! Modifier.isStatic(modifiers))
            {
                throw new Exception("@Inject " + testClass.getName() + '.'
                                    + field.getField().getName()
                                    + " must be public static");
            }

            Dimension dim = new Dimension();
            dim.property = field.getField().getAnnotation(Inject.class).value();
            dim.descriptor = findDescriptor(testClass, descriptors, field, dim.property);
            dim.values = (Object[]) field.get(null);
            dimensions[i++] = dim;
        }

        return dimensions;
    }


    private static PropertyDescriptor findDescriptor(TestClass testClass,
                                              PropertyDescriptor[] descriptors,
                                              FrameworkField field,
                                              String name)
    throws Exception
    {
        for (PropertyDescriptor d : descriptors)
        {
            if (d.getName().equals(name))
            {
                if (d.getWriteMethod() == null) break;  // To throw error
                return d;
            }
        }

        throw new Exception("@Inject value '" + name
                            + "' doesn't match a writeable property near "
                            + testClass.getName() + '.'
                            + field.getField().getName());
    }
}
