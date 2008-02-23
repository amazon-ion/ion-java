package amazon.platform.clienttoolkit.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonNull;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonValue;
import com.amazon.ion.impl.IonBoolImpl;
import com.amazon.ion.impl.IonFloatImpl;
import com.amazon.ion.impl.IonIntImpl;
import com.amazon.ion.impl.IonStringImpl;
import com.amazon.ion.impl.IonTimestampImpl;
import com.amazon.ion.system.StandardIonSystem;


public class OldIonTypedCreator implements Serializer<Object> {

    private static final IonSystem sys = new StandardIonSystem();

    private final Map<Class, Serializer> _serializerMap = new HashMap<Class, Serializer>();

    private final Serializer<Map<String,Object>> _mapSerializer = new Serializer<Map<String,Object>>() {

        public IonValue getIonValue(Map<String,Object> map)
        {
            IonStruct result = sys.newEmptyStruct();
            for (Map.Entry<String,Object> pair : map.entrySet()) {
                result.add(pair.getKey(), OldIonTypedCreator.this.getIonValue(pair.getValue()));
            }
            return result;
        }

        public Map<String,Object> getOrigionalValue(IonValue orig)
        {
            IonStruct struct = (IonStruct) orig;
            Map<String,Object> result = new HashMap<String,Object>();
            struct.deepMaterialize();
            for (IonValue val : struct) {
                result.put(val.getFieldName(),OldIonTypedCreator.this.getOrigionalValue(val));
            }
            return result;
        }
    };

    private final Serializer<List> _listSerializer = new Serializer<List>() {

        public IonValue getIonValue(List list)
        {
            List<IonValue> result = new ArrayList<IonValue>(list.size());
            Serializer serializer = null;
            for (Object val : list) {
                if (serializer == null)
                    serializer = OldIonTypedCreator.this.getSerializer(val.getClass());
                result.add(serializer.getIonValue(val));
            }
            return sys.newList(result);
        }

        public List getOrigionalValue(IonValue orig)
        {
            IonList list = (IonList) orig;
            List result = new ArrayList();
            list.deepMaterialize();
            Serializer serializer = null;
            for (IonValue val : list) {
                if (serializer == null)
                    serializer = OldIonTypedCreator.this.getSerializer(val.getClass());
                result.add(serializer.getOrigionalValue(val));
            }
            return result;
        }
    };

    Serializer getSerializer(Class cl) {
        Serializer serializer;

        serializer = _serializerMap.get(cl);
        if (serializer != null)
            return serializer;

        if (Map.class.isAssignableFrom(cl) || IonStruct.class.isAssignableFrom(cl))
            return _mapSerializer;
        else if (Collection.class.isAssignableFrom(cl) || cl.isArray() || IonList.class.isAssignableFrom(cl))
            return _listSerializer;
        else
            return getSerializer(cl.getSuperclass());
    }

    public IonValue getIonValue(Object o)
    {
        if (o == null)
           return sys.newNull();
        Serializer s = getSerializer(o.getClass());
        return s.getIonValue(o);
    }

    public Object getOrigionalValue(IonValue orig)
    {
        if (orig instanceof IonNull)
            return null;
        Serializer s = getSerializer(orig.getClass());
        return s.getOrigionalValue(orig);
    }

    public IonDatagram getIonDatagram(Object o) {
        return sys.newDatagram(getIonValue(o));
    }

    public Object getOrigionalValue(byte[] bytes) {
        IonLoader l = sys.newLoader();
        return getOrigionalValue(l.load(bytes).get(0));
    }

    //Init Logic
    {
        Serializer s;
        s = new Serializer<Boolean>() {
            public IonValue getIonValue(Boolean o) {
                return sys.newBool(o);
            }
            public Boolean getOrigionalValue(IonValue orig) {
                return ((IonBool)orig).booleanValue();
            }
        };
        _serializerMap.put(Boolean.class,s);
        _serializerMap.put(boolean.class,s);
        _serializerMap.put(IonBoolImpl.class,s);
        abstract class numberSerializer<T extends Number> implements Serializer<T> {
            public T getOrigionalValue(IonValue orig)
            {
                String type = orig.getTypeAnnotations()[0];
                if ("Int".equals(type))
                    return (T)(Integer)((IonInt)orig).intValue();
                else if ("Long".equals(type))
                    return (T)(Long)((IonInt)orig).longValue();
                else if ("Short".equals(type))
                    return (T)(Short)(short)((IonInt)orig).intValue();
                else if ("Byte".equals(type))
                    return (T)(Byte)(byte)((IonInt)orig).intValue();
                else if ("Float".equals(type))
                    return (T)(Float)((IonFloat)orig).floatValue();
                else if ("Double".equals(type))
                    return (T)(Double)((IonFloat)orig).doubleValue();
                throw new IllegalStateException("Corrupt datagram.");
            }
        }
        s = new numberSerializer<Integer>() {
            public IonValue getIonValue(Integer o) {
                IonValue result = sys.newInt(o);
                result.addTypeAnnotation("Int");
                return result;
            }
        };
        _serializerMap.put(Integer.class,s);
        _serializerMap.put(int.class,s);
        _serializerMap.put(IonIntImpl.class,s);

        s = new numberSerializer<Long>() {
            public IonValue getIonValue(Long o) {
                IonValue result = sys.newInt(o);
                result.addTypeAnnotation("Long");
                return result;
            }
        };
        _serializerMap.put(Long.class,s);
        _serializerMap.put(long.class,s);
        _serializerMap.put(IonIntImpl.class,s);

        s = new numberSerializer<Double>() {
            public IonValue getIonValue(Double o) {
                IonFloat result = sys.newFloat(o);
                result.addTypeAnnotation("Double");
                return result;
            }
        };
        _serializerMap.put(Double.class,s);
        _serializerMap.put(double.class,s);
        _serializerMap.put(IonFloatImpl.class,s);

        s = new numberSerializer<Float>() {
            public IonValue getIonValue(Float o) {
                IonFloat result = sys.newFloat(o);
                result.addTypeAnnotation("Float");
                return result;
            }
        };
        _serializerMap.put(Float.class,s);
        _serializerMap.put(float.class,s);
        _serializerMap.put(IonFloatImpl.class,s);

        s = new Serializer<String>() {
            public IonValue getIonValue(String o) {
                return sys.newString(o);
            }
            public String getOrigionalValue(IonValue orig) {
                return ((IonString)orig).stringValue();
            }
        };
        _serializerMap.put(String.class,s);
        _serializerMap.put(IonStringImpl.class,s);

        s = new Serializer<Date>() {
            public IonValue getIonValue(Date o) {
                return sys.newUtcTimestamp(o);
            }
            public Date getOrigionalValue(IonValue orig) {
                return ((IonTimestamp)orig).dateValue();
            }
        };
        _serializerMap.put(Date.class,s);
        _serializerMap.put(IonTimestampImpl.class,s);
    }

    /**
     *
     */
    public void test()
    {
        IonLoader loader = sys.newLoader();
        IonStruct s = sys.newStruct();
        for (int i = 0; i<5; i++) {
            IonList l = sys.newList();
            for (int j = 0;j<5;j++) {
                l.add(sys.newString("Hello: "+j));
            }
            s.add("HashKey "+i, l);
        }
        loader.load(sys.newDatagram(s).toBytes());
    }

}
