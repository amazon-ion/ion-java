# See https://www.guardsquare.com/manual/configuration/usage
-keep,includecode class !com.amazon.ion.shaded_.** { *; }
-keepattributes Signature,InnerClasses,EnclosingMethod,Exceptions,*Annotation*
-dontoptimize
-dontobfuscate
-dontwarn java.sql.Timestamp
