# See https://www.guardsquare.com/manual/configuration/usage
-keep class !com.amazon.ion.shaded_.** { *; }
-keepattributes *Annotation*,Signature,InnerClasses,EnclosingMethod
-dontoptimize
-dontobfuscate
-dontwarn java.sql.Timestamp
