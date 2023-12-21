# See https://www.guardsquare.com/manual/configuration/usage
-keep,includecode class !com.amazon.ion.shaded_.** { *; }
-dontoptimize
-dontobfuscate
-dontwarn java.sql.Timestamp
