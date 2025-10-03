# See https://www.guardsquare.com/manual/configuration/usage
-keep,includecode class !com.amazon.ion.shaded_.** { *; }
-dontoptimize
-dontobfuscate
-dontwarn java.sql.Timestamp
# We don't need this at runtime, so it's okay if it's missing.
-dontwarn edu.umd.cs.findbugs.annotations.SuppressFBWarnings
