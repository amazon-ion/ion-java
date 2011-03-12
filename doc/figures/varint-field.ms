.begin dformat
style linethrutext 0
style bitwid .2
style recspread 0.05
VarInt field
        7--1-dashed 0
        --7-invis \fIpayload\fR
        --8-invis ...
        n+7--1 1
        --7-invis \fIpayload\fR
noname
        --1-invis \fIend flag\fR
        6--1 \fIsign\fR
        5-0-6 \fImagnitude\fR
        --8-invis ...
        --1-invis \fIend flag\fR
        n+6-n-7-dashed \fImagnitude\fR
.end
