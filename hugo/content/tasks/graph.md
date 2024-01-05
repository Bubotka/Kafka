
---
menu:
    after:
        name: graph
        weight: 1
title: Построение графа
---
{{< mermaid >}}
graph LR
1{rhombus} --> 12{rhombus}
1{rhombus} --> 23{ellipse}
1{rhombus} --> 15{round-rect}
5{square} --> 1{rhombus}
5{square} --> 7{rhombus}
5{square} --> 11{circle}
5{square} --> 23{ellipse}
6{rect} --> 21{ellipse}
6{rect} --> 19{rhombus}
7{rhombus} --> 19{rhombus}
7{rhombus} --> 11{circle}
7{rhombus} --> 12{rhombus}
7{rhombus} --> 22{rhombus}
8{square} --> 11{circle}
8{square} --> 22{rhombus}
9{round-rect} --> 23{ellipse}
10{ellipse} --> 4{rhombus}
11{circle} --> 9{round-rect}
11{circle} --> 6{rect}
12{rhombus} --> 8{square}
12{rhombus} --> 18{square}
12{rhombus} --> 9{round-rect}
13{circle} --> 10{ellipse}
13{circle} --> 6{rect}
13{circle} --> 18{square}
15{round-rect} --> 23{ellipse}
15{round-rect} --> 16{rect}
16{rect} --> 7{rhombus}
16{rect} --> 23{ellipse}
17{circle} --> 12{rhombus}
17{circle} --> 16{rect}
17{circle} --> 7{rhombus}
17{circle} --> 22{rhombus}
19{rhombus} --> 5{square}
19{rhombus} --> 13{circle}
19{rhombus} --> 11{circle}
20{rhombus} --> 10{ellipse}
20{rhombus} --> 7{rhombus}
21{ellipse} --> 19{rhombus}
21{ellipse} --> 7{rhombus}
21{ellipse} --> 18{square}
22{rhombus} --> 10{ellipse}
{{< /mermaid >}}
