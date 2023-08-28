```text
hello java         --> 偏移量0
hello php          --> 偏移量12 （hello java的长度为10个字符，加上行尾的换行符，一共是12个字节）  "hello java"有 10 个字符，加上换行符的 CR 和 LF，总共是 12 个字符。
hello go           --> 偏移量23 （hello java和hello php的长度加起来是19个字符，再加上两个换行符，一共是23个字节）
hello C++          --> 偏移量33 （hello java、hello php和hello go的长度加起来是33个字符，再加上三个换行符，一共是33个字节）
hello C            --> 偏移量44 （hello java、hello php、hello go和hello C++的长度加起来是44个字符，再加上四个换行符，一共是44个字节）

```
