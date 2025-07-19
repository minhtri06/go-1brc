# GO-1BRC

The Go version for [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc), which was originally for Java.

The task is simple, there's a file with temperature measurements from various stations. You need to find the minimum, maximum, and average of the temperature for each station. The problem is, the file is quite big (1 billion rows), so you should optimize your solution to run faster.

The file contains multiple lines, each line has the following format: `<string: station name>;<double: measurement>`. For example, 10 rows of the file look like this:

```text
Ouahigouya;10.1
Moncton;2.6
San Francisco;13.1
Odienné;23.1
Niigata;28.2
Lahore;27.7
Dubai;23.6
Bishkek;5.2
Bucharest;19.5
Odesa;-9.6
```

After completing the aggregation, you should write the results to stdout, or a file, etc. The output should look like this:

```text
{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5}
```

For more details please check to the [origin repo](https://github.com/gunnarmorling/1brc?tab=readme-ov-file#1%EF%B8%8F%E2%83%A3%EF%B8%8F-the-one-billion-row-challenge).

## Rules and limits

[rules and limits](https://github.com/gunnarmorling/1brc?tab=readme-ov-file#rules-and-limits)

## How can I generate the file?

To generate the file, they use their provided Java program to generate a random dataset. You can check [this section](https://github.com/gunnarmorling/1brc?tab=readme-ov-file#running-the-challenge) for more details.

## Solutions Statistics

This repository contains 9 different solutions with progressive optimizations.

**Benchmark Environment:** Tested on MacBook Air M2, 16GB RAM, 8-Core CPU.

| Solution | Execution Time | Key Optimizations                                                                              |
| -------- | -------------- | ---------------------------------------------------------------------------------------------- |
| **s1**   | 74.04s         | Baseline implementation - idiomatic Go with `bufio.Scanner` and `strings.Split()`              |
| **s2**   | 60.09s         | Bytes processing + integer arithmetic (sumX10) instead of floating point                       |
| **s3**   | 56.09s         | Custom separator parsing - reverse iteration to find ';' instead of `bytes.Cut`                |
| **s4**   | 53.08s         | Pre-allocated map with initial capacity to reduce rehashing                                    |
| **s5**   | 40.89s         | Combined name/value parsing + direct map access with `string(name)`                            |
| **s6**   | 38.66s         | Custom hash map implementation with FNV-1a hashing                                             |
| **s7**   | 20.69s         | Single-pass file processing - removed `bufio.Scanner`, custom buffered reading                 |
| **s8**   | 26.09s         | Parallel processing with file chunking (based on s2 with concurrency)                          |
| **s9**   | 7.94s          | Combines parallel processing + custom map + single-pass reading (based on s7 with concurrency) |
