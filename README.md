# Scalable Processing of Dominance Based Queries

## Description
This project implements a benchmark for evaluating the performance of three tasks:
1. Computing d-dimensional points that are NOT dominated (skyline).
2. Computing k points that have the highest dominance score.
3. Computing k points from skyline that have the highest dominance score.

To ensure scalability and performance, the code employs Apache Spark, specifically RDDs, accumulators, Spark SQL, and DataFrames. Additionally, the "Sort-Filter-Skyline" (SFS) technique is utilized to enhance computational efficiency.

## Supporting software
* Phase 1, baseline (naive) algorithm implementation [LINK](https://github.com/TeoMastro/dominance-based-queries)
* Dataset generator implemented in Google Collab [LINK](https://colab.research.google.com/drive/1aPKrecFJUGDFkRGWZu5Ni9vWQeuPugIR?usp=sharing)
* Dataset generator implemented in Python [LINK](https://github.com/TeoMastro/generate-points)

## Usage
This program was implemented using
* Intellij IDEA
* Scala plugin 
* SBT

For the execution you may use spark-shell directly, but it is recommended to lunch the program via the Intellij IDE

You can source the same datasets as the once used in the benchmark [HERE](https://drive.google.com/drive/folders/1qoeabD-O3pOwUuctfors7V73vOgIRGtl?usp=sharing) (be mindful of the relative path you use during import!)

The program's Main runs the entire benchmark automatically going through every distribution (correlated, anticorrelated, normal, uniform) of each dimension (2, 3, 4, 5) and prints the results. Feel free to change that if you want to target a combination individually.
```scala
val dimensions = Seq(2, 3, 4, 5)
val distributions = Seq("correlated", "anticorrelated", "normal", "uniform")

for {
  dimension <- dimensions
  distribution <- distributions
} yield {
  println(s"\n==== Benchmark has started | DIMENSION: ${dimension}D | DISTRIBUTION: ${distribution} | K: ${K_VALUE} ====\n")
  benchmark.run(spark, s"datasets/one_hundred_thousand_points/dimensions_${dimension}-dist_${distribution}-points_100000.csv", K_VALUE)
}
```
You can also find the output of our own execution HERE (memory: 8G, cpu: 4)
## Implementation Details

### Task 1: Skyline Query
The `skylineQuery` function computes the skyline set by performing a "Sort-Filter-Skyline" (SFS) algorithm. It utilizes Spark's DataFrame operations to calculate the sum of each row, sorts the DataFrame by the sum, and then computes the local and global skyline.

### Task 2: Top-k Dominating
The `topKDominating` function computes k points with the highest dominance score. It iteratively finds the skyline points, calculates the dominance score for each point, and removes the top point from the dataset until k points are obtained.

### Task 3: Top-k Skyline Dominance
The `topKSkyline` function computes k points from the skyline set with the highest dominance score. It calculates the dominance score for each skyline point and selects the top k points based on their scores.

## Contributors
- [sakispap](https://github.com/SakisPap)
- [TeoMastro](https://github.com/TeoMastro)

