
This part of the project documentation focuses on a *task-oriented* approach.
Use it as a guide to accomplish implementing your own `Waterfall` class.

## Abstract methods

The  `PandasWaterfall` and `SparkWaterfall` classes inherent from the `Waterfall` base class. The base `Waterfall` class has two abstract methods that need to implemented in any class inheriting from it.

### _count_or_distinct

::: waterfall_logging.log.Waterfall._count_or_distinct
    options:
      show_root_toc_entry: False
      show_root_heading: False
      show_root_full_path: False
      show_signature_annotations: True

### _count_or_distinct_dropna

::: waterfall_logging.log.Waterfall._count_or_distinct_dropna
    options:
      show_root_toc_entry: False
      show_root_heading: False
      show_root_full_path: False
      show_signature_annotations: True


## Contributions

If you find a better way to implement these functions (for instance, a computationally less expensive way in Spark), please contribute to this package in Github!
