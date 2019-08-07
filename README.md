# Image Matching with Map Reduce
- Find identical images by comparing hash values of images in parallel using Map Reduce.
- Handle small file problem in HDFS with SequenceFile.
- Optimize Hadoop configurations on a small cluster (1 master & 3 slaves, each with 2 processors & 8 GB memory):
```
yarn.nodemanager.resource.memory-mb: 7168
yarn.scheduler.minimum-allocation-mb: 256
mapreduce.map.memory.mb: 3072
mapreduce.reduce.memory.mb: 256
mapreduce.input.fileinputformat.split.maxsize: 3221225472
mapreduce.input.fileinputformat.split.minsize: 3221225472
```
