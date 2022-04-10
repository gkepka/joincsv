# About joincsv
Tool for joining csv files using specified column. Prints joined csv files to standard output. Can perform join independently of files size. Uses Hash Join algorithm by default, if files are too
big to fit into memory then uses GRACE Join algoritm. GRACE Join Algorithm can perform two phases of partitioning, dividing files into up to 4096 partitions.
If still any pair of partitions is too big, then tool falls back on Nested Join algorithm. Implementation of algorithms is inspired by explanations
available [here](https://www.youtube.com/watch?v=nUwT7PEQ49o&list=PLSE8ODhjZXjbohkNBWQs_otTrBTrjyohi&index=11) and [here](https://www.youtube.com/watch?v=vPP1CwCGjVg&list=PLSE8ODhjZXja7K1hjZ01UTVDnGQdx5v5U&index=17).

# Getting Started

1. Clone the repo
```
git clone https://github.com/gkepka/joincsv.git
```
2. Go to repo directory
```
cd joincsv
```
3. Build tool
```
./gradlew build
```
4. Move tool to prefered location
```
mv build/libs/join /target/directory/join
```

# Usage
```
join file_name file_name column_name join_type
```

Accepted join types are: `inner`, `left`, `right`. If join type is not specified, then inner join will be performed. 
