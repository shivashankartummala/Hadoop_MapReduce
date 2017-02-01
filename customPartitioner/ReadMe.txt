Custom Partitioner

This example illustrates how to use customized partitioner in a MapReduce program. The partitioning phase takes place after the map phase and before the reduce phase. The number of partitions is equal to the number of reducers. The data gets partitioned across the reducers according to the partitioning function[1] . The difference between a partitioner and a combiner is that the partitioner divides the data according to the number of reducers so that all the data in a single partition gets executed by a single reducer. However, the combiner functions similar to the reducer and processes the data in each partition. The combiner is an optimization to the reducer. The default partitioning function is the hash partitioning function where the hashing is done on the key. However it might be useful to partition the data according to some other function of the key or the value.

For this example we do not consider a graph problem. Let's consider the data that has input in the following format:

Input Format:
name<tab>age<tab>gender<tab>score

We will use custom partitioning in MapReduce program to find the maximum scorer in each gender and three age categories: less than 20, 20 to 50, greater than 50.

Input

Alice<tab>23<tab>female<tab>45
Bob<tab>34<tab>male<tab>89
Chris<tab>67<tab>male<tab>97
Kristine<tab>38<tab>female<tab>53
Connor<tab>25<tab>male<tab>27
Daniel<tab>78<tab>male<tab>95
James<tab>34<tab>male<tab>79
Alex<tab>52<tab>male<tab>69
Nancy<tab>7<tab>female<tab>98
Adam<tab>9<tab>male<tab>37
Jacob<tab>7<tab>male<tab>23
Mary<tab>6<tab>female<tab>93
Clara<tab>87<tab>female<tab>72
Monica<tab>56<tab>female<tab>92
Mark<tab>34<tab>female<tab>46
Gary<tab>44<tab>male<tab>81
Drum<tab>57<tab>male<tab>91
Kris<tab>34<tab>female<tab>53
Connoly<tab>32<tab>male<tab>25
Dan<tab>58<tab>male<tab>96
Jameson<tab>54<tab>male<tab>75
Alexa<tab>42<tab>male<tab>66
Nathan<tab>17<tab>female<tab>99
Adamson<tab>19<tab>male<tab>32
Jacobsob<tab>17<tab>male<tab>33
Maria<tab>16<tab>female<tab>94
Claire<tab>61<tab>female<tab>74
Mony<tab>57<tab>female<tab>96

Output:

Partition - 0: (this partition contains the maximum scorers for each gender whose age is less than 20)

Nathan	age- 17	female	score-99
Adam	age- 9	male	score-37

Partition - 1: (this partition contains the maximum scorers for each gender whose age is between 20 and 50)

Kristine	age- 38	female	score-53
Bob	age- 34	male	score-89

Partition - 2: (this partition contains the maximum scorers for each gender whose age is greater than 50)

Mony	age- 57	female	score-96
Chris	age- 67	male	score-97

