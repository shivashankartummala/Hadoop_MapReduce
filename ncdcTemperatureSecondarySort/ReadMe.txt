
Implementation Details

To implement the secondary sort feature, we need additional plug-in Java classes. We have to tell the MapReduce/Hadoop framework:

 -How to sort reducer keys
 -How to partition keys passed to reducers (custom partitioner)
 -How to group data that has arrived at each reducer
 
 
 This approach involves creating a composite key by adding a part of, or the entire value to the natural key to achieve your sorting objectives. The trade off between these two approaches is doing an explicit sort on values in the reducer would most likely be faster(at the risk of running out of memory) but implementing a “value to key” conversion approach, is offloading the sorting the MapReduce framework, which lies at the heart of what Hadoop/MapReduce is designed to do. For the purposes of this post, we will consider the “value to key” approach. We will need to write a custom partitioner to ensure all the data with same key (the natural key not including the composite key with the value) is sent to the same reducer and a custom Comparator so the data is grouped by the natural key once it arrives at the reducer.
 
 Value to Key Conversion
Creating a composite key is straight forward. What we need to do is analyze what part(s) of the value we want to account for during the sort and add the appropriate part(s) to the natural key. Then we need to work on the compareTo method either in key class, or comparator class to make sure the composite key is accounted. We will be re-visiting the weather data set and include the temperature as part of the natural key (the natural key being the year and month concatenated together). The result will be a listing of the coldest day for a given month and year. This example was inspired from the secondary sorting example found in Hadoop, The Definitive Guide book. While there are probably better ways to achieve this objective, but it will be good enough to demonstrate how secondary sorting works.

Mapper Code
Our mapper code already concatenates the year and month together, but we will also include the temperature as part of the key. Since we have included the value in the key itself, the mapper will emit a NullWritable, where in other cases we would emit the temperature.
Now we have added the temperature to the key, we set the stage for enabling secondary sorting. What’s left to do is write code taking temperature into account when necessary. Here we have two choices, write a Comparator or adjust the compareTo method on the TemperaturePair class (TemperaturePair implements WritableComparable). In most cases I would recommend writing a separate Comparator, but the TemperaturePair class was written specifically to demonstrate secondary sorting, so we will modify the TemperaturePair class compareTo method.If we wanted to sort in descending order, we could simply multiply the result of the temperature comparison by a -1.
Now that we have completed the part necessary for sorting, we need to write a custom partitioner.

Partitoner Code
To ensure only the natural key is considered when determining which reducer to send the data to, we need to write a custom partitioner. The code is straight forward and only considers the yearMonth value of the TemperaturePair class when calculating the reducer the data will be sent to.While the custom partitioner guarantees that all of the data for the year and month arrive at the same reducer, we still need to account for the fact the reducer will group records by key.

Grouping Comparator
Once the data reaches a reducer, all data is grouped by key. Since we have a composite key, we need to make sure records are grouped solely by the natural key. This is accomplished by writing a custom GroupPartitioner. We have a Comparator object only considering the yearMonth field of the TemperaturePair class for the purposes of grouping the records together.
