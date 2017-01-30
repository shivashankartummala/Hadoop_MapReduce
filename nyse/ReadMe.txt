This program is the MRv2 API version of the program discussed at http://atomkpadhy.blogspot.com/2013/08/an-example-java-map-reduce-program.html

Requirements:

Now lets consider generating reports split by years which shows the below fields:
stock_symbol, company_name, max_stock_price for the year for the company, min_stock_price for the year for the company, average dividends for the year for the company. Reports for different years have to go to different output files.

Design Considerations:

Point 1: As in most MapReduce Program cases, our mapper(s) will do the filtration and transformation of the input data and our reducer will do the aggregation and generation of desired output.

Point 2: Since I use data from multiple files, I have to join the input files to generate the desired report. Since the dividends and Prices files are huge, I am going to use MultipleInputs Class on them and create two Mappers one for each file. The two mappers are going to generate similar key-value pairs and there will be a identifier in the values which will tell the reducer about which file the key-value pair has come from. Thus this program is going to be a classic example of joining input files based on some keys.

Point 3: Since I am dealing with multiple inputs and outputs, it would be easier implementing the MapReduce job using the deprecated(old) MapRed API.

Point 4: The NYSE Companies File is not a huge one and thus qualifies to be used as a Distributed Cache File to map the company_symbol to the Company_Name.

Point 5: I create two Customized Hadoop Datatypes(Objects) to be passed from the mappers to the reducer as key-value pairs - NYSEWritable to be used as the value and NYSESymbolYearWritable to be used as the Key. Since its used as the key, NYSESymbolYearWritable has to implement WritableComparable. NYSEWritable implements Writable interface. Hence You can learn creating and using Hadoop User Defined Data Types.

Point 6: I use the MultipleOutputFormat class to split the reducer output to files in different directories based on the Year. This class is defined in job.setOutputFormat. This is another important feature of MapReduce Frame Work that can be learnt from this example.
