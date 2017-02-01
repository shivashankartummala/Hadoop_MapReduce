package nyse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/* Customized hadoop datatype created to be used as key from mapper to reducer. 
 * Refer to point 5 in Design Methodology */

@SuppressWarnings("rawtypes")
public class NYSESymbolYearWritable implements WritableComparable{
 String stock_symbol;
 int stock_year;
 @Override
 public void readFields(DataInput in) throws IOException {
  stock_symbol = in.readUTF();
  stock_year = in.readInt();
 }

 @Override
 public void write(DataOutput out) throws IOException {
  out.writeUTF(stock_symbol);
  out.writeInt(stock_year);
 }

 @Override
 // The objects of this class are sorted by stock_year and then by stock_symbol
 public int compareTo(Object o) {
  NYSESymbolYearWritable other = (NYSESymbolYearWritable)o;
  if (this.stock_year == other.stock_year){
   return this.stock_symbol.compareTo(other.stock_symbol);
  } else {
   return (this.stock_year > other.stock_year) ? 1 : -1;
  }
 }
 

 @Override
 public int hashCode() {
  final int prime = 31;
  int result = 1;
  result = prime * result
    + ((stock_symbol == null) ? 0 : stock_symbol.hashCode());
  result = prime * result + stock_year;
  return result;
 }

 @Override
 public boolean equals(Object obj) {
  if (this == obj)
   return true;
  if (obj == null)
   return false;
  if (getClass() != obj.getClass())
   return false;
  NYSESymbolYearWritable other = (NYSESymbolYearWritable) obj;
  if (stock_symbol == null) {
   if (other.stock_symbol != null)
    return false;
  } else if (!stock_symbol.equals(other.stock_symbol))
   return false;
  if (stock_year != other.stock_year)
   return false;
  return true;
 }

 @Override
 public String toString() {
  return "stock_symbol=" + stock_symbol
    + ", stock_year=" + stock_year + ",";
 }

 public String getStock_symbol() {
  return stock_symbol;
 }

 public int getStock_year() {
  return stock_year;
 }

 public void setStock_symbol(String stock_symbol) {
  this.stock_symbol = stock_symbol;
 }

 public void setStock_year(int stock_year) {
  this.stock_year = stock_year;
 }

}
