package nyse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/* Customized hadoop datatype created to be used as value from mapper to reducer. 
 * Refer to point 5 in Design Methodology */

public class NYSEWritable implements Writable {

 String file_identifier;
 String stock_exchange;
 String stock_symbol;
 String stock_date;
 double stock_price_open;
 double stock_price_high;
 double stock_price_low;
 double stock_price_close;
 long stock_volume;
 double stock_price_adj_close;
 double stock_dividend;

 @Override
 public void readFields(DataInput in) throws IOException {
  file_identifier = in.readUTF();
  stock_exchange = in.readUTF();
  stock_symbol = in.readUTF();
  stock_date = in.readUTF();
  stock_price_open = in.readDouble();
  stock_price_high = in.readDouble();
  stock_price_low = in.readDouble();
  stock_price_close = in.readDouble();
  stock_volume = in.readLong();
  stock_price_adj_close = in.readDouble();
  stock_dividend = in.readDouble();
 }

 @Override
 public void write(DataOutput out) throws IOException {
  out.writeUTF(file_identifier);
  out.writeUTF(stock_exchange);
  out.writeUTF(stock_symbol);
  out.writeUTF(stock_date);
  out.writeDouble(stock_price_open);
  out.writeDouble(stock_price_high);
  out.writeDouble(stock_price_low);
  out.writeDouble(stock_price_close);
  out.writeLong(stock_volume);
  out.writeDouble(stock_price_adj_close);
  out.writeDouble(stock_dividend);
 }

 public String getFile_identifier() {
  return file_identifier;
 }

 public void setFile_identifier(String file_identifier) {
  this.file_identifier = file_identifier;
 }

 public String getStock_exchange() {
  return stock_exchange;
 }

 public String getStock_symbol() {
  return stock_symbol;
 }

 public String getStock_date() {
  return stock_date;
 }

 public double getStock_price_open() {
  return stock_price_open;
 }

 public double getStock_price_high() {
  return stock_price_high;
 }

 public double getStock_price_low() {
  return stock_price_low;
 }

 public double getStock_price_close() {
  return stock_price_close;
 }

 public long getStock_volume() {
  return stock_volume;
 }

 public double getStock_price_adj_close() {
  return stock_price_adj_close;
 }

 public double getStock_dividend() {
  return stock_dividend;
 }

 public void setStock_exchange(String stock_exchange) {
  this.stock_exchange = stock_exchange;
 }

 public void setStock_symbol(String stock_symbol) {
  this.stock_symbol = stock_symbol;
 }

 public void setStock_date(String stock_date) {
  this.stock_date = stock_date;
 }

 public void setStock_price_open(double stock_price_open) {
  this.stock_price_open = stock_price_open;
 }

 public void setStock_price_high(double stock_price_high) {
  this.stock_price_high = stock_price_high;
 }

 public void setStock_price_low(double stock_price_low) {
  this.stock_price_low = stock_price_low;
 }

 public void setStock_price_close(double stock_price_close) {
  this.stock_price_close = stock_price_close;
 }

 public void setStock_volume(long stock_volume) {
  this.stock_volume = stock_volume;
 }

 public void setStock_price_adj_close(double stock_price_adj_close) {
  this.stock_price_adj_close = stock_price_adj_close;
 }

 public void setStock_dividend(double stock_dividend) {
  this.stock_dividend = stock_dividend;
 }

}
