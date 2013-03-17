package org.dueam.hadoop.tools;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

/**
 * 商品成长分析
 *
 * @author suoni
 *
 */
public class NewItemGrowing {

	/**
	 * @param args
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws Exception {
		if(args.length <1){
			args = new String[]{"20100312","20100314","20100316","20100318","20100404","20100406","20100408","20100421","20100505","20100511",
					"20100313","20100315","20100317","20100403","20100405","20100407","20100409","20100422","20100510","20100512"};
		}
		for(String arg : args){
			proc(arg);
		}
		for(int i=0;i<allCount.length;i++){
			if(allCount[i] > 0){
				System.out.println("第"+i+"个周期(采样:"+allCount[i]+"),有效商品消亡均值=>"+(allAvSum[i]/allCount[i])+",在线商品消亡均值:"+(allOSum[i]/allCount[i]));
			}
		}

	}
	static int[] allCount = new int[100];
	static double[] allAvSum = new double[100];
	static double[] allOSum =  new double[100];
	public static void proc(String input) throws Exception{
		File baseDir = new File(input);
		Date start = DateUtils.parseDate(baseDir.getName(), new String[] { "yyyyMMdd" });
		List<ItemStatusSum> list = new ArrayList<ItemStatusSum>();
		if(baseDir.exists()){
			for(File f : baseDir.listFiles()){
				list.add(getStatus(f));
			}
		}
		int count = 0;
		ItemStatusSum last = null;
		int icount = 0;
		double isum = 0;
		double iosum = 0;
		long ifcount = 0;

		List<String> result = new ArrayList<String>();
		String cols = (baseDir.getName()+"-时间,所属周期,有效商品数,消亡的商品数,消亡百分比(%),一个周期的消亡平均值,在线商品数,消亡的在线商品数,在线商品数消亡百分比(%),一个周期的消亡平均值");
		result.add(cols);
		for(ItemStatusSum ss : list){
			if(ifcount !=0 && ifcount != ss.getCount(start)){
				icount = 0;
				isum = 0;
				iosum =0;
			}
			icount ++ ;
			double avgIsum=0,avgIosum=0;
			isum += ss.getReducePresent(last);
			iosum += ss.getOnlineReducePresent(last);

			if (icount == 7 ) {
				avgIsum = (double)((int)(isum * 10000  )/7) /10000;
				avgIosum = (double)((int)(iosum * 10000  )/7) /10000;
			}
			if(++ count >= list.size())continue;
			long c = ss.getCount(start);

			String line = (ss.getDate()+"," + ss.getCount(start) +  ","+ss.getAvItem()+","+ss.getReduce(last)+","+ss.getReducePresent(last)+","+(avgIsum>0?avgIsum:"")+","+ss.getOnline()+","+ss.getOnlineReduce(last)+","+ss.getOnlineReducePresent(last)+","+(avgIosum>0?avgIosum:""));
			//System.out.println(line);
			allAvSum[(int) ss.getCount(start)] += ss.getReducePresent(last);
			allOSum[(int) ss.getCount(start)] += ss.getOnlineReducePresent(last);
			allCount[(int) ss.getCount(start)] ++;
			if(avgIsum > 0)
			System.out.println(baseDir.getName()+"," + ss.getCount(start)+","+avgIsum+","+avgIosum);
			result.add(line);
			last = ss;
			ifcount = ss.getCount(start);
		}
		FileUtils.writeLines(new File(baseDir.getName()+".csv"), "GBK", result);

	}

	public static ItemStatusSum getStatus(File file) throws IOException {
		String date = file.getName();
		long sum = 0, online = 0, instock = 0, del = 0;
		for (String line : (List<String>) FileUtils.readLines(new File(file,
				"part-00000"))) {
			String[] array = StringUtils.split(line, "\t");
			if (array.length == 2) {
				long count = Long.parseLong(array[1]);
				int status = Integer.parseInt(array[0]);
				switch (status) {
				case -1:
				case -4:
					del += count;
					break;
				case 0:
				case 1:
					online += count;
					break;
				case -2:
				case -3:
				case -5:
				case -9:
					instock += count;
					break;
				}
				sum += count;
			}
		}
		return new ItemStatusSum(sum, online, instock, date,del);
	}

	static class ItemStatusSum {
		private String date;
		private long del;

		public long getDel() {
			return del;
		}

		public void setDel(long del) {
			this.del = del;
		}

		public String getDate() {
			return date;
		}

		public void setDate(String date) {
			this.date = date;
		}

		public long getCount(Date start) throws ParseException{
			Date now =  DateUtils.parseDate(date, new String[] { "yyyyMMdd" });
			return ( now.getTime() - start.getTime()) / (24 * 3600 * 1000)/7 + 1;
		}

		public long getReduce(ItemStatusSum last){
			if(null == last)return 0;
			return last.getAvItem() - this.getAvItem();
		}

		public double getReducePresent(ItemStatusSum last){
			if(null == last)return 0;
			return (double)((last.getAvItem() - this.getAvItem())*10000/last.getAvItem())/100;
		}

		public long getOnlineReduce(ItemStatusSum last){
			if(null == last)return 0;
			return last.getOnline() - this.getOnline();
		}

		public double getOnlineReducePresent(ItemStatusSum last){
			if(null == last)return 0;
			return (double)((last.getOnline() - this.getOnline())*1000/last.getOnline())/100;
		}

		public ItemStatusSum(long sum, long online, long instock, String date,long del) {
			super();
			this.sum = sum;
			this.online = online;
			this.instock = instock;
			this.date = date;
			this.del = del;
		}
		private long getAvItem(){
			return sum-del;
		}
		public long getSum() {
			return sum;
		}

		public void setSum(long sum) {
			this.sum = sum;
		}

		public long getOnline() {
			return online;
		}

		public void setOnline(long online) {
			this.online = online;
		}

		public long getInstock() {
			return instock;
		}

		public void setInstock(long instock) {
			this.instock = instock;
		}

		private long sum;
		private long online;
		private long instock;

	}
}
