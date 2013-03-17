package org.dueam.hadoop.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.dueam.hadoop.utils.TaobaoPath;

public class Growing {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		List<String> atime = new ArrayList<String>();
		List<String> otime = new ArrayList<String>();
		List<Long> aCount = new ArrayList<Long>();
		List<Long> oCount = new ArrayList<Long>();
		for(String line:(List<String>)FileUtils.readLines(new File("part-00000"), "GBK")){
			if(line.startsWith("online:")){
				String[] array = line.split("\t");
				otime.add(array[0].substring("online:".length()));
				oCount.add(Long.parseLong(array[1]));
			}else if(line.startsWith("av:")){
				String[] array = line.split("\t");
				atime.add(array[0].substring("av:".length()));
				aCount.add(Long.parseLong(array[1]));
			}

		}
		atime.remove(atime.size()-1);
		otime.remove(otime.size()-1);
		aCount.remove(aCount.size()-1);
		oCount.remove(oCount.size()-1);

		String start = "2010-05-30";
		long asum = 905966854,hasum = 905966854,osum = 510831167,iosum = 510831167/*598957643*/,iosum2 = 510831167,iosum3=510831167;
		List<String> result = new ArrayList<String>();
		result.add("时间,发布的有效商品数,净增长的有效商品数,新发布的在线商品数,净增长的在线商品,新发商品数（不去销售周期）");
		System.out.println("时间,有效商品数(停历史迁移),在线商品(去销售周期/停历史迁移),在线商品(不去销售周期/停历史迁移),有效商品数(不停历史迁移),在线商品(去销售周期/不停历史迁移),在线商品(不去销售周期/不停历史迁移)");
		int chufa = 1700000;
		for(int i=0;i<365;i++){

			long aReduce = reduce(aCount);
			long onlineReduce = reduceOnline(oCount);
			atime.add(start);
			aCount.add(av[i%7]);
			otime.add(start);
			oCount.add(online[i%7]);
			asum += (av[i%7]-aReduce);
			hasum += (av[i%7]-aReduce - (long)(8000000*(1-0.63)));
			osum += (online[i%7]-onlineReduce); //停历史迁移
			iosum += ((long)(online[i%7])-onlineReduce -(long)(8000000*(1-0.63)*0.6) );
			iosum2 += ((long)(online[i%7]* 510831167 /598957643)-onlineReduce);//停历史迁移
			iosum3 += ((long)(online[i%7] * 510831167 /598957643)-onlineReduce -(long)(8000000*(1-0.63)*0.6));
			result.add(start +"," + av[i%7] +"," + (av[i%7]-aReduce)+","+ online[i%7] +","  + (online[i%7]-onlineReduce)+ (long)(online[i%7] * iosum /598957643)  + ",");
			start = TaobaoPath.newDates2(start, 1);
//System.out.println(((long)(online[i%7])-onlineReduce -(long)(8000000*(1-0.63)*0.6) ) +"," + ((long)(online[i%7] * 510831167 /598957643)-onlineReduce -(long)(8000000*(1-0.63)*0.6)));
			asum -= chufa;
			hasum -= chufa;
			osum -= chufa;
			iosum -= chufa;
			iosum2 -= chufa;
			iosum3 -= chufa;

			if(i == 7 || i == 30 || i==90 || i== 364){
				if(false){
				System.out.println("第"+(i+1)+"天之后有效商品总数：" + asum);
				System.out.println("第"+(i+1)+"天之后在线商品总数：" + osum);
				System.out.println("第"+(i+1)+"天之后有效商品总数(不停历史迁移)：" + hasum);

				System.out.println("第"+(i+1)+"天之后在线商品（不去销售周期）总数：" + iosum);
				System.out.println("第"+(i+1)+"天之后在线商品（不去销售周期和停历史库）总数：" + iosum2);
				System.out.println();}
				//System.out.println("第"+(i+1)+"天"+","+asum+","+hasum+","+osum+","+iosum+","+iosum2+","+(iosum2-2000000*(i+1)));
				System.out.println("第"+(i+1)+"天后"+","+asum+","+osum+","+iosum2+","+hasum+","+iosum+","+iosum3);
			}

		}
		FileUtils.writeLines(new File("增长趋势.csv"), "GBK", result);
	}

	static long[] av = new long[]{8574008,9306690,9897346,10635653,10269332,10271416,9936398};
	static long[] online = new long[]{6341918,6775718,7334658,7978549,7864291,8165596,8135453};
	static int rdays = 60;
	public static long reduce(List<Long> count){
		long sum = 0;
		for(int i=1;i<rdays;i++){
			Long t = reduceItemCount(count.get(count.size() -i),i);
			//System.out.println( count.get(count.size() -i) + "t=>"+t);
			sum += (count.get(count.size() -i) - t);
			count.set(count.size() -i,t);
		}
		return sum;
	}
	public static long reduceOnline(List<Long> count){
		long sum = 0;
		for(int i=1;i<rdays;i++){
			Long t = reduceOnlineItemCount(count.get(count.size() -i),i);
			sum += (count.get(count.size() -i) - t);
			count.set(count.size() -i,t);
		}
		return sum;
	}

	public static long reduceItemCount(long firstCount,int days){
		/*
		if(days <= 7){
			return (long)(firstCount *  Math.pow((double)(1-0.015),(double)days));
		}else if(days <= 14){
			return (long)(firstCount *  Math.pow((double)(1-0.015),7)*Math.pow((double)(1-0.01),days-7));
		}else{
			return (long)(firstCount *  Math.pow((double)(1-0.015),7)*Math.pow((double)(1-0.01),7)*Math.pow((double)(1-0.075),days-7));
		}
		*/
		if(days <= 7){
			return (long)(firstCount *  (1-0.01446));
		}else if(days <= 14){
			return (long)(firstCount *  (1-0.01073));
		}else if(days <= 21){
			return (long)(firstCount *  (1-0.00875));
		}else if(days <= 28){
			return (long)(firstCount *  (1-0.00708));
		}else if(days <= 42){
			return (long)(firstCount *  (1-0.0057));
		}else{
			return (long)(firstCount *  (1-0.0025));
		}

	}

	public static long reduceOnlineItemCount(long firstCount,int days){
		/*
		if(days <= 7){
			return (long)(firstCount *  Math.pow((double)(1-0.0025),(double)days));
		}else if(days <= 14){
			return (long)(firstCount *  Math.pow((double)(1-0.0025),7)*Math.pow((double)(1-0.0019),days-7));
		}else{
			return (long)(firstCount *  Math.pow((double)(1-0.0025),7)*Math.pow((double)(1-0.0019),7)*Math.pow((double)(1-0.0011),days-7));
		}
		*/
		if(days <= 7){
			return (long)(firstCount *  (1-0.00227));
		}else if(days <= 14){
			return (long)(firstCount *  (1-0.0021));
		}else if(days <= 21){
			return (long)(firstCount *  (1-0.00091));
		}else if(days <= 28){
			return (long)(firstCount *  (1-0.00114));
		}else if(days <= 42){
			return (long)(firstCount *  (1-0.00915));
		}else{
			return (long)(firstCount *  (1-0.00625));
		}
	}


}
