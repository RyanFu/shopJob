package org.dueam.hadoop.utils;
import java.util.ArrayList;
import java.util.List;
/**
 * job help util
 * @author zhuliu 2010-10-27
 *
 */
public class JobStringUtil {
	/**
	 * ����Ʒ�����ݽ����з�
	 * @param str
	 * @param separatorChars
	 * @return
	 */
	public static String[] splitPreserveAllTokens(String str, String separatorChars) {
        return splitWorker(str, separatorChars, -1, true);
    }
   
       private static String[] splitWorker(String str, String separatorChars, int max, boolean preserveAllTokens) {

        if (str == null) {
            return null;
        }
        int len = str.length();
        if (len == 0) {
            return new String[0];
        }
        List<String> list = new ArrayList<String>();
        int sizePlus1 = 1;
        int i = 0, start = 0;
        boolean match = false;
        boolean lastMatch = false;
        if (separatorChars == null) {
            // Null separator means use whitespace
            while (i < len) {
                if (Character.isWhitespace(str.charAt(i))) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                } else {
                    lastMatch = false;
                }
                match = true;
                i++;
            }
        } else if (separatorChars.length() == 1) {
            // Optimise 1 character case
            char sep = separatorChars.charAt(0);
            while (i < len) {
                if (str.charAt(i) == sep) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                } else {
                    lastMatch = false;
                }
                match = true;
                i++;
            }
        } else {
            // standard case
            while (i < len) {
                if (separatorChars.indexOf(str.charAt(i)) >= 0) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
           
         start = ++i;
                    continue;
                } else {
                    lastMatch = false;
                }
                match = true;
                i++;
            }
        }
        if (match || (preserveAllTokens && lastMatch)) {
            list.add(str.substring(start, i));
        }
        return (String[]) list.toArray(new String[list.size()]);
    }
       
	/**
	 * ����Ʒ��PV���ҳ�pid��Ӧ��vid
	 * @param auctionPvStr
	 * @param pid
	 * @return
	 */
	public static long getVidFromAuctionPV(final String auctionPvStr, long pid) {

		// Ҫ���ص�vid(Ĭ����-1������Ʒ�в�����)
		long vid = -1;

		// ����: ��Ʒ�ĸ������Ƿ��ж������ֵ
		int count = 0;
		
		//auctionPvStr����Ϊ��
		if(auctionPvStr==null){
			return vid;
		}

		// 123:456;789:000
		String[] auctionPVArrays = auctionPvStr.split(";");
		if (auctionPVArrays != null && auctionPVArrays.length > 0) {
			for (String pv : auctionPVArrays) {
				// 123:456
				if (pv != null && !pv.equals("")) {
					String[] pvArrays = pv.split(":");
					if (pvArrays != null && pvArrays.length == 2) {
						// �ҵ�pid��
						try {
							if (pid == Long.parseLong(pvArrays[0])) {
								vid = Long.parseLong(pvArrays[1]);
								count++;
							}
						} catch (NumberFormatException e) {
							return -1;
						}
					}
				}
			}
		}

		// ��Ʒ�к��ж�������Ե�����ֵ
		if (count > 1) {
			vid = -1;
		}

		if (vid <= 0) {
			vid = -1;
		}
		return vid;
	}
}
