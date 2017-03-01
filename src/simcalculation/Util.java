package simcalculation;

import java.util.ArrayList;
import java.util.List;

/**
 * 工具类
 * 
 * @date 2016-7-11
 * @author dengxing
 */
public class Util {
	
	/**
	 * 字符串分割
	 * 
	 * @param str 待处理字符串
	 * @param str1 分割字符
	 * @return  分割后的字符串的list
	 */
	public static List<String>  split(String str,String str1)
	{
		List<String> list = new ArrayList<String>();
		String[] ss = str.split(str1);
		for(String s:ss)
		{
			list.add(s);
		}
		return list;		
	}
    
}
