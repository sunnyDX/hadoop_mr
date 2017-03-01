package simcalculation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
* mapreduce相似度计算(余弦相似度)
* 
* @date 2016-7-11
* @author dengxing
*/
public class RecommendCosin {

  private  static final  String  COLON = ":";                      // 冒号
  private  static final  String  COMMA = ",";                      // 逗号
  private  static  final String  TABLE_DELIMETER =  "\t";          // TABLE键
  /**
   * 第一阶段mapper 输入value= userId,itemId1:w1,itemId2:w2,,,,,
   * 
   * @date 2016-7-11
   * @author dengxing
   */
  static  class  FirstStageMapper extends Mapper<Object, Text, Text, Text> {
      protected void map(Object key,Text value,Context context)
              throws java.io.IOException, InterruptedException {
        if (null == value) {
            return;
        }
        String text = value.toString();
        int beginIndex = 0;
        // 截取key  value
        beginIndex = text.indexOf(",");
        if (beginIndex == -1) {
            return;
        }
        String userId = text.substring(0, beginIndex);
        String content = text.substring(beginIndex + 1);
        // 根据','分开value
        List<String>  contentList = Util.split(content, COMMA);
        if (contentList.size() ==  0) {
            return ;
        }
        for (String  subContent : contentList) {
            // 截取itemId
            beginIndex = subContent.indexOf(COLON);
            // 输出  key = userId  value = itemId +  '\t' + w1
            context.write(new Text(userId),new Text(subContent.substring(0,beginIndex)+ TABLE_DELIMETER + subContent.substring(beginIndex + 1)));
        }
        }

  }
    /**
     * 第一阶段Reducer 输入：key = userId value = itemId \t w，，，，
     * 
     * @date 2016-7-11
     * @author dengxing
     */
   static  class  FirstStageReducer  extends  Reducer<Text, Text, Text, Text> {
       private int  beginIndex = 0;
       private Text value = new Text();
       private double weight = 0;
       private double distance = 0;
       protected void reduce(Text key,Iterable<Text> values,Context context)
               throws java.io.IOException, InterruptedException {
           if (null == key.toString() || key.toString().equals("")) {
               return;
           }
           Iterator<Text> text = values.iterator();
           List<String> valuesList = new ArrayList<String>();
           double  wscore = 0;
           while (text.hasNext()) {
               // 由于迭代器中保存的是引用，需要转化为值
               value.set(text.next().toString().getBytes());
               beginIndex = value.toString().indexOf(TABLE_DELIMETER);
               String courseId=value.toString().substring(0,beginIndex);
               double weight = Double.valueOf(value.toString().substring(beginIndex + 1));
               wscore +=  weight * weight;
               valuesList.add(courseId+TABLE_DELIMETER+weight);
           }
           distance = Math.sqrt(wscore);
           for (String  value : valuesList) {
               //输出  key = userId, value = itemId \t weight \t distance 
               context.write(key, new Text( value+ TABLE_DELIMETER + distance));
           }
       }
   }

   /**
    * 第二阶段mapper
    * 
    * @date 2016-7-11
    * @author dengxing
    */
   static class SecondStageMapper extends Mapper<Object, Text, Text, Text> {
       private Text textKey = new Text();
       protected void map(Object key,Text value,Context context)
               throws java.io.IOException, InterruptedException {
           if (null == value) {
               return;
           }
           List<String>  retList = Util.split(value.toString(), TABLE_DELIMETER);
           // retList.get(0)=userId  retList.get(1)=itemId retList.get(2)=weight retList.get(3)=distance
           if (retList.size() == 4) {
               //输出   key = itemId   value = userId \t distance \t weight
               if (null != retList.get(1) && !retList.get(1).equals("")) {
                   textKey.set(retList.get(1).toString());
                   context.write(textKey, new Text(retList.get(0) + TABLE_DELIMETER + retList.get(3) +  TABLE_DELIMETER + retList.get(2)));
               }
           }
        }
       }
  /**
   * 第二阶段Reducer
   * 
   * @date 2016-7-11
   * @author dengxing
   */
  static  class  SecondStageReducer  extends  Reducer<Text, Text, Text, Text> {
      private  Text value = new Text();
      protected void reduce(Text key,Iterable<Text> values,Context context)
              throws java.io.IOException, InterruptedException {
          if (null == key.toString() || key.toString().equals("")) {
              return;
          }
          Iterator<Text> text = values.iterator();
          List<String> valuesList = new ArrayList<String>();
          while (text.hasNext()) {
              // 由于迭代器中保存的是引用，需要转化为值
              value.set(text.next().toString().getBytes());
              List<String>  retList = Util.split(value.toString(), TABLE_DELIMETER);
              // retList.get(0) = userId,retList.get(1) = distance,retList.get(2) = weight
              if (retList.size() == 3) {  
                  String userId = retList.get(0);
                  double distance = Double.valueOf(retList.get(1));
                  double weight = Double.valueOf(retList.get(2));
                  valuesList.add(userId +TABLE_DELIMETER+ distance + TABLE_DELIMETER + weight);
              }
             
          }
          // 计算一个列表的两两组合
          // 输出    key = userid1 \t userid2   value = w1*w2  \t  distance1 * distance2
          for (int i = 0; i < valuesList.size(); i++) {
              for (int j = i + 1; j < valuesList.size(); j++) {
                  // 每次比较将小的ID放在前面
                  if (valuesList.get(i).split(TABLE_DELIMETER)[0].compareTo(valuesList.get(j).split(TABLE_DELIMETER)[0]) < 0) {
                      context.write(new Text(valuesList.get(i).split(TABLE_DELIMETER)[0] + TABLE_DELIMETER + valuesList.get(j).split(TABLE_DELIMETER)[0]),
                          new Text(Double.parseDouble(valuesList.get(i).split(TABLE_DELIMETER)[2]) * Double.parseDouble(valuesList.get(j).split(TABLE_DELIMETER)[2])+ TABLE_DELIMETER +
                              Double.parseDouble(valuesList.get(i).split(TABLE_DELIMETER)[1]) * Double.parseDouble(valuesList.get(j).split(TABLE_DELIMETER)[1])));
                  } else {
                      context.write(new Text(valuesList.get(j).split(TABLE_DELIMETER)[0] + TABLE_DELIMETER + valuesList.get(i).split(TABLE_DELIMETER)[0]),
                    		  new Text(Double.parseDouble(valuesList.get(i).split(TABLE_DELIMETER)[2]) * Double.parseDouble(valuesList.get(j).split(TABLE_DELIMETER)[2])+ TABLE_DELIMETER +
                                      Double.parseDouble(valuesList.get(i).split(TABLE_DELIMETER)[1]) * Double.parseDouble(valuesList.get(j).split(TABLE_DELIMETER)[1])));
                  }
              }

          }
      }
     }

  /**
   * 第三阶段mapper
   * 
   * @date 2016-7-11
   * @author dengxing
   */
  static class ThirdStageMapper extends Mapper<Object, Text, Text, Text> {
      protected void map(Object key,Text value,Context context)
              throws java.io.IOException, InterruptedException {
          if (null == value.toString() || value.toString().equals("")) {
              return;
          }
          // 截取key  value
          List<String> retList = Util.split(value.toString(), TABLE_DELIMETER);
          // retList.get()  0 = userId1  1 = userId2 2 = weight 3 = distance
          if (retList.size() == 4) {
              // 输出   key = userId1 \t userId1 value = weight \t distance
              context.write(new Text(retList.get(0) + TABLE_DELIMETER  + retList.get(1)),new Text(retList.get(2)
                  + TABLE_DELIMETER + retList.get(3)));
          }
      }
    }
 
  /**
   * 第三阶段Reducer
   * 
   * @date 2016-7-11
   * @author dengxing
   */
  static  class  ThirdStageReducer  extends  Reducer<Text, Text, Text, DoubleWritable> {
      private Text value = new Text();
      protected void reduce(Text key,Iterable<Text> values,Context context)
              throws java.io.IOException, InterruptedException {
          if (null == key.toString() || key.toString().equals("")) {
              return;
          }
          Iterator<Text> text = values.iterator();
          int n = 0;
          double weight = 0;
          double distance = 0;
          while (text.hasNext()) {
              // 由于迭代器中保存的是引用，需要转化为值
              value.set(text.next().toString().getBytes());
              List<String>  retList = Util.split(value.toString(), TABLE_DELIMETER);
              // retList.get(0) = weight,retList.get(1) = distance
              if (n == 0) {
                  distance = Double.valueOf(retList.get(1));
              }
              weight += Double.valueOf(retList.get(0));
              n ++;
          }
          // 输出   key = userId1 \t userId2  value = weight/distance
          context.write(key, new DoubleWritable(weight/distance));
      }
   }

  /**
   * 第四阶段mapper
   * 
   * @date 2016-7-11
   * @author dengxing
   */
  static class FourStageMapper extends Mapper<Object, Text, Text, Text> {
       protected void map(Object key,Text value,Context context)
              throws java.io.IOException, InterruptedException {
          if (null == value.toString() || value.toString().equals("")) {
              return;
          }
          List<String>  retList = Util.split(value.toString(), TABLE_DELIMETER);
          // retList 0=userId1 1 = userId2  2 = score
          //输出  key = userId1  value = userId2 ：  score(weight/distance)
          context.write(new Text(retList.get(0)), new Text(retList.get(1)+COLON+retList.get(2)));
      }
  }
  
  /**
   * 第四阶段Reducer
   * 
   * @date 2016-7-11
   * @author dengxing
   */
  static  class  FourStageReducer  extends  Reducer<Text, Text, Text, Text> {

      protected void reduce(Text key,Iterable<Text> values,Context context)
              throws java.io.IOException, InterruptedException {
          if (null == key.toString() || key.toString().equals("")) {
              return;
          }
          List<String> entries = new ArrayList<String>();
          Iterator<Text> it = values.iterator();
          while (it.hasNext()) {
              String otherUser = it.next().toString();
              entries.add(otherUser);
          }      
          for (String otherUser : entries) {
          // 输出   key = userId1 value = userId2 : socre 
        	  context.write(key, new Text(otherUser));           
          }
          
      }
    }
  
  public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      for(String agr: otherArgs){
    	  System.out.println(agr);  
      }
      if (otherArgs.length != 5){
          System.err.println("Usage: <intput> <output> <output1> <output2> <output3>");
          System.exit(2);
      }
      Job job = Job.getInstance();
      job.setJarByClass(RecommendCosin.class);
      /** 第一阶段job */
      job.setJobName("SimCalculation.stage-1");
      job.setMapperClass(FirstStageMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setNumReduceTasks(2);
      job.setMapOutputValueClass(Text.class);
      job.setReducerClass(FirstStageReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
      job.waitForCompletion(true);

      /**第二阶段job */
      Job job1 = Job.getInstance();
      job1.setJarByClass(RecommendCosin.class);
      job1.setJobName("SimCalculation.stage-2");
      job1.setMapperClass(SecondStageMapper.class);
      job1.setMapOutputKeyClass(Text.class);
      job1.setMapOutputValueClass(Text.class);
      job1.setReducerClass(SecondStageReducer.class);
      job1.setOutputKeyClass(Text.class);
      job1.setNumReduceTasks(2);
      job1.setOutputValueClass(Text.class);
      FileInputFormat.setInputPaths(job1, new Path(otherArgs[1]));
      FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
      job1.waitForCompletion(true);

      /**第三阶段job */
      Job job2 = Job.getInstance();
      job2.setJarByClass(RecommendCosin.class);
      job2.setJobName("SimCalculation.stage-3");
      job2.setMapperClass(ThirdStageMapper.class);
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);
      job2.setReducerClass(ThirdStageReducer.class);
      job2.setOutputKeyClass(Text.class);
      job2.setNumReduceTasks(2);
      job2.setOutputValueClass(DoubleWritable.class);
      FileInputFormat.setInputPaths(job2, new Path(otherArgs[2]));
      FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
      job2.waitForCompletion(true);

      /**第四阶段job */
      Job job3 = Job.getInstance();
      job3.setJarByClass(RecommendCosin.class);
      job3.setJobName("SimCalculation.stage-4");
      job3.setMapperClass(FourStageMapper.class);
      job3.setMapOutputKeyClass(Text.class);
      job3.setMapOutputValueClass(Text.class);
      job3.setReducerClass(FourStageReducer.class);
      job3.setOutputKeyClass(Text.class);
      job3.setNumReduceTasks(2);
      job3.setOutputValueClass(Text.class);
      FileInputFormat.setInputPaths(job3, new Path(otherArgs[3]));
      FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));
      job3.waitForCompletion(true);
   }
}   
