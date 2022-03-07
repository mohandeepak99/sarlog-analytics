package com.mohan.sar_log_analysis;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
public class Sar_log_analysisMapper extends Mapper<LongWritable,Text,Text,FloatWritable> {
	private FloatWritable percentVal = new FloatWritable();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
//		HostName	TimeStamp			    				CPU        %user       %nice    %system      %iowait     %steal   %idle
//		phddn001        240613,20:44        Average:        all        4.05        0        10.17        0.02        0        85.76
		
		String valueTokens[] = value.toString().split(" ");
		String hostName = valueTokens[0];
		String date = "";
		String timestamp = "";
		for (int cnt = 1; cnt < valueTokens.length; cnt++)
		{
			if (valueTokens[cnt].length() > 0)
			{
				timestamp = valueTokens[cnt];
				break;
			}
		}
		try
		{
			date = timestamp.split(",")[0];
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		float cpuPercent = 100.0f - Float.parseFloat(valueTokens[valueTokens.length - 1]);
		
		percentVal.set(cpuPercent);
		
		context.write(new Text(hostName + "\t" + date), percentVal);
	}
}