package com.zack.stat;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.*;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Collections;

public class MRStat
{

	SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

	String localSide;
	String []fileNameList;
	DataInputStream in;

	int total;
	int total_without_local;
	HashMap<String, Integer> receiveSize;

	LinkedList<DuringTime> duringTimeTable;
	public MRStat(String localSide, String[] fileNameList)
	{
		this.localSide = localSide;
		this.fileNameList = new String[fileNameList.length];
		for(int i=0;i<fileNameList.length;i++)
			this.fileNameList[i] = new String(fileNameList[i]);
		total = 0;
		total_without_local = 0;
		receiveSize = new HashMap<String, Integer>();
		receiveSize.put("master", new Integer(0));
		receiveSize.put("slave1", new Integer(0));
		receiveSize.put("slave2", new Integer(0));
		receiveSize.put("slave3", new Integer(0));

		duringTimeTable = new LinkedList<DuringTime>();
	}

	public void doIt()
	{
		for(String fileName : fileNameList)
		{
			try
			{
				in = new DataInputStream(new FileInputStream(fileName));
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String line;
				HashSet<String> remoteStart = new HashSet<String>();
				while((line = br.readLine()) != null)
				{
					int index = line.indexOf("### ");
					if(index <0)
						continue;
					String s = line.substring(index+4);
					if(s.startsWith("Start"))
					{
						s = s.substring(5);
						StringTokenizer strtok = new StringTokenizer(s, " ");
						String remoteSide = strtok.nextToken();
						String time = strtok.nextToken();

						//if we have found the remoteSide in remoteStart, the log file has error
						if(remoteStart.contains(remoteSide))
							throw new Exception(fileName + " in start, remoteStart has contained element: " + remoteSide);
						remoteStart.add(remoteSide);

						DuringTime dt = new DuringTime(remoteSide, time);
						duringTimeTable.add(dt);
						Collections.sort(duringTimeTable);
					}
					else if(s.startsWith("End"))
					{
						s = s.substring(3);
						StringTokenizer strtok = new StringTokenizer(s, " ");
						String remoteSide = strtok.nextToken();
						String time = strtok.nextToken();
						String size = strtok.nextToken();
						for(DuringTime dt : duringTimeTable)
						{
							if(remoteSide.equals(dt.getRemote()) && !dt.isEnd())
							{
								//if we have not found remoteSide in remoteStart, the log file has error
								if(!remoteStart.contains(remoteSide))
									throw new Exception(fileName + " in end, remoteStart does not contain element: " + remoteSide);
								remoteStart.remove(remoteSide);

								dt.setEnd(time);
								int sizeInt = Integer.parseInt(size);
								total += sizeInt;
								if(!remoteSide.equals(localSide))
									total_without_local += sizeInt;
								int remoteSideSize = receiveSize.get(remoteSide).intValue();
								remoteSideSize += sizeInt;
								receiveSize.put(remoteSide, remoteSideSize);
							}
						}
					}
				}
				in.close();
				if(!remoteStart.isEmpty())
				{
					//we need to check the duringTimeTable if there are some entry is start but not end
					//this occur when there is a cleanup task
					LinkedList<Integer> indexToRemove = new LinkedList<Integer>();
					for(DuringTime dt : duringTimeTable)
					{
						if(!dt.isEnd())
							indexToRemove.add(duringTimeTable.indexOf(dt));
					}
					int c = 0;
					for(Integer i : indexToRemove)
					{
						duringTimeTable.remove(i.intValue() - c);
						c++;
					}
				}
			} catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	public void dumpFile(String fn)
	{
		try
		{
			FileWriter fstream = new FileWriter(fn);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(this.toString());
			out.close();
		} catch(Exception e)
		{
			System.out.println("Write to file " + fn + " error. " + e.getMessage());
		}
	}
	public String toString()
	{
		StringBuffer sb = new StringBuffer(localSide);
		sb.append(":\n");
		sb.append("\ttotal: " + (total/(double)1000000) + "\n");
		sb.append("\ttotal_no_local: " + (total_without_local/(double)1000000) + "\n");
		sb.append("\n");
		for(String key : receiveSize.keySet())
		{
			Integer size = receiveSize.get(key);
			sb.append("\tfrom " + key + ":\n");
			sb.append("\t\t" + (size/(double)1000000) + "\n");
		}
		sb.append("\n\n");

		for(DuringTime dt : duringTimeTable)
		{
			if(dt.isEnd() && !dt.toString().equals(""))
				sb.append("\t" + dt.toString() + "\n");
		}
		sb.append("\n");

		return sb.toString();
	}

	private class DuringTime implements Comparable<DuringTime>
	{
		String start;
		String end;
		String remote;
		Date date;
		boolean _end;
		public DuringTime(String remote, String start)
		{
			this.remote = remote;
			this.start = start;
			try
			{
				date = sdf.parse(start);
			} catch(Exception e)
			{
				System.out.println("Create DuringTime error. " + e.getMessage());
			}
			_end = false;
		}
		public String getRemote()
		{
			return remote;
		}
		public boolean isEnd()
		{
			return _end;
		}
		public void setEnd(String end)
		{
			if(_end)
				return;
			_end = true;
			this.end = end;
		}
		@Override
		public int compareTo(DuringTime d)
		{
			return date.compareTo(d.date);
		}

		@Override
		public boolean equals(Object obj)
		{
			if(this == obj)
				return true;
			if(obj == null || this == null)
				return obj == this;

			if(getClass() != obj.getClass())
				return false;
			DuringTime other = (DuringTime)obj;
			boolean is_start_eq = false;
			boolean is_end_eq = false;
			boolean is_remote_eq = false;
			if(start == null)
			{
				if(other.start != null)
					return false;
				is_start_eq = true;
			}
			else
				is_start_eq = start.equals(other.start);

			if(end == null)
			{
				if(other.end != null)
					return false;
				is_end_eq = true;
			}
			else
				is_end_eq = end.equals(other.end);
			if(remote == null)
			{
				if(other.remote != null)
					return false;
				is_remote_eq = true;
			}
			else
				is_remote_eq = remote.equals(other.remote);

			return (is_start_eq && is_end_eq && is_remote_eq);
		}

		@Override
		public String toString()
		{
			if(remote.equals(localSide))
				return "";
			return start + " - " + end + " : " + remote;
		}
	}
	public static void main(String[] args)
	{
		if(args.length <3)
		{
			System.out.println("Usage: localSide OutputFile filenameList...");
			return;
		}

		String localSide = args[0];
		String outputFile = args[1];
		int listLen = args.length - 2;
		String[] filenameList = new String[listLen];
		for(int i=0;i<listLen;i++)
			filenameList[i] = new String(args[i + 2]);
		MRStat t = new MRStat(localSide, filenameList);
		t.doIt();
		t.dumpFile(outputFile);
	}
}
