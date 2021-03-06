package com.joyveb.test.cassandra;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.joyveb.test.cassandra.dao.SimGameInfoDAO;
import com.joyveb.test.cassandra.domain.SimGameInfo;

public class CassBatchPutPresure {
	private static SimGameInfoDAO dao;
	private static ExecutorService executorService;
	private static AtomicLong count = new AtomicLong(0);
	private static AtomicLong failedCount = new AtomicLong(0);
	private static boolean STOP = false;
	private static long RUN_TIME = 3 * 60 * 60 * 1000;//
	private static long sleeptime = 60000;
	private static int poolsize = 30;

	public static void init() {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"cassandra-presure.xml");
		dao = (SimGameInfoDAO) context.getBean("simGameInfoDAO");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		init();
		executorService = new ThreadPoolExecutor(poolsize, poolsize, 10,
				TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
		long start = System.currentTimeMillis();
		for (int i = 0; i < poolsize; i++) {
			executorService.execute(new Presure());
		}
		long end = System.currentTimeMillis();
		long elapseTime = end - start;
		while (elapseTime < RUN_TIME) {
			try {
				Thread.sleep(sleeptime);
				elapseTime += sleeptime;
				long tps = count.get() * 1000 / elapseTime;
				System.out.println("elaspe time : " + elapseTime
						+ "ms, count is " + count.get() + " TPS is : " + tps);
				end = System.currentTimeMillis();
				elapseTime = end - start;
				record(elapseTime, tps);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		STOP = true;
		executorService.shutdown();
		System.out.println("presure is stop. count is " + count.get()
				+ ", TPS is : " + (count.get() * 1000 / RUN_TIME)
				+ ", failed is " + failedCount);
	}

	private static File file = new File("tps-" + poolsize);
	private static FileWriter fw = null;

	public static void record(long time, long tps) {
		try {
			fw = new FileWriter(file, true);
			fw.append(time + "	" + tps + "\n");
			fw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	static class Presure implements Runnable {
		@Override
		public void run() {
			while (!STOP) {
				try {
					List<SimGameInfo> list = new ArrayList<SimGameInfo>();
					for (int i = 0; i < 10; i++) {
						SimGameInfo info = new SimGameInfo();
						info.setKey(IDGenerator.nextID());
						info.setGamename("SLTO");
						info.setLtype("SLTO");
						info.setPlayname("SLTO");
						info.setGamename("SLTO");
						list.add(info);
					}
					dao.batchInsert(list);
					count.incrementAndGet();
				} catch (Exception e) {
					e.printStackTrace();
					failedCount.incrementAndGet();
				}
			}
		}
	}
}
