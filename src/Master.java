import java.io.*;
import java.net.*;

public class Master {

    private static String masterIP = null;
    private static int masterPort;

    private static String[] mapperIPs = null;
    private static String[] reducerIPs = null;
    private static int mapperNum;
    private static int reducerNum;
    private static int mapperPort;
    private static int reducerPort;

    private static String mapperClass = null;
    private static String mapperFunc = null;
    private static String reducerClass = null;
    private static String reducerFunc = null;

    public static void main(String[] args) {
	/*
	 *  Check the standard input,
	 *  Ex: java Master <config file> <jar file>
	 */
	if (args.length != 3) {
	    System.out.println("Error input: java Master <config file> <jar file> <input file>");
	    return;
	}
	String configFile = args[0];
	String jarFile = args[1];
	String inputFile = args[2];

	/*
	 *  1. Read in the configuration file
	 */
	try {
	    /* Task Configuration */
	    BufferedReader configbr = new BufferedReader(new FileReader(configFile));

	    String[] strs = configbr.readLine().split("\\s+");
	    mapperNum = Integer.valueOf(strs[1]);

	    strs = configbr.readLine().split("\\s+");
	    reducerNum = Integer.valueOf(strs[1]);

	    mapperClass = configbr.readLine().split("\\s+")[1];
	    mapperFunc = configbr.readLine().split("\\s+")[1];
	    reducerClass = configbr.readLine().split("\\s+")[1];
	    reducerFunc = configbr.readLine().split("\\s+")[1];

	    /* System Configuration */
	    configbr.close();
	    configbr = new BufferedReader(new FileReader("master"));
	    configbr.readLine();
	    strs = configbr.readLine().split("\\s+");
	    masterIP = strs[0];
	    masterPort = Integer.valueOf(strs[1]);

	} catch (FileNotFoundException e1) {
	    e1.printStackTrace();
	} catch (IOException e) {
	    System.out.println("Error config file: wrong format!!!");
	    return;
	}

	/*
	 *  2. Send Map Reduce Task request to the system master.
	 */
	try {
	    MapReduceTask mTask = new MapReduceTask();
	    mTask.setMapperNum(mapperNum);
	    mTask.setReducerNum(reducerNum);
	    Socket socketTaskRequest = new Socket(masterIP, masterPort);
	    
	    ObjectOutputStream oos = new ObjectOutputStream(socketTaskRequest.getOutputStream());
	    oos.writeObject(mTask);
	    oos.flush();
	    
	    ObjectInputStream ois = new ObjectInputStream(socketTaskRequest.getInputStream());
	    Object obj = ois.readObject();
	    if (obj instanceof MapReduceTask) {
		mTask = (MapReduceTask)obj;
		mapperIPs = mTask.getMapperIP();
		mapperPort = mTask.getMapperPort();
		reducerIPs = mTask.getReducerIP();
		reducerPort = mTask.getReducerPort();
		
		for (String s : reducerIPs) System.out.print(s + " ");
		System.out.println();
		
	    }
	    else {
		System.out.println("Wrong Task Request...");
		return;
	    }
	} catch (UnknownHostException e1) {
	    e1.printStackTrace();
	} catch (IOException e1) {
	    e1.printStackTrace();
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	}

	
	/*
	 *  3. Send the file segments, and jar files to mappers. 
	 *     Then notify the reducers to start after all mappers complete.
	 */
	try {
	    SendFileThread mSocketRunnable = new SendFileThread(mapperIPs, mapperPort, inputFile);
	    Thread t = new Thread(mSocketRunnable);
	    t.start();
	    t.join();

	    Thread[] sendJavaThreads = new Thread[mapperIPs.length];
	    for (int i = 0; i < mapperIPs.length; i++) {
		SendJarThread mJarSocketRunnable = new SendJarThread(mapperIPs[i], mapperPort, jarFile);
		sendJavaThreads[i] = new Thread(mJarSocketRunnable);
		sendJavaThreads[i].start();
	    }

	    for (int i = 0; i < sendJavaThreads.length; i++) sendJavaThreads[i].join();
	    System.out.println("all threads finish");


	    SendReduceStartThread sendToReducer = new SendReduceStartThread(reducerIPs, reducerPort);
	    t = new Thread(sendToReducer);
	    t.start();

	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
	
	/*
	 *  4. Tell the System Master the finish of the task,
	 *     so that the System Master can release the resources.
	 */
	
	
	
    }

    public static class SendReduceStartThread implements Runnable {
	private Socket mSocket = null;
	private String[] reducers;
	private int port;

	public SendReduceStartThread(String[] s, int p) {
	    reducers = s;
	    port = p;
	}

	@Override
	public void run() {
	    try {
		for (int i = 0; i < reducers.length; i++) {
		    //System.out.println("reducer:  " + reducers[i] + " " + port);
		    mSocket = new Socket(reducers[i], port);
		    DataOutputStream dout = new DataOutputStream(mSocket.getOutputStream());

		    dout.writeUTF("OK");
		    dout.flush();
		    dout.close();
		}

		mSocket.close();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}

    }

    public static class SendFileThread implements Runnable {
	private Socket mSocket = null;
	private String filename;
	private String[] slaves;
	private int port;

	public SendFileThread(String[] s, int p, String fname) {
	    slaves = s;
	    port = p;
	    filename = fname;
	}

	@Override
	public void run() {

	    try {

		BufferedReader br = new BufferedReader(new FileReader(filename));
		String s = null;

		/* Count the total Line */
		int len = 0;
		while ((s = br.readLine()) != null)
		    len++;
		br.close();

		int lenInOnePart = (int) Math.ceil((double) len / (double) mapperNum);

		/* Send line */
		br = new BufferedReader(new FileReader(filename));

		for (int slaveIdx = 0; slaveIdx < slaves.length; slaveIdx++) {
		    //System.out.println(slaves[slaveIdx] + " " + port + "   ========");
		    mSocket = new Socket(slaves[slaveIdx], port);

		    System.out.println("socket:  " + slaves[slaveIdx] + "  " + port);
		    
		    
		    ObjectOutputStream out = new ObjectOutputStream(
			    mSocket.getOutputStream());

		    int idx = 0;

		    out.write((slaveIdx + "\n").getBytes());
		    out.flush();

		    while (idx < lenInOnePart && (s = br.readLine()) != null) {
			//System.out.println(s + " " + idx);
			byte[] buf = (s + "\n").getBytes();

			if (s.length() <= 1024) {
			    out.write(buf, 0, buf.length);
			    out.flush();
			} else {
			    for (int j = 0; j < buf.length; j += 1024) {

				out.write(buf, j,
					(j + 1024 >= buf.length) ? buf.length
						- j : 1024);
				out.flush();
			    }
			}

			idx++;
		    }

		    mSocket.close();
		}

		br.close();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}

    }



    public static class SendJarThread implements Runnable{
	private Socket mSocket = null;
	private String filename;
	private String slave;
	private int port;

	public SendJarThread(String s, int p, String fname) {
	    slave = s;
	    port = p;
	    filename = fname;
	}

	@Override
	public void run() {
	    try {
		mSocket = new Socket(slave, port);
		DataOutputStream dout = new DataOutputStream(mSocket.getOutputStream());
		DataInputStream din = new DataInputStream(mSocket.getInputStream());


		File jarFile = new File(filename);
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(jarFile));

		dout.writeUTF(filename);

		byte[] buffer = new byte[1024];
		int count = 0;
		while((count = bis.read(buffer)) > 0) {
		    dout.write(buffer, 0, count);
		    dout.flush();
		}
		mSocket.shutdownOutput();
		System.out.println("Done.");

		/* Wait for finish signal */
		String rtnMsg = din.readUTF();
		System.out.println("return msg: " + rtnMsg + " " + rtnMsg.equals("OK"));

		bis.close();
		mSocket.close();
	    }
	    catch (Exception e) {
		e.printStackTrace();
	    }
	}



    }
}
