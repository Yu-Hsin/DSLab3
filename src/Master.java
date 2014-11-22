import java.io.*;
import java.net.*;

public class Master {

    private static String masterIP = null;
    private static int masterPort;
    private static Socket socketTaskRequest = null;

    private static String[] mapperIPs = null;
    private static String[] reducerIPs = null;
    private static int mapperNum;
    private static int reducerNum;
    private static int[] mapperPort = null;
    private static int[] reducerPort = null;

    private static String mapperClass = null;
    private static String mapperFunc = null;
    private static String reducerClass = null;
    private static String reducerFunc = null;

    public static void main(String[] args) {
	/*
	 *  Check the standard input,
	 *  Ex: java Master <config file> <mapper java file> <reducer java file> <input file>"
	 */
	if (args.length != 4) {
	    System.out.println("Error input: java Master <config file> <mapper java file> <reducer java file> <input file>");
	    return;
	}
	String configFile = args[0];
	String mapperJavaFile = args[1];
	String reducerJavaFile = args[2];
	String inputFile = args[3];

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
	// Here initializes the information of a mapreduce task.
	MapReduceTask mTask = new MapReduceTask();
	mTask.setMapperNum(mapperNum);
	mTask.setReducerNum(reducerNum);
	mTask.setMapperClass(mapperClass);
	mTask.setMapperFunc(mapperFunc);
	mTask.setReducerClass(reducerClass);
	mTask.setReducerFunc(reducerFunc);
	//
	
	try {
	    socketTaskRequest = new Socket(masterIP, masterPort);

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
	 *  3. Send the initial information to the mapper,
	 *     which is contained in the MapReduceTask Object.
	 */
	try {
	    System.out.println("Send the initial information(reducer number, IP...) to mappers...");
	    Thread[] initialInfoThreads = new Thread[mapperNum];
	    for (int i = 0; i < mapperNum; i++) {
		initialInfoThreads[i] = new Thread(new InitialInfoThread(mapperIPs[i], mapperPort[i], mTask));
		initialInfoThreads[i].start();
	    }
	    
	    for (int i = 0; i < mapperNum; i++) initialInfoThreads[i].join();
	    System.out.println("Initial information all set.");
	    
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
	
	/*
	 *  4. Send the file segments, and jar files to mappers. 
	 *     Then notify the reducers to start after all mappers complete.
	 */
	try {
	    System.out.println("Now transmit the Input file...");
	    SendFileThread mSocketRunnable = new SendFileThread(mapperIPs, mapperPort, inputFile);
	    Thread t = new Thread(mSocketRunnable);
	    t.start();
	    t.join();

	    System.out.println("Now transmit the Jar file and run Map function...");
	    Thread[] sendJavaThreads = new Thread[mapperIPs.length];
	    for (int i = 0; i < mapperIPs.length; i++) {
		SendJavaThread mJarSocketRunnable = new SendJavaThread(mapperIPs[i], mapperPort[i], mapperJavaFile);
		sendJavaThreads[i] = new Thread(mJarSocketRunnable);
		sendJavaThreads[i].start();
	    }

	    for (int i = 0; i < sendJavaThreads.length; i++) sendJavaThreads[i].join();
	    System.out.println("all Mappers finish.");

	    System.out.println("Now run Reduce function...");
	    Thread[] sendToReducers = new Thread[reducerNum];
	    for (int i = 0; i < reducerNum; i++) {
		sendToReducers[i] = new Thread(new SendJavaThread(reducerIPs[i], reducerPort[i], reducerJavaFile));
		sendToReducers[i].start();
	    }

	    for (int i = 0; i < reducerNum; i++) sendToReducers[i].join();
	    System.out.println("All Reducers finish.");

	} catch (InterruptedException e) {
	    e.printStackTrace();
	}

	/*
	 *  5. Tell the System Master the finish of the task,
	 *     so that the System Master can release the resources.
	 */
	try {
	    ObjectOutputStream oos = new ObjectOutputStream(socketTaskRequest.getOutputStream());
	    oos.writeObject(mTask);
	    oos.flush();
	    oos.close();
	    socketTaskRequest.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
/*
    public static class SendReduceStartThread implements Runnable {
	private Socket mSocket = null;
	private String reducerIP = null;
	private int reducerPort;
	
	public SendReduceStartThread(String ip, int port) {
	    reducerIP = ip;
	    reducerPort = port;
	}
	
	@Override
	public void run() {
	    try {
		mSocket = new Socket(reducerIP, reducerPort);
		
		DataOutputStream dout = new DataOutputStream(mSocket.getOutputStream());
		dout.writeUTF("OK");
		dout.flush();
		mSocket.shutdownOutput();
		
		DataInputStream din = new DataInputStream(mSocket.getInputStream());
		String rtnVal = din.readUTF();
		if (rtnVal.equals("OK")) return;
		
	    } catch (UnknownHostException e) {
		e.printStackTrace();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}
    }
   */ 
    public static class InitialInfoThread implements Runnable {
	private Socket mSocket = null;
	private String slaveIP = null;
	private int slavePort;
	private MapReduceTask mTask = null;
	
	public InitialInfoThread(String ip, int p, MapReduceTask info) {
	    slaveIP = ip;
	    slavePort = p;
	    mTask = info;
	}
	
	@Override
	public void run() {
	    try {
		mSocket = new Socket(slaveIP, slavePort);
		
		ObjectOutputStream oos = new ObjectOutputStream(mSocket.getOutputStream());
		oos.writeObject(mTask);
		oos.flush();
		oos.close();
		mSocket.close();
	    } catch (UnknownHostException e) {
		e.printStackTrace();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}
	
    }
    
    public static class SendFileThread implements Runnable {
	private Socket mSocket = null;
	private String filename;
	private String[] slaves;
	private int[] port;

	public SendFileThread(String[] s, int[] p, String fname) {
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
		    mSocket = new Socket(slaves[slaveIdx], port[slaveIdx]);

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



    public static class SendJavaThread implements Runnable{
	private Socket mSocket = null;
	private String filename;
	private String slave;
	private int port;

	public SendJavaThread(String s, int p, String fname) {
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
