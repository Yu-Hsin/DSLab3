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

    private static int[] mapperStatusPort = null;
    private static String[] mapperBackIPs = null;
    private static int[] mapperBackPort = null;
    private static int[] mapperBackStatusPort = null;

    private static String[] reducerBackIPs = null;
    private static int[] reducerBackPort = null;

    private static int[] reducerStatusPort = null;
    private static int[] reducerBackStatusPort = null;

    private static String mapperClass = null;
    private static String mapperFunc = null;
    private static String reducerClass = null;
    private static String reducerFunc = null;

    private static BufferedWriter results = null;
    private static Object bwLock = new Object();

    private static boolean mapperOK = true;

    private static int lenInOnePart = 0;
    private static String inputFile = null;

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
	inputFile = args[3];

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
	mTask.setJobID();
	mTask.setMapperNum(mapperNum);
	mTask.setReducerNum(reducerNum);
	mTask.setMapperClass(mapperClass);
	mTask.setMapperFunc(mapperFunc);
	mTask.setReducerClass(reducerClass);
	mTask.setReducerFunc(reducerFunc);
	//

	ObjectOutputStream masteroos = null;
	ObjectInputStream masterois = null;
	try {
	    socketTaskRequest = new Socket(masterIP, masterPort);

	    masteroos = new ObjectOutputStream(socketTaskRequest.getOutputStream());
	    masteroos.writeObject(mTask);
	    masteroos.flush();

	    masterois = new ObjectInputStream(socketTaskRequest.getInputStream());
	    Object obj = masterois.readObject();
	    if (obj instanceof MapReduceTask) {
		mTask = (MapReduceTask)obj;
		mapperIPs = mTask.getMapperIP();
		mapperPort = mTask.getMapperPort();
		mapperStatusPort = mTask.getMapperStatusPort();
		mapperBackIPs = mTask.getBackMapperIP();
		mapperBackPort = mTask.getBackMapperPort();
		mapperBackStatusPort = mTask.getBackMapperStatusPort();
		reducerIPs = mTask.getReducerIP();
		reducerPort = mTask.getReducerPort();
		reducerBackIPs = mTask.getBackReducerIP();
		reducerBackPort = mTask.getBackReducerPort();
		reducerStatusPort = mTask.getReducerStatusPort();
		reducerBackStatusPort = mTask.getBackReducerStatusPort();

		for (int i = 0; i < reducerBackIPs.length; i++) System.out.print(mapperBackIPs[i] + " " + mapperBackPort[i]);
		System.out.println();

		if (reducerIPs == null) {
		    System.out.println("System is busy... Try Later...");
		    return;
		}
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
	    Thread[] initialBackInfoThreads = new Thread[mapperNum];
	    for (int i = 0; i < mapperNum; i++) {
		MapReduceTask newTask = new MapReduceTask(mTask);
		newTask.setMachineNum(i);
		initialInfoThreads[i] = new Thread(new InitialInfoThread(mapperIPs[i], mapperPort[i], newTask));
		initialInfoThreads[i].start();

		initialBackInfoThreads[i] = new Thread(new InitialInfoThread(mapperBackIPs[i], mapperBackPort[i], newTask));
		initialBackInfoThreads[i].start();
	    }

	    for (int i = 0; i < mapperNum; i++) initialInfoThreads[i].join();
	    for (int i = 0; i < mapperNum; i++) initialBackInfoThreads[i].join();
	    System.out.println("Initial information all set.");

	    /*
	     *  4. Send the file segments, and jar files to mappers. 
	     */
	    System.out.println("Now transmit the Input file...");
	    SendFileThread mSocketRunnable = new SendFileThread(mapperIPs, mapperPort, mapperBackIPs, mapperBackPort, inputFile);
	    Thread t = new Thread(mSocketRunnable);
	    t.start();
	    t.join();

	    if (!mapperOK) {
		System.out.println("[Master][System Fail] A mapper and its backup mapper both fail...");
		return;
	    }

	    System.out.println("Now transmit the Java file and run Map function...");
	    Thread[] sendJavaThreads = new Thread[mapperIPs.length];
	    for (int i = 0; i < mapperIPs.length; i++) {
		SendJavaThread mJavaSocketRunnable = new SendJavaThread(mapperIPs[i], mapperPort[i], mapperJavaFile, 0);
		mJavaSocketRunnable.backIP = mapperBackIPs[i];
		mJavaSocketRunnable.backPort = mapperBackPort[i];
		sendJavaThreads[i] = new Thread(mJavaSocketRunnable);
		sendJavaThreads[i].start();
	    }

	    for (int i = 0; i < sendJavaThreads.length; i++) sendJavaThreads[i].join();

	    if (!mapperOK) {
		System.out.println("[Master][System Fail] A mapper and its backup mapper both fail...");
		return;
	    }

	    System.out.println("all Mappers finish.");

	} catch (InterruptedException e) {
	    e.printStackTrace();
	}


	/*
	 *  5. Send initial informaiton, java file to reducer
	 */
	try {

	    System.out.println("Send the initial information(reducer number, IP...) to reducers...");
	    Thread[] initialInfoThreads = new Thread[reducerNum];
	    Thread[] initialInfoBackThreads = new Thread[reducerNum];
	    for (int i = 0; i < reducerNum; i++) {
		MapReduceTask newTask = new MapReduceTask(mTask);
		newTask.setMachineNum(i);

		System.out.println("[Reducer][Initial] " + reducerIPs[i] + " " + reducerPort[i] + " / " +
			reducerBackIPs[i] + " " + reducerBackPort[i] + " ; ");

		initialInfoThreads[i] = new Thread(new InitialInfoThread(reducerIPs[i], reducerPort[i], newTask));
		initialInfoThreads[i].start();

		initialInfoBackThreads[i] = new Thread(new InitialInfoThread(reducerBackIPs[i], reducerBackPort[i], newTask));
		initialInfoBackThreads[i].start();
	    }

	    for (int i = 0; i < reducerNum; i++) initialInfoThreads[i].join();
	    for (int i = 0; i < reducerNum; i++) initialInfoBackThreads[i].join();
	    System.out.println("Initial information for reducers all set.");


	    System.out.println("Now run Reduce function...");
	    Thread[] sendToReducers = new Thread[reducerNum];
	    Thread[] sendToBackReducers = new Thread[reducerNum];
	    for (int i = 0; i < reducerNum; i++) {
		sendToReducers[i] = new Thread(new SendJavaThread(reducerIPs[i], reducerPort[i], reducerJavaFile, reducerStatusPort[i]));
		sendToReducers[i].start();

		sendToBackReducers[i] = new Thread(new SendJavaThread(reducerBackIPs[i], reducerBackPort[i], reducerJavaFile, reducerBackStatusPort[i]));
		sendToBackReducers[i].start();
	    }

	    for (int i = 0; i < reducerNum; i++) sendToReducers[i].join();
	    for (int i = 0; i < reducerNum; i++) sendToBackReducers[i].join();

	    /* Result File */
	    results = new BufferedWriter(new FileWriter("results.txt"));

	    for (int i = 0; i < reducerNum; i++) {
		sendToReducers[i] = new Thread(new FinalResultThread(reducerIPs[i], reducerPort[i], 
			reducerStatusPort[i], reducerBackIPs[i], reducerBackPort[i], reducerBackStatusPort[i]));
		sendToReducers[i].start();
	    }
	    for (int i = 0; i < reducerNum; i++) sendToReducers[i].join();

	    results.flush();
	    results.close();
	    System.out.println("All Reducers finish.");
	} catch (InterruptedException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}



	/*
	 *  5. Tell the System Master the finish of the task,
	 *     so that the System Master can release the resources.
	 */
	try {
	    masteroos.writeObject(mTask);
	    masteroos.flush();
	    masteroos.close();
	    socketTaskRequest.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

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

	private String[] backIPs;
	private int[] backPorts;
	private int offset = 0;
	private int preoffset = 0;

	public SendFileThread(String[] s, int[] p, String[] bs, int[] bp, String fname) {
	    slaves = s;
	    port = p;
	    backIPs = bs;
	    backPorts = bp;
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

		lenInOnePart = (int) Math.ceil((double) len / (double) mapperNum);

		/* Send line */
		br = new BufferedReader(new FileReader(filename));

		for (int slaveIdx = 0; slaveIdx < slaves.length; slaveIdx++) {
		    try {
			preoffset = offset;
			//System.out.println("Now:  " + slaveIdx + " " + preoffset);

			mSocket = new Socket(slaves[slaveIdx], port[slaveIdx]);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(mSocket.getOutputStream()));
			bw.write("1\n");
			bw.flush();
			bw.close();
			mSocket.close();

			//System.out.println("socket:  " + slaves[slaveIdx] + "  " + port);

			mSocket = new Socket(slaves[slaveIdx], port[slaveIdx]);
			ObjectOutputStream out = new ObjectOutputStream(
				mSocket.getOutputStream());

			int idx = 0;

			out.write((slaveIdx + "\n").getBytes());
			out.flush();

			while (idx < lenInOnePart && (s = br.readLine()) != null) {
			    //System.out.println(s + " " + idx);
			    offset++;
			    byte[] buf = (s + "\n").getBytes();

			    if (s.length() <= 1024) {
				out.write(buf, 0, buf.length);
				out.flush();
			    } else {
				for (int j = 0; j < buf.length; j += 1024) {
				    out.write(buf, j,
					    (j + 1024 >= buf.length) ? buf.length - j : 1024);
				    out.flush();
				}
			    }

			    idx++;
			}

			mSocket.close();
		    } catch(Exception e) {
			System.out.println("[Master][Mapper Fail] Try its back-up node...");
			mSocket.close();
			sendToBack(slaveIdx, br);
		    }

		}
		br.close();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}

	private void sendToBack(int slaveIdx, BufferedReader br) {
	    try {
		mSocket = new Socket(backIPs[slaveIdx], backPorts[slaveIdx]);
		mapperIPs[slaveIdx] = backIPs[slaveIdx];
		mapperPort[slaveIdx] = backPorts[slaveIdx];
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(mSocket.getOutputStream()));
		bw.write("1\n");
		bw.flush();
		bw.close();
		mSocket.close();

		mSocket = new Socket(backIPs[slaveIdx], backPorts[slaveIdx]);
		System.out.println("[Mapper Backup]socket:  " + backIPs[slaveIdx] + "  " + backPorts[slaveIdx]);

		br = new BufferedReader(new FileReader(filename));
		for (int i = 0; i < preoffset; i++) br.readLine();

		ObjectOutputStream out = new ObjectOutputStream(
			mSocket.getOutputStream());

		int idx = 0;

		out.write((slaveIdx + "\n").getBytes());
		out.flush();

		String s = null;
		while (idx < lenInOnePart && (s = br.readLine()) != null) {
		    preoffset++;
		    byte[] buf = (s + "\n").getBytes();

		    if (s.length() <= 1024) {
			out.write(buf, 0, buf.length);
			out.flush();
		    } else {
			for (int j = 0; j < buf.length; j += 1024) {
			    out.write(buf, j,
				    (j + 1024 >= buf.length) ? buf.length - j : 1024);
			    out.flush();
			}
		    }

		    idx++;
		}

		offset = preoffset;
		mSocket.close();
	    } catch(Exception e) {
		mapperOK = false;
	    }
	}

    }

    public static class FinalResultThread implements Runnable {
	private Socket mSocket = null;
	private String firstIP = null;
	private int firstPort;
	private int firstStatusPort;

	private String backIP = null;
	private int backPort;
	private int backStatusPort;

	public FinalResultThread(String s, int p, int sp, String bs, int bp, int bsp) {
	    firstIP = s;
	    firstPort = p;
	    firstStatusPort = sp;

	    backIP = bs;
	    backPort = bp;
	    backStatusPort = bsp;
	}

	@Override
	public void run() {
	    try {
		System.out.println("[Master] Keep polling reducer until it finishes...");
		while(true) {
		    Thread.sleep(1000);
		    mSocket = new Socket(firstIP, firstStatusPort);
		    DataInputStream din = new DataInputStream(mSocket.getInputStream());

		    String s = din.readUTF();
		    if (s.equals("idle")) {
			Socket backSocket = new Socket(backIP, backPort);

			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(backSocket.getOutputStream()));

			bw.write("0\n");
			bw.flush();
			bw.close();
			backSocket.close();
			break;
		    }
		}
		System.out.println("A reducer finishs...");
		mSocket.close();

		/* Get the result file */
		mSocket = new Socket(firstIP, firstPort);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(mSocket.getOutputStream()));
		bw.write("1\n");
		bw.flush();

		mSocket.shutdownOutput();

		System.out.println("[Master] Reqeust the results from reducers  " + firstIP + " " + firstPort);
		BufferedReader sbr = new BufferedReader(new InputStreamReader(mSocket.getInputStream()));

		String str = "";
		while((str = sbr.readLine()) != null) {
		    //System.out.println("[Master][Get String]  " + str);
		    synchronized (bwLock) {
			results.write(str + "\n");
		    }
		}
		sbr.close();
		mSocket.close();

	    } catch (Exception e) {
		System.out.println("[Master][Reducer Fail]One Reducer failed... Call the back-up reducer...");
		if (!askBackReducer()) System.out.println("[Master][System Fail]Reducer and its backup both fail...");
	    }
	}

	private boolean askBackReducer() {
	    try {
		while(true) {
		    mSocket.close();
		    mSocket = new Socket(backIP, backStatusPort);
		    DataInputStream din = new DataInputStream(mSocket.getInputStream());
		    System.out.println("send status request");

		    String s = din.readUTF();
		    if (s.equals("idle")) break;
		}
		System.out.println("A backup reducer finishs...");
		mSocket.close();

		/* Get the result file */
		mSocket = new Socket(backIP, backPort);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(mSocket.getOutputStream()));
		bw.write("1\n");
		bw.flush();

		mSocket.shutdownOutput();

		System.out.println("[Master][Backup] Reqeust the results from back-up reducers  " + firstIP + " " + firstPort);
		BufferedReader sbr = new BufferedReader(new InputStreamReader(mSocket.getInputStream()));

		String str = "";
		while((str = sbr.readLine()) != null) {
		    //System.out.println("[Master][Get String]  " + str);
		    synchronized (bwLock) {
			results.write(str + "\n");
		    }
		}
		sbr.close();
		mSocket.close();
		return true;
	    } catch (Exception e) {
		e.printStackTrace();
		return false;
	    }
	}
    }

    public static class SendJavaThread implements Runnable{
	private Socket mSocket = null;
	private String filename;
	private String slave;
	private int port;

	public String backIP = null;
	public int backPort;

	private int statusport = 0;

	public SendJavaThread(String s, int p, String fname, int statusPort) {
	    slave = s;
	    port = p;
	    filename = fname;
	    statusport = statusPort;
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

		if (statusport == 0) {
		    mSocket.shutdownOutput();
		    System.out.println("[Master][Mapper] Wait for finish signal from the mapper.");

		    /* Wait for finish signal */
		    String rtnMsg = din.readUTF();
		    System.out.println("[Master] mapper returns msg: " + rtnMsg);
		}
		else {
		    Socket pinSocket = new Socket(slave, port);
		    System.out.println("[Master][Reducer] Start reducer function...");
		    pinSocket.close();
		}

		bis.close();
		mSocket.close();

		if (statusport == 0) {
		    mSocket = new Socket(backIP, backPort);
		    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(mSocket.getOutputStream()));
		    bw.write("0\n");
		    bw.flush();
		    bw.close();
		    mSocket.close();
		}
	    }
	    catch (Exception e) {
		if (statusport == 0) {
		    try {
			System.out.println("[Master][Java Download] Node Fail " + slave + ":" + port);
			mSocket.close();

			int curOffset = 0;
			int iidx = mapperIPs.length;
			for (int i = 0; i < mapperIPs.length; i++) {
			    if (mapperIPs[i].equals(slave)) {
				iidx = i;
				break;
			    }
			    else curOffset += lenInOnePart;
			}

			System.out.println("[Master][Java Download] Check if it is required to resend input file");
			if (slave != backIP) {
			    BufferedReader br = new BufferedReader(new FileReader(inputFile));
			    for (int i = 0; i < curOffset; i++) br.readLine();
			    //=======================================
			    try {
				
				mSocket = new Socket(backIP, backPort);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(mSocket.getOutputStream()));
				bw.write("1\n");
				bw.flush();
				bw.close();
				mSocket.close();

				System.out.println("[Master][Resend] input file to -> socket:  " + backIP + ":" + backPort);

				mSocket = new Socket(backIP, backPort);
				ObjectOutputStream out = new ObjectOutputStream(
					mSocket.getOutputStream());

				int idx = 0;

				out.write((iidx + "\n").getBytes());
				out.flush();

				String s = "";
				while (idx < lenInOnePart && (s = br.readLine()) != null) {
				    
				    byte[] buf = (s + "\n").getBytes();

				    if (s.length() <= 1024) {
					out.write(buf, 0, buf.length);
					out.flush();
				    } else {
					for (int j = 0; j < buf.length; j += 1024) {
					    out.write(buf, j,
						    (j + 1024 >= buf.length) ? buf.length - j : 1024);
					    out.flush();
					}
				    }

				    idx++;
				}

				mSocket.close();
			    } catch(Exception e1) {
				mSocket.close();
				mapperOK = false;
			    }
			    br.close();
			    //=================================
			}

			System.out.println("[Mapper] Start to transmit java file to back-up mapper...");
			mSocket = new Socket(backIP, backPort);
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

			if (statusport == 0) {
			    mSocket.shutdownOutput();
			    System.out.println("Done.");

			    /* Wait for finish signal */
			    String rtnMsg = din.readUTF();
			    System.out.println("return msg: " + rtnMsg + " " + rtnMsg.equals("OK"));
			}
			else {
			    Socket pinSocket = new Socket(slave, port);
			    System.out.println("send ok");
			    pinSocket.close();
			}

			bis.close();
			mSocket.close();
			System.out.println("[Mapper] Finish to transmit java file to back-up node.");
		    } catch (IOException e1) {
			System.out.println("[Master][System Fail] During java file transmission, original node and back-up node both fail.");
			mapperOK = false;
		    }

		}

		else e.printStackTrace();
	    }
	}
    }
}
