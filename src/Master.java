import java.io.*;
import java.net.*;

public class Master {

    private static final int SlaveNode = 2;

    //private static String[] ips = {"ghc53.ghc.andrew.cmu.edu"};

    private static String[] mapperIPs = { "ghc54.ghc.andrew.cmu.edu",
    "ghc53.ghc.andrew.cmu.edu" };

    private static String[] reducerIPs = {};
    
    public static void main(String[] args) {

	try {
	    int mapperPort = Integer.parseInt(args[0]);
	    int reducerPort = Integer.parseInt(args[1]);

	    SendFileThread mSocketRunnable = new SendFileThread(mapperIPs, mapperPort, args[2]);
	    Thread t = new Thread(mSocketRunnable);
	    t.start();
	    t.join();

	    Thread[] sendJavaThreads = new Thread[mapperIPs.length];
	    for (int i = 0; i < mapperIPs.length; i++) {
		SendJarThread mJarSocketRunnable = new SendJarThread(mapperIPs[i], mapperPort, args[3]);
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
    }

    /*
    public static class SendInitialInfoThread implements Runnable {
	private Socket mSocket = null;
	private String[] slaves;
	private int port;
	private int reducerNum;

	public SendInitialInfoThread(String[] s, int p, int n) {
	    slaves = s;
	    port = p;
	    reducerNum = n;
	}

	@Override
	public void run() {

	}
    }
     */

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

		int lenInOnePart = (int) Math.ceil((double) len
			/ (double) SlaveNode);

		/* Send line */
		br = new BufferedReader(new FileReader(filename));

		for (int slaveIdx = 0; slaveIdx < slaves.length; slaveIdx++) {
		    //System.out.println(slaves[slaveIdx]);
		    mSocket = new Socket(slaves[slaveIdx], port);

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
		System.out.println(slave);
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
