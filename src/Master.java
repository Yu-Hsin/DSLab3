import java.io.*;
import java.net.*;

public class Master {

    private static final int SlaveNode = 1;

    private static String[] ips = {"128.237.183.43"};

    //private static String[] ips = { "ghc53.ghc.andrew.cmu.edu",
    //	    "ghc52.ghc.andrew.cmu.edu" };

    public static void main(String[] args) {
	SendFileThread mSocketRunnable = new SendFileThread(ips,
		Integer.parseInt(args[0]), args[1]);
	Thread t = new Thread(mSocketRunnable);
	t.start();
	try {
	    t.join();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}

	SendJarThread mJarSocketRunnable = new SendJarThread(ips, Integer.parseInt(args[0]), args[2]);
	t = new Thread(mJarSocketRunnable);
	t.start();
	
	
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

		for (int slaveIdx = 0; slaveIdx < SlaveNode; slaveIdx++) {
		    System.out.println(slaves[slaveIdx]);
		    mSocket = new Socket(slaves[slaveIdx], port);

		    ObjectOutputStream out = new ObjectOutputStream(
			    mSocket.getOutputStream());

		    int idx = 0;

		    out.write((slaveIdx + "\n").getBytes());
		    out.flush();

		    while (idx < lenInOnePart && (s = br.readLine()) != null) {
			System.out.println(s + " " + idx);
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
		}
		mSocket.close();
		br.close();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}

    }



    public static class SendJarThread implements Runnable{
	private Socket mSocket = null;
	private String filename;
	private String[] slaves;
	private int port;

	public SendJarThread(String[] s, int p, String fname) {
	    slaves = s;
	    port = p;
	    filename = fname;
	}

	@Override
	public void run() {
	    try {
		for (int slaveIdx = 0; slaveIdx < SlaveNode; slaveIdx++) {
		    System.out.println(slaves[slaveIdx]);
		    mSocket = new Socket(slaves[slaveIdx], port);
		    OutputStream out = mSocket.getOutputStream();
		    DataOutputStream dout = new DataOutputStream(out);

		    File jarFile = new File(filename);
		    BufferedInputStream bis = new BufferedInputStream(new FileInputStream(jarFile));

		    dout.writeUTF(filename);
		    
		    
		    byte[] buffer = new byte[1024];
		    int count = 0;
		    while((count = bis.read(buffer)) > 0) {
			dout.write(buffer, 0, count);
			dout.flush();
		    }
		    System.out.println("Done.");
		    
		    bis.close();
		} 
		
		mSocket.close();
	    }
	    catch (Exception e) {
		e.printStackTrace();
	    }
	}



    }
}
