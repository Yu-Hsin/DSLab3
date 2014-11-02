import java.io.*;
import java.net.*;

public class Master {

    private static final int SlaveNode = 4;

    public static void main(String[] args) {
	try {
	    Client mSocketRunnable = new Client(new Socket(args[0],
		    Integer.valueOf(args[1])), args[2]);
	    Thread t = new Thread(mSocketRunnable);
	    t.start();

	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public static class Client implements Runnable {
	private Socket mSocket;
	private String filename;

	public Client(Socket s, String fname) {
	    mSocket = s;
	    filename = fname;
	}

	@Override
	public void run() {

	    try {
		ObjectOutputStream out = new ObjectOutputStream(
			mSocket.getOutputStream());

		BufferedReader br = new BufferedReader(new FileReader(filename));
		String s = null;

		/* Count the total Line */
		int len = 0;
		while ((s = br.readLine()) != null)
		    len++;
		br.close();

		int lenInOnePart = len / SlaveNode;

		/* Send line */
		int idx = 0;
		br = new BufferedReader(new FileReader(filename));
		while ((s = br.readLine()) != null && idx < lenInOnePart) {
		    byte[] buf = s.getBytes();
		    if (s.length() <= 1024) {
			out.write(buf, 0, buf.length);
			out.flush();
		    } else {
			for (int i = 0; i < buf.length; i += 1024) {
			    out.write(buf, i,
				    (i + 1024 >= buf.length) ? buf.length - i
					    : 1024);
			}
			out.flush();
		    }
		    idx++;
		}

		br.close();
		mSocket.close();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}

    }

}
