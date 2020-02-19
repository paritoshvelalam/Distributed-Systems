package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static final String[] remote_ports = {"11108", "11112", "11116", "11120", "11124" };
	static final String[] ports = {"5554", "5556", "5558", "5560", "5562"};
	HashMap<String, Boolean> portstatus = new HashMap<>();
	NavigableMap<String, String> chordmap = new TreeMap<>();
	Map.Entry<String, String> temp;
	String myPort;
	String myport_hash;
	String portStr;
	int[] timeouts = {550, 600, 650, 700, 750};
	int mytimeout;
	List<String> keylist = new ArrayList<String>();
	int versionnum = 1;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	List<String> fkeys = new ArrayList<String>();
	List<String> fvalues = new ArrayList<String>();
	HashMap<String, String> backup = new HashMap<>();
	List<String> successornode = new ArrayList<String>();
	List<String> successornode_hash = new ArrayList<String>();
	Boolean initialized = false;
	String successor;
	String successor1_hash;
	String successor2_hash;
	String predecessor1_hash;
	String predecessor2_hash;
	String successor1_port;
	String successor2_port;
	String predecessor1_port;
	String predecessor2_port;
	String successor1_port_id;
	String successor2_port_id;
	String predecessor1_port_id;
	String predecessor2_port_id;

	private Object getContentResolver() {
		return getContext().getContentResolver();
	}
	//
	private Object getSystemService(String telephonyService) {
		return getContext().getSystemService(Context.TELEPHONY_SERVICE);
	}

	private void getpredecessorsuccessor(String k){
		temp=chordmap.higherEntry(k);
		if(temp!=null){
			successor1_port_id = temp.getValue();
			successor1_port = String.valueOf((Integer.parseInt(successor1_port_id) * 2));
			successor1_hash = temp.getKey();
		}
		else {
			successor1_port_id = chordmap.firstEntry().getValue();
			successor1_port = String.valueOf((Integer.parseInt(successor1_port_id) * 2));
			successor1_hash = chordmap.firstEntry().getKey();
		}
		temp=chordmap.higherEntry(successor1_hash);
		if(temp!=null){
			successor2_port_id = temp.getValue();
			successor2_port = String.valueOf((Integer.parseInt(successor2_port_id) * 2));
			successor2_hash = temp.getKey();
		}
		else {
			successor2_port_id = chordmap.firstEntry().getValue();
			successor2_port = String.valueOf((Integer.parseInt(successor2_port_id) * 2));
			successor2_hash = chordmap.firstEntry().getKey();
		}
		temp = chordmap.lowerEntry(k);
		if(temp!=null){
			predecessor1_port_id = temp.getValue();
			predecessor1_port = String.valueOf((Integer.parseInt(predecessor1_port_id) * 2));
			predecessor1_hash = temp.getKey();
		}
		else {
			predecessor1_port_id = chordmap.lastEntry().getValue();
			predecessor1_port = String.valueOf((Integer.parseInt(predecessor1_port_id) * 2));
			predecessor1_hash = chordmap.lastEntry().getKey();
		}
		temp = chordmap.lowerEntry(predecessor1_hash);
		if(temp!=null){
			predecessor2_port_id = temp.getValue();
			predecessor2_port = String.valueOf((Integer.parseInt(predecessor2_port_id) * 2));
			predecessor2_hash = temp.getKey();
		}
		else {
			predecessor2_port_id = chordmap.lastEntry().getValue();
			predecessor2_port = String.valueOf((Integer.parseInt(predecessor2_port_id) * 2));
			predecessor2_hash = chordmap.lastEntry().getKey();
		}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		//myport
		TelephonyManager tel = (TelephonyManager) getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		int i = (Integer.parseInt(portStr)-5554)/2;
		mytimeout = timeouts[i];
		try{
			myport_hash = genHash(portStr);
		} catch(NoSuchAlgorithmException e){
			Log.e(TAG, "Oncreate NoSuchAlgorithm Exception");
		}
		//generate hash values for all ports
		for (int j=0;j<5;j++){
			try{
				chordmap.put(genHash(ports[j]),ports[j]);
			} catch(NoSuchAlgorithmException e){
				Log.e(TAG, "Oncreate NoSuchAlgorithm Exception");
			}
		}
		//successor

		getpredecessorsuccessor(myport_hash);
		Log.i("Successor 1", successor1_port);
		Log.i("Successor 2", successor2_port);

		for(int k=0;k<5;k++){
			portstatus.put(remote_ports[k], true);
		}

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask((ContentResolver)getContentResolver(), myPort, getContext()).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

		try{
			String msg = "F:" + myPort;
			Log.i(TAG, "sent succ F: "+successor1_port);
			new ClientInitTask((ContentResolver)getContentResolver(), myPort, getContext()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,successor1_port,msg).get();
		} catch(InterruptedException e){
			Log.i("Intialize error", "Initialize error");
		} catch(ExecutionException e){
			Log.i("Initialize errror", "Initialize error");
		}
		initialized = true;

		Log.i("I am done", "i am done");
		return true;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("*")){
			int keysize = keylist.size();
			String[] files = getContext().fileList();
			for(String file : files){
				try {
					getContext().deleteFile(file);
				} catch(Exception e){
					Log.e(TAG, "delete * Exception");
				}
			}
			keylist.clear();
		}
		else if(selection.equals("@")){
			int keysize = keylist.size();
			String[] files = getContext().fileList();
			for(String file : files){
				try {
					getContext().deleteFile(file);
				} catch(Exception e){
					Log.e(TAG, "delete * Exception");
				}
			}
			keylist.clear();
		}
		else{
			String[] files = getContext().fileList();
			for(String file : files){
				try {
					getContext().deleteFile(file);
				} catch(Exception e){
					Log.e(TAG, "delete * Exception");
				}
			}
			keylist.clear();
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		while (!initialized){
			try{
				Thread.sleep(100);
			} catch (InterruptedException e){
				Log.e(TAG, "Interrupted Exception");
			}
		}
		String key = (String) values.get("key");
		String value = (String) values.get("value");
		String key_hash = null;
		List<String> fwdnode = new ArrayList<String>();
		List<String> fwdnode_hash = new ArrayList<String>();
		String msg = null;
		List<String> qr = new ArrayList<String>();
		Log.i("req for ins key", key);
		try {
			key_hash = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "insert NoSuchAlgoException");
			return null;
		}
		fwdnode_hash.add(key_hash);
		String tempnode;
		for (int i=0;i<4;i++) {
			temp = chordmap.higherEntry(fwdnode_hash.get(i));
			if (temp != null) {
				tempnode = temp.getValue();
				fwdnode.add(String.valueOf((Integer.parseInt(tempnode) * 2)));
				fwdnode_hash.add(temp.getKey());
			} else {
                tempnode = chordmap.firstEntry().getValue();
				fwdnode.add(String.valueOf((Integer.parseInt(tempnode) * 2)));
				fwdnode_hash.add(chordmap.firstEntry().getKey());
			}
		}
		Log.i("key belongs to", fwdnode.get(0));
		String query_reply = null;
		try{
			msg = "V" + ":" + key;
			query_reply = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,fwdnode.get(0),msg).get();
			Log.i("First V query insert", query_reply);
			if(query_reply!=null)
				if(query_reply.equals("Server down") || query_reply.equals("no data"))
					qr.add(null);
				else
					qr.add(query_reply);
			else
				qr.add(null);
			query_reply = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,fwdnode.get(1),msg).get();
			Log.i("Second V query insert", query_reply);
			if(query_reply!=null)
				if(query_reply.equals("Server down") || query_reply.equals("no data"))
					qr.add(null);
				else
					qr.add(query_reply);
			else
				qr.add(null);
			query_reply = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,fwdnode.get(2),msg).get();
			Log.i("Third V query insert", query_reply);
			if(query_reply!=null)
				if(query_reply.equals("Server down") || query_reply.equals("no data"))
					qr.add(null);
				else
					qr.add(query_reply);
			else
				qr.add(null);

		} catch(InterruptedException e){
			Log.i("Get version error", "get version error");
		} catch(ExecutionException e){
			Log.i("Get version errror", "get version error");
		}
		List<Integer> version= new ArrayList<Integer>();
		String[] contents;
		String[] valver;
		for(int i=0; i<3; i++){
			if(qr.get(i)!=null) {
				contents = qr.get(i).split(":");
				valver = contents[1].split("-");
				version.add(Integer.parseInt(valver[1]));
			}
			else
				version.add(0);
		}
		int maxversionnum = Collections.max(version, null);
		maxversionnum++;
		for (int i=0;i<3;i++) {
			try {
				msg = "R" + ":" + key + ":" + value + "-" + maxversionnum;
				Log.i("Insert ", msg + " + " + fwdnode.get(i));
				String status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fwdnode.get(i), msg).get();
				Log.i("R f loop insert", status);
				if (status.equals("Server down")) {
					i++;
					if(i==3){
						contents = msg.split(":");
						contents[0] = "B";
						msg = null;
						int size = contents.length;
						msg = contents[0];
						for(int j=1;j<size;j++){
							msg = msg + ":" + contents[j];
						}
						Log.i("Insert ", msg + " + " + fwdnode.get(i));
						status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fwdnode.get(i), msg).get();
						Log.i("R s loop i=3 insert", status);
					}
					else{
						msg = msg + ":" + "backup";
						Log.i("Insert ", msg + " + " + fwdnode.get(i));
						status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fwdnode.get(i), msg).get();
						Log.i("R s loop i!=3 insert", status);
					}
				}
			} catch (ExecutionException e) {
				Log.i(TAG, "replication error");
				return null;
			} catch (InterruptedException e) {
				Log.i(TAG, "replication error");
				return null;
			}
		}
		return uri;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		Log.i("I am called query", selection);
		// TODO Auto-generated method stub
		String key = selection;
		String[] valuecontents;
		String[] columnNames = {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(columnNames);
		String msg= null;
		if(selection.equals("*")){
			String query_reply = null;
			String keyvalues = null;
			for(int i=0;i<5;i++) {
				try {
					msg = "G" + ":" +"@";
					query_reply = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remote_ports[i],msg).get();
					if(query_reply!=null && !query_reply.equals("no data") && !query_reply.equals("Server down")){
						if(keyvalues!=null)
							keyvalues = keyvalues + ":" + query_reply;
						else
							keyvalues = query_reply;
					}
				} catch (InterruptedException e) {
					Log.e(TAG, "query * Interrupted Exception");
				} catch (ExecutionException e) {
					Log.e(TAG, "query * Concurrent Exception");
				}
			}
			if(keyvalues!=null){
				String[] contents = keyvalues.split(":");
				int contents_size= contents.length;
				for (int i=0;i<contents_size;i=i+2){
					String[] row_values = new String[2];
					row_values[0] = contents[i];
					valuecontents = contents[i+1].split("-");
					row_values[1] = valuecontents[0];
					cursor.addRow(row_values);
					Log.v("query", "key="+contents[i] + "  " +"value=" + contents[i+1]);
				}
			}
			return cursor;
		}
		else if(selection.equals("@")){
//			String query_reply = null;
//			String keyvalues = null;
			String[] files = getContext().fileList();
//			try {
//				msg = "G" + ":" + "@";
//				query_reply = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, msg).get();
//				if(query_reply!=null && !query_reply.equals("no data") && !query_reply.equals("Server down")){
//					if(keyvalues!=null)
//						keyvalues = keyvalues + ":" + query_reply;
//					else
//						keyvalues = query_reply;
//				}
//			} catch (InterruptedException e) {
//				Log.e(TAG, "query * Interrupted Exception");
//			} catch (ExecutionException e) {
//				Log.e(TAG, "query * Concurrent Exception");
//			}
			for(String file : files){
					key = file;
					String key_hash =null;
					List<String> fwdnode = new ArrayList<String>();
					List<String> fwdnode_hash = new ArrayList<String>();
					List<String> qr = new ArrayList<String>();
					Log.i("* req for que key", key);
					try {
						key_hash = genHash(key);
					} catch (NoSuchAlgorithmException e) {
						Log.e(TAG, "insert NoSuchAlgoException");
						return null;
					}
					fwdnode_hash.add(key_hash);
					String tempnode;
					for (int i=0;i<4;i++) {
						temp = chordmap.higherEntry(fwdnode_hash.get(i));
						if (temp != null) {
							tempnode = temp.getValue();
							fwdnode.add(String.valueOf((Integer.parseInt(tempnode) * 2)));
							fwdnode_hash.add(temp.getKey());
						} else {
							tempnode = chordmap.firstEntry().getValue();
							fwdnode.add(String.valueOf((Integer.parseInt(tempnode) * 2)));
							fwdnode_hash.add(chordmap.firstEntry().getKey());
						}
					}
					Log.i("* key belongs to", fwdnode.get(0));
					String query_reply = null;
					try{
						msg = "V" + ":" + key;
						query_reply = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,fwdnode.get(0),msg).get();
						Log.i("* First V query query", query_reply );
						if(query_reply!=null)
							if(query_reply.equals("Server down") || query_reply.equals("no data"))
								qr.add(null);
							else
								qr.add(query_reply);
						else
							qr.add(null);
						query_reply = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,fwdnode.get(1),msg).get();
						Log.i("* Second V query query", query_reply);
						if(query_reply!=null)
							if(query_reply.equals("Server down") || query_reply.equals("no data"))
								qr.add(null);
							else
								qr.add(query_reply);
						else
							qr.add(null);
						query_reply = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,fwdnode.get(2),msg).get();
						Log.i("* Third V query query", query_reply);
						if(query_reply!=null)
							if(query_reply.equals("Server down") || query_reply.equals("no data"))
								qr.add(null);
							else
								qr.add(query_reply);
						else
							qr.add(null);
					} catch(InterruptedException e){
						Log.i("Get version error", "get version error");
					} catch(ExecutionException e){
						Log.i("Get version errror", "get version error");
					}
					List<Integer> version= new ArrayList<Integer>();
					String[] contents = null;
					String[] valver = null;
					for(int i=0; i<3; i++){
						if(qr.get(i)!=null) {
							contents = qr.get(i).split(":");
							valver = contents[1].split("-");
							version.add(Integer.parseInt(valver[1]));
						}
						else
							version.add(0);
					}
					int maxversionnum = Collections.max(version, null);
					if(maxversionnum!=0) {
						String value = qr.get(version.indexOf(maxversionnum)).split(":")[1];
						for (int i = 0; i < 3; i++) {
							if(version.get(i)< maxversionnum) {
								try {
									msg = "R" + ":" + key + ":" + value;
									Log.i("* Insert ", msg + " + " + fwdnode.get(i));
									String status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fwdnode.get(i), msg).get();
									Log.i("* R f loop insert", status);
								} catch (ExecutionException e) {
									Log.i(TAG, "* replication error");
									return null;
								} catch (InterruptedException e) {
									Log.i(TAG, "* replication error");
									return null;
								}
							}
						}
						String[] row_values = new String[2];
						row_values[0] = key;
						row_values[1] = value.split("-")[0];
						cursor.addRow(row_values);
						Log.v("* Query ", key + ":" + value);
					}
				}
			return cursor;
		}
		else{
		    String key_hash =null;
            List<String> fwdnode = new ArrayList<String>();
            List<String> fwdnode_hash = new ArrayList<String>();
            List<String> qr = new ArrayList<String>();
			Log.i("req for que key", key);
			try {
				key_hash = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG, "insert NoSuchAlgoException");
				return null;
			}
			fwdnode_hash.add(key_hash);
			String tempnode;
			for (int i=0;i<4;i++) {
				temp = chordmap.higherEntry(fwdnode_hash.get(i));
				if (temp != null) {
					tempnode = temp.getValue();
					fwdnode.add(String.valueOf((Integer.parseInt(tempnode) * 2)));
					fwdnode_hash.add(temp.getKey());
				} else {
					tempnode = chordmap.firstEntry().getValue();
					fwdnode.add(String.valueOf((Integer.parseInt(tempnode) * 2)));
					fwdnode_hash.add(chordmap.firstEntry().getKey());
				}
			}
			Log.i("key belongs to", fwdnode.get(0));
			String query_reply = null;
			try{
				msg = "V" + ":" + key;
				query_reply = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,fwdnode.get(0),msg).get();
				Log.i("First V query query", query_reply );
				if(query_reply!=null)
					if(query_reply.equals("Server down") || query_reply.equals("no data"))
						qr.add(null);
					else
						qr.add(query_reply);
				else
					qr.add(null);
				query_reply = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,fwdnode.get(1),msg).get();
				Log.i("Second V query query", query_reply);
				if(query_reply!=null)
					if(query_reply.equals("Server down") || query_reply.equals("no data"))
						qr.add(null);
					else
						qr.add(query_reply);
				else
					qr.add(null);
				query_reply = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,fwdnode.get(2),msg).get();
				Log.i("Third V query query", query_reply);
				if(query_reply!=null)
					if(query_reply.equals("Server down") || query_reply.equals("no data"))
						qr.add(null);
					else
						qr.add(query_reply);
				else
					qr.add(null);
			} catch(InterruptedException e){
				Log.i("Get version error", "get version error");
			} catch(ExecutionException e){
				Log.i("Get version errror", "get version error");
			}
			List<Integer> version= new ArrayList<Integer>();
			String[] contents = null;
			String[] valver = null;
			for(int i=0; i<3; i++){
				if(qr.get(i)!=null) {
					contents = qr.get(i).split(":");
					valver = contents[1].split("-");
					version.add(Integer.parseInt(valver[1]));
				}
				else
					version.add(0);
			}
			int maxversionnum = Collections.max(version, null);
			if(maxversionnum!=0) {
				String value = qr.get(version.indexOf(maxversionnum)).split(":")[1];
				for (int i = 0; i < 3; i++) {
					try {
						msg = "R" + ":" + key + ":" + value ;
						Log.i("Insert ", msg + " + " + fwdnode.get(i));
						String status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fwdnode.get(i), msg).get();
						Log.i("R f loop insert", status);
						if (status.equals("Server down")) {
							i++;
							if(i==3){
								contents = msg.split(":");
								contents[0] = "B";
								msg = null;
								int size = contents.length;
								msg = contents[0];
								for(int j=1;j<size;j++){
									msg = msg + ":" + contents[j];
								}
								Log.i("Insert ", msg + " + " + fwdnode.get(i));
								status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fwdnode.get(i), msg).get();
								Log.i("R s loop i=3 insert", status);
							}
							else{
								msg = msg + ":" + "backup";
								Log.i("Insert ", msg + " + " + fwdnode.get(i));
								status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fwdnode.get(i), msg).get();
								Log.i("R s loop i!=3 insert", status);
							}
						}
					} catch (ExecutionException e) {
						Log.i(TAG, "replication error");
						return null;
					} catch (InterruptedException e) {
						Log.i(TAG, "replication error");
						return null;
					}
				}
				String[] row_values = new String[2];
				row_values[0] = key;
				row_values[1] = value.split("-")[0];
				cursor.addRow(row_values);
				Log.v("Query ", key + ":" + value);
				return cursor;
			}
			else
				return null;
			}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }



	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}


    // server task
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		private final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		private final ContentResolver contentResolver;
		private ContentValues contentvalues = new ContentValues();
		private final Context mContext;
		String myPort; // Server Port
		String data;
		String[] contents;
		public ServerTask (ContentResolver content_resolve, String myPort, Context context) //constructor
		{
			contentResolver = content_resolve;
			this.myPort = myPort;
			this.mContext = context;

		}

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			while(true) {
				try {
					Socket server_rec = serverSocket.accept();
					BufferedReader received_data = new BufferedReader(new InputStreamReader(server_rec.getInputStream(), StandardCharsets.UTF_8));
					PrintWriter send_data = new PrintWriter(server_rec.getOutputStream(), true);
					data = received_data.readLine();
					Log.i("server task rec data", data );
					send_data.println("ack");
					if (data != null) {
						contents = data.split(":");
						int size = contents.length;
							if(contents[0].equals("R")){
							int versnum = 0;
							try {
								FileInputStream inputStream = mContext.openFileInput(contents[1]);
								byte[] b = new byte[150];
								int len_read = inputStream.read(b);
								String value1 = new String(b, 0, len_read);
								versnum = Integer.parseInt(value1.split("-")[1]);
							} catch (Exception e){
								versnum = 0;
							}
							String filename = contents[1];
							String value = contents[2];
							int newversnum = Integer.parseInt(value.split("-")[1]);
							if (newversnum>=versnum) {
								FileOutputStream outputStream;
								try {
									outputStream = mContext.openFileOutput(filename, mContext.MODE_PRIVATE);
									outputStream.write(value.getBytes());
									outputStream.close();
									keylist.add(contents[1]);
									if(size == 4){
										if(contents[3].equals("backup")){
											backup.put(filename, value);
										}
									}
									send_data.println("replication successfull");
									Log.i("sent replication ack", "sent");
									Log.i(TAG, "Inserted key: " + filename);
								} catch (Exception e) {
									Log.e("Insert", "Insert File File not Found Exception");
									send_data.println("replication not successfull");
								}
							}
							else
								send_data.println("replication successfull");
						}
						else if(contents[0].equals("G")){
							Log.i("I am called", "I am called");
							String query_reply=null;
							String[] files = mContext.fileList();
							int keysize = keylist.size();
							Log.i("keylist size", String.valueOf(keysize));
							for(String file : files){
								try
								{
									FileInputStream inputStream = mContext.openFileInput(file);
									byte[] b = new byte[150];
									int len_read = inputStream.read(b);
									String value = new String(b, 0 , len_read);
									//Log.i("key + value" , selection + "    " + value);
									if(query_reply!=null)
										query_reply = query_reply + ":" + file + ":" + value;
									else
										query_reply = file + ":"+ value;
									inputStream.close();
								}
								catch(Exception e)
								{
									Log.e("query", "G:@");
								}
							}
							if(query_reply!=null)
								send_data.println(query_reply);
							else
								send_data.println("no data");
						}
						else if(contents[0].equals("V")){
							String query_reply=null;
							try {
								FileInputStream inputStream = getContext().openFileInput(contents[1]);
								byte[] b = new byte[150];
								int len_read = inputStream.read(b);
								String value = new String(b, 0, len_read);
								query_reply = contents[1] + ":" + value;
							} catch(Exception e){
								query_reply = "no data";
								Log.i("Server Task", "Version Error");
							}
							send_data.println(query_reply);
						}
						else if(contents[0].equals("F")){
							Log.i("I am called by", contents[1]);
							String query_reply=null;
							for (Map.Entry<String, String> entry : backup.entrySet()) {
								if(entry!=null) {
									if (query_reply != null) {
										query_reply = query_reply + ":" + entry.getKey() + ":" + entry.getValue();
									} else
										query_reply = entry.getKey() + ":" + entry.getValue();
								}
							}
							if(query_reply!=null){
								send_data.println(query_reply);
							}
							else
								send_data.println("no data");
								Log.i("F reply ", "no data");
							backup.clear();
						}
						else if(contents[0].equals("B")){
								String key = contents[1];
								String value = contents[2];
								backup.put(key, value);
								send_data.println("backupsuccessfull");
						}
					}
					send_data.close();
					received_data.close();
					server_rec.close();
				} catch(IOException e){
					Log.e(TAG, "Servertask IOException");
				}
			}

//		return null;
		}

		protected void onProgressUpdate(String...strings) {
//            String strReceived = strings[0].trim();

			Log.i("Remote Insertion", "Remote Insertion");
			contentvalues.put("key", strings[0]);
			contentvalues.put("value", strings[1]);
			// Log.i("key" , strReceived);

			try {
				contentResolver.insert(uri, contentvalues);
			} catch (Exception e){
				Log.e(TAG, e.toString());
			}
		}
	}


	//Insert Client Task for forwarding insert messages not belonging to my node
	private class ClientTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {
			String query_reply = null;
			Socket socket = null;
			PrintWriter send_data = null;
			BufferedReader received_data = null;
			try{
				String msgToSend = msgs[1];
				String msgID = msgToSend.split(":")[0];
				if(msgID.equals("A")){
					for(int i=0;i<5;i++) {
						socket = new Socket();
						socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remote_ports[i])), 0);
						send_data = new PrintWriter(socket.getOutputStream(), true);
						received_data = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						socket.setSoTimeout(4000);
						do {
							send_data.println(msgToSend);
						} while (!received_data.readLine().equals("ack"));
						send_data.close();
						received_data.close();
						socket.close();
					}
				}
				else {
					socket = new Socket();
					socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(msgs[0])), 0);
					send_data = new PrintWriter(socket.getOutputStream(), true);
					received_data = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					if (msgID.equals("V")) {
						do {
							send_data.println(msgToSend);
						} while (!received_data.readLine().equals("ack"));
						String reply = received_data.readLine();
						Log.i("V received data", reply);
						if(reply!= null)
							query_reply = reply;
						send_data.close();
						received_data.close();
						socket.close();
						return query_reply;
					} else if (msgID.equals("R")) {
						do {
							send_data.println(msgToSend);
						} while (!received_data.readLine().equals("ack"));
						String reply = received_data.readLine();
						if (reply.equals("replication successfull"))
							reply = reply;
						if (reply != null) {
							query_reply = reply;
						}
						send_data.close();
						received_data.close();
						socket.close();
						return query_reply;
					} else if (msgID.equals("G")) {
						do {
							send_data.println(msgToSend);
						} while (!received_data.readLine().equals("ack"));
						String reply = received_data.readLine();
						if (reply!=null)
							query_reply = reply;
						send_data.close();
						received_data.close();
						socket.close();
						return query_reply;
					} else if (msgID.equals("B")){
						do {
							send_data.println(msgToSend);
						} while (!received_data.readLine().equals("ack"));
						String reply = received_data.readLine();
						if(reply!=null)
							query_reply = reply;
						send_data.close();
						received_data.close();
						socket.close();
						return query_reply;
					}
				}
			} catch(IOException e){
				Log.e(TAG, "ClientTask IOException");
				try {
					send_data.close();
					received_data.close();
					socket.close();
				} catch(IOException e1){
					Log.e(TAG, "inside catch IOException");
				}
				return "Server down";
			} catch(NullPointerException e){
				Log.e(TAG, "ClientTask Nullpointer exception");
				Log.i("failed port is ", msgs[0]);
				portstatus.put(msgs[0], false);
				try {
					send_data.close();
					received_data.close();
					socket.close();
				} catch(IOException e1){
					Log.e(TAG, "inside catch IOException");
				}
				return "Server down";
			}
			return null;
		}

	}

	private class ClientInitTask extends AsyncTask<String, Void, String> {

		private final ContentResolver contentResolver;
		private ContentValues contentvalues = new ContentValues();
		private final Context mContext;
		String myPort; // Server Port
		String data;
		String[] contents;
		public ClientInitTask (ContentResolver content_resolve, String myPort, Context context) //constructor
		{
			contentResolver = content_resolve;
			this.myPort = myPort;
			this.mContext = context;

		}
		@Override
		protected String doInBackground(String... msgs) {
			String query_reply = null;
			int count = 0;
			Socket socket = null;
			PrintWriter send_data = null;
			BufferedReader received_data = null;
			while (count < 2) {
				try {
					socket = new Socket();
					socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(msgs[0])), 0);
					socket.setSoTimeout(2000);
					send_data = new PrintWriter(socket.getOutputStream(), true);
					received_data = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String msgToSend = msgs[1];
					String msgID = msgToSend.split(":")[0];
					if (msgID.equals("F")) {
						do {
							send_data.println(msgToSend);
						} while (!received_data.readLine().equals("ack"));
						Log.i("Init Client Task", "Ack received");
						String reply = received_data.readLine();
						if (reply.equals("no data"))
							reply = null;
						if (reply != null) {
							int versnum = 0;
							contents = reply.split(":");
							int contentsize = contents.length;
							for (int i = 0; i < contentsize; i = i + 2) {
								try {
									FileInputStream inputStream = mContext.openFileInput(contents[i]);
									byte[] b = new byte[150];
									int len_read = inputStream.read(b);
									String value1 = new String(b, 0, len_read);
									versnum = Integer.parseInt(value1.split("-")[1]);
								} catch (Exception e) {
									versnum = 0;
								}
								String filename = contents[i];
								String value = contents[i + 1];
								int newversnum = Integer.parseInt(value.split("-")[1]);
								if (newversnum > versnum) {
									FileOutputStream outputStream;
									try {
										outputStream = mContext.openFileOutput(filename, mContext.MODE_PRIVATE);
										outputStream.write(value.getBytes());
										outputStream.close();
										keylist.add(contents[i]);
										Log.i(TAG, "Init Inserted key: " + filename);
									} catch (Exception e) {
										Log.e("Insert", "Insert File File not Found Exception");
									}
								}
							}
						}
						send_data.close();
						received_data.close();
						socket.close();
						return query_reply;
					}
				} catch (SocketTimeoutException e) {
					count++;
					try {
						send_data.close();
						received_data.close();
						socket.close();
					} catch(IOException e1){
						Log.e(TAG, "inside catch IOException");
					}
				} catch (IOException e) {
					Log.e(TAG, "ClientTask IOException");
					try {
						send_data.close();
						received_data.close();
						socket.close();
					} catch(IOException e1){
						Log.e(TAG, "inside catch IOException");
					}
					return null;
				} catch (NullPointerException e) {
					Log.e(TAG, "Null pointer exception");
					Log.e(TAG, successor1_port);
					portstatus.put(successor1_port, false);
					try {
						send_data.close();
						received_data.close();
						socket.close();
					} catch(IOException e1){
						Log.e(TAG, "inside catch IOException");
					}
					return null;
				}
			}
			return null;
		}

	}

}


