package edu.buffalo.cse.cse486586.groupmessenger2;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.net.Uri;
import android.text.method.ScrollingMovementMethod;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import android.app.Activity;
import android.content.Context;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.View;
import android.view.View.OnKeyListener;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *Ref: PA1 and https://developer.android.com/guide/topics/providers/content-providers
 * Ref: Implemented ISIS algoritgm for achieving FIFO and total ordering
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String[] remote_ports = {"11108", "11112", "11116", "11120", "11124" };
    String counter = "0";
    boolean[] connection_status = {true,true,true,true,true }; //connection status of all AVDs
    int myseq = 0; //Sequence number for incoming sequence requests


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
/***************************************************************************************************/

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        TextView tv = (TextView) findViewById(R.id.textView1);

        tv.setMovementMethod(new ScrollingMovementMethod());
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            new ServerTask(getContentResolver(), tv, myPort).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /* TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */


        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final EditText editText = (EditText) findViewById(R.id.editText1);



        final Button button = (Button) findViewById(R.id.button4);
        button.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v){
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                // for(int j=0; j<5; j++)
                if(msg.length()>0) {
                    counter = String.format("%04d", (Integer.parseInt(counter) + 1)); //counter serves as message ID from a particular client.
                    Log.i("counter value is", counter + "for" + msg); //Increment by one and send counter value along with message
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort, counter); //client async task to multicast messageand receive sequence number proposals
                }
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    PriorityQueue<pendingmsg> pq = new PriorityQueue<pendingmsg>(5, new pendingmsgcomparator());//priority Queue in which messages waiting for delivery are placed
    public class pendingmsg{ // each message contains message source, message ID, message sequence number, seq no suggested by which server, delivery status
        public String msg_data;
        public int mid;
        public String msg_source;
        public float seq_msg;
        public String seq_sug_source;
        public int deliverystatus;

        public pendingmsg(String msg_data, int mid, String msg_source, float seq_msg, String seq_sug_source, int deliverystatus){
            this.msg_data = msg_data;
            this.mid = mid;
            this.msg_source = msg_source;
            this.seq_msg = seq_msg;
            this.seq_sug_source = seq_sug_source;
            this.deliverystatus = deliverystatus;
        }

        public int getDeliverystatus(){
            return deliverystatus;
        }
        public String getmsgdata(){
            return msg_data;
        }
        public int getmid(){ return mid;}
        public String getmsgsrc(){return msg_source;}
        public String getappended(){return (msg_source+String.format("%04d",mid));}
    }

    public class pendingmsgcomparator implements Comparator<pendingmsg> { //comparator implementation for priority queue
        public int compare(pendingmsg m1, pendingmsg m2){
            if(m1.seq_msg > m2.seq_msg)
                return 1;
            else if(m1.seq_msg < m2.seq_msg)
                return -1;
            return 0;
        }
    }
    List<pendingmsg> tmpque = new ArrayList<pendingmsg>(); //temporary queue to hold all queue elements after changing priority queue elements

    public void sendfailmsg(int j, String myp){ //method to multicast message to inform others of failure of a server
        for(int jj=0;jj<5;jj++) {
            if (connection_status[jj] == true) {
                try {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_ports[jj])), 0);
                    socket.setSoTimeout(1000);
                    PrintWriter send_data = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader received_data = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String fail_msg = "flnode" + myp + remote_ports[j];
                    do {
                        send_data.println(fail_msg);
                        Log.i("Failed msg sent to ", String.valueOf(jj));
                    } while (!received_data.readLine().equals("ack"));
                    while(!received_data.readLine().equals("deleteddata")){
                        do {
                            send_data.println(fail_msg);
                            Log.i("Failed msg sent to ", String.valueOf(jj));
                        } while (!received_data.readLine().equals("ack"));
                    }
                    socket.close();
                    send_data.close();
                    received_data.close();
                }catch (IOException e1){
                    Log.e(TAG, "Client Socket exception inside catch");
                }
            }
        }
    }

    public boolean isalive(String src){ //checking whether the port is alive if serversocket timeout exception occurs.
        try { //This is to handle the case when the failed AVD and other AVD are multicasting message and failed AVD is the last AVD that has failed
            Socket socket = new Socket(); //In this case no client task of AVD can detect the failed AVD. So server waits for connection request.
            socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), //If it timesout indicates communication has ended.
                    Integer.parseInt(src)), 0); //Now priority queue is checked for deliverable status 0 messages and checks the msg source port is alive
            socket.setSoTimeout(1000);//if it is not so, delete failed node messages after detecting the failed node
            PrintWriter send_data = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader received_data = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String check_msg = "islive";
            do {
                send_data.println(check_msg);
                Log.i("check msg sent to ", src);
            } while (!received_data.readLine().equals("ack"));
            socket.close();
            send_data.close();
            received_data.close();
            return true;
        } catch (UnknownHostException e){
            Log.e(TAG, "Check alive status unknownhostexception");
            connection_status[((Integer.parseInt(src) - 11108) / 4) % 5] = false;
            return false;
        } catch(SocketTimeoutException e){
            Log.e(TAG, "Check alive status sickettimeout exception");
            connection_status[((Integer.parseInt(src) - 11108) / 4) % 5] = false;
            return false;
        } catch(SocketException e){
            Log.e(TAG, "Check alive status socketexception");
            connection_status[((Integer.parseInt(src) - 11108) / 4) % 5] = false;
            return false;
        } catch(NullPointerException e){
            Log.e(TAG, "Check alive status null exception");
            connection_status[((Integer.parseInt(src) - 11108) / 4) % 5] = false;
            return false;
        } catch(IOException e){
            Log.e(TAG, "Check alive status IOexception");
            connection_status[((Integer.parseInt(src) - 11108) / 4) % 5] = false;
            return false;
        }
    }
    /**********************************ServerTask**************************************************/

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        private final ContentResolver contentResolver;
        private final TextView textview;
        private ContentValues contentvalues = new ContentValues();
        private int key = 0;
        private final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
        private String s_key;
        String myPort; // Server Port
        String data; //received message
        String sender; //message received from
        String sender_mid; //message ID
        String sender_msg;// message data
        float strseq; // seq no of message
        int send_mid; // message ID
        int strseq_int;//int of seq no of message
        int tmpsize;
        String fromnode;//failed message received from
        String failednode;//failed AVD
        int failednode_i;//int of failednode
        String check_src;
        int check_src_i;
        public ServerTask (ContentResolver content_resolve, TextView tvw, String myPort) //constructor
        {
            contentResolver = content_resolve;
            textview = tvw;
            this.myPort = myPort;
        }

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
                ServerSocket serverSocket = sockets[0];
                Log.i("my port is ", myPort);
                while (true) {
                    try {
                        serverSocket.setSoTimeout(4000);
                        Socket server_rec = serverSocket.accept();
                        server_rec.setSoTimeout(1000); //setting timeout of 500ms
                        BufferedReader received_data = new BufferedReader(new InputStreamReader(server_rec.getInputStream(), StandardCharsets.UTF_8));
                        PrintWriter send_data = new PrintWriter(server_rec.getOutputStream(), true);
                        data = received_data.readLine();
                        send_data.println("ack");
                        Log.i("server rec data", data);
                        if (data != null) {
                            if (data.substring(0, 6).equals("reqseq")) { //message received from a client asking for sequence number proposal
                                myseq += 1;
                                do {
                                    send_data.println(String.format("%04d", myseq)); //sending msg seq no proposal
                                    Log.i("sending to", String.format("%04d", myseq));
                                } while (!received_data.readLine().equals("ack"));
                                //dividing the information received from incoming message
                                sender = data.substring(6, 11);
                                sender_mid = data.substring(11, 15);
                                sender_msg = data.substring(15);
                                strseq = (float) myseq + (float) ((((Integer.parseInt(myPort) - 11108) / 4) % 5) * 0.1);
                                Log.i("suggested value by me: ", String.valueOf(strseq));
                                pq.add(new pendingmsg(sender_msg, Integer.parseInt(sender_mid), sender, strseq, myPort, 0)); //add the received message to priority queue with delivery status 0
                            } else if (data.substring(0, 6).equals("finseq")) { //Finalized sequence message received from a client
                                sender = data.substring(6, 11);
                                sender_mid = data.substring(11, 15);
                                send_mid = Integer.parseInt(sender_mid);
                                strseq = Float.parseFloat(data.substring(15));
//                           String src = data.substring(19);
                                strseq_int = (int) strseq;
                                if (strseq_int > myseq)
                                    myseq = strseq_int;
                                while (pq.size() > 0) {
                                    pendingmsg pm = pq.poll();
                                    if (pm.getmid() == send_mid) {
                                        if (pm.msg_source.equals(sender)) { // changing the delivery status to 1 and setting the sequence no based on the message source and message ID
                                            pm.deliverystatus = 1;
                                            pm.seq_msg = strseq;
                                        }
                                    }
                                    tmpque.add(pm); //storing the priority queue elements in temporary queue so as to add the elements in priority queue again to get the least priority element at head of the queue
                                }
                                tmpsize = tmpque.size();
                                for (int i = 0; i < tmpsize; i++) {
                                    pq.add(tmpque.get(i)); //adding the elements back to priority queue
                                    Log.i("tmpque msgs", String.valueOf(tmpque.get(i).seq_msg));
                                }
                                tmpque.clear();

                            } else if (data.substring(0, 6).equals("flnode")) { //message received from a client containing the port of server which failed and the client that detected failure
                                fromnode = data.substring(6, 11);
                                failednode = data.substring(11);
                                failednode_i = Integer.parseInt(failednode);
                                connection_status[((failednode_i - 11108) / 4) % 5] = false;
                                Log.i("failed node:msg rec frm", failednode + " " + fromnode);
                                while (pq.size() > 0) {
                                    pendingmsg pm3 = pq.poll();
                                    if (!((pm3.msg_source.equals(failednode)) && (pm3.deliverystatus == 0))) { //removing failed client messages with delivery status=0 from the queue
                                        tmpque.add(pm3);
                                    } else
                                        Log.i("deleted msgsrc DS data", pm3.msg_source + " " + String.valueOf(pm3.deliverystatus) + " " + pm3.msg_data);
                                }
                                tmpsize = tmpque.size();
                                for (int i = 0; i < tmpsize; i++) {
                                    pq.add(tmpque.get(i));
                                }
                                tmpque.clear();
                                send_data.println("deleteddata");
                            }
                            Log.i("msgs delivered at once", "msgs delivered at once");
                            while ((pq.size()) > 0 ? (pq.peek().getDeliverystatus() == 1) : false) { //checking if there is any message at the head of the queue waiting to be delivered
                                pendingmsg pm2 = pq.poll();
                                publishProgress(pm2.getmsgdata());
                                Log.i("Msg", pm2.getmsgdata());
                                Log.i("Sequence nums", String.valueOf(pm2.seq_msg));
                                Log.i("deliverable status", String.valueOf(pm2.deliverystatus));
                            }

                        }
                        server_rec.close();
                    } catch (SocketTimeoutException e) {
                        while ((pq.size()) > 0 ? (pq.peek().getDeliverystatus() == 0) : false) { //checking if there is any message at the head of the queue blocking
                            check_src = pq.peek().msg_source;
                            check_src_i = Integer.parseInt(check_src);
                            Log.i(TAG, "last messages from queue " + check_src);
                            if(connection_status[((check_src_i - 11108) / 4) % 5] == false){
                                pq.poll();
                            }
                            else if (isalive(check_src) == false) {
                                pq.poll();
                            }
                        }
                        while ((pq.size()) > 0 ? (pq.peek().getDeliverystatus() == 1) : false) { //checking if there is any message at the head of the queue waiting to be delivered
                            pendingmsg pm3 = pq.poll();
                            publishProgress(pm3.getmsgdata());
                            Log.i(TAG, "last messages from queue getting delivered" + check_src);
                            Log.i("Msg", pm3.getmsgdata());
                            Log.i("Sequence nums", String.valueOf(pm3.seq_msg));
                            Log.i("deliverable status", String.valueOf(pm3.deliverystatus));
                        }
                        Log.e(TAG, "ServerTask socketTimeout Exception");
                    } catch (SocketException e){
                        while ((pq.size()) > 0 ? (pq.peek().getDeliverystatus() == 0) : false) { //checking if there is any message at the head of the queue blocking
                            check_src = pq.peek().msg_source;
                            check_src_i = Integer.parseInt(check_src);
                            Log.i(TAG, "last messages from queue " + check_src);
                            if(connection_status[((check_src_i - 11108) / 4) % 5] == false){
                                pq.poll();
                            }
                            else if (isalive(check_src) == false) {
                                pq.poll();
                            }
                        }
                        while ((pq.size()) > 0 ? (pq.peek().getDeliverystatus() == 1) : false) { //checking if there is any message at the head of the queue waiting to be delivered
                            pendingmsg pm3 = pq.poll();
                            publishProgress(pm3.getmsgdata());
                            Log.i(TAG, "last messages from queue getting delivered" + check_src);
                            Log.i("Msg", pm3.getmsgdata());
                            Log.i("Sequence nums", String.valueOf(pm3.seq_msg));
                            Log.i("deliverable status", String.valueOf(pm3.deliverystatus));
                        }
                        Log.e(TAG, "ServerTask socket Exception");
                    }
                    catch (IOException e) {
                        while ((pq.size()) > 0 ? (pq.peek().getDeliverystatus() == 0) : false) { //checking if there is any message at the head of the queue blocking
                            check_src = pq.peek().msg_source;
                            check_src_i = Integer.parseInt(check_src);
                            Log.i(TAG, "last messages from queue " + check_src);
                            if(connection_status[((check_src_i - 11108) / 4) % 5] == false){
                                pq.poll();
                            }
                            else if (isalive(check_src) == false) {
                                pq.poll();
                            }
                        }
                        while ((pq.size()) > 0 ? (pq.peek().getDeliverystatus() == 1) : false) { //checking if there is any message at the head of the queue waiting to be delivered
                            pendingmsg pm3 = pq.poll();
                            publishProgress(pm3.getmsgdata());
                            Log.i(TAG, "last messages from queue getting delivered" + check_src);
                            Log.i("Msg", pm3.getmsgdata());
                            Log.i("Sequence nums", String.valueOf(pm3.seq_msg));
                            Log.i("deliverable status", String.valueOf(pm3.deliverystatus));
                        }
                        Log.e(TAG, "ServerTask socket IOException");
                    } catch (NullPointerException e) {
                        while ((pq.size()) > 0 ? (pq.peek().getDeliverystatus() == 0) : false) { //checking if there is any message at the head of the queue blocking
                            check_src = pq.peek().msg_source;
                            check_src_i = Integer.parseInt(check_src);
                            Log.i(TAG, "last messages from queue " + check_src);
                            if(connection_status[((check_src_i - 11108) / 4) % 5] == false){
                                pq.poll();
                            }
                            else if (isalive(check_src) == false) {
                                pq.poll();
                            }
                        }
                        while ((pq.size()) > 0 ? (pq.peek().getDeliverystatus() == 1) : false) { //checking if there is any message at the head of the queue waiting to be delivered
                            pendingmsg pm3 = pq.poll();
                            publishProgress(pm3.getmsgdata());
                            Log.i(TAG, "last messages from queue getting delivered" + check_src);
                            Log.i("Msg", pm3.getmsgdata());
                            Log.i("Sequence nums", String.valueOf(pm3.seq_msg));
                            Log.i("deliverable status", String.valueOf(pm3.deliverystatus));
                        }
                        Log.e(TAG, "ServerTask Null pointer exception");
                    }
                }
//            return null;
        }

        protected void onProgressUpdate(String...strings) {

            String strReceived = strings[0].trim();


            s_key = String.valueOf(key);
            contentvalues.put("key", s_key);
            contentvalues.put("value", strReceived);
            // Log.i("key" , strReceived);

            try {
                contentResolver.insert(uri, contentvalues);
            } catch (Exception e){
                Log.e(TAG, e.toString());
            }
            key++;
            textview.append(strReceived + "\t\n");

            return;
        }
    }


    /**********************************************************************************************/

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            float f_maxseqnum = 0;
            int maxseqnum = 0;
            int k = 0;
            int j = 0;

            for (j = 0; j < 5; j++) { //multicast the message to all servers to get the sequence number proposal for a message
                if (connection_status[j]) {
                    try {
                        Socket socket = new Socket();
                        socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remote_ports[j])), 0);
                        socket.setSoTimeout(1000); //500ms socket timeout
                        String msgToSend = "reqseq" + msgs[1] + msgs[2] + msgs[0]; //msgs[1]= myport, msgs[2]=counter, deliverable status, msgs[0] = message
                        PrintWriter send_data = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader received_data = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        do {
                            send_data.println(msgToSend);
                            Log.i("Client data sent to ", String.valueOf(j));
                        } while (!received_data.readLine().equals("ack"));

                        String sugseqmsg = received_data.readLine(); //waiting for sequence number from server
                        Log.i("Server sent seq", sugseqmsg);
                        send_data.println("ack");
                        if (Integer.parseInt(sugseqmsg) >= maxseqnum) { //select the maximum of all seq no received
                            maxseqnum = Integer.parseInt(sugseqmsg);
                            k = j;
                        }
                        socket.close();
                        send_data.close();
                        received_data.close();
                    } catch (SocketTimeoutException e) {
                        Log.e(TAG, "Client Task Socket TimeoutException " + String.valueOf(j));
                        connection_status[j] = false;
                        sendfailmsg(j, msgs[1]);
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException");
                        connection_status[j] = false;
                        sendfailmsg(j, msgs[1]);
                    } catch (NullPointerException e) {
                        Log.e(TAG, "ClientTask socket NullException");
                        connection_status[j] = false;
                        sendfailmsg(j, msgs[1]);
                    }
                }
            }
            f_maxseqnum = (float) maxseqnum + (float) ((((Integer.parseInt(remote_ports[k]) - 11108) / 4) % 5) * 0.1);//adding after decimal point the server number that suggested it so as to break ties
            Log.i("maxseqnum", String.format("%06.1f", f_maxseqnum));
            j = 0;
            for (j = 0; j < 5; j++) { //multicasting final seq num of  particular message to all servers
                if (connection_status[j]) {
                    try {
                        Socket socket = new Socket();
                        socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remote_ports[j])), 0);
                        socket.setSoTimeout(1000);
                        String msgToSend = "finseq" + msgs[1] + msgs[2] + String.format("%06.1f", f_maxseqnum); //msgs[1]= myport, msgs[2]=counter, deliverable status, msgs[0] = message
                        PrintWriter send_data = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader received_data = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        do {
                            send_data.println(msgToSend);
                            Log.i("final seq sent to ", String.valueOf(j));
                        } while (!received_data.readLine().equals("ack"));
                        send_data.close();
                        received_data.close();
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        Log.e(TAG, "Client Task Socket TimeoutException" + String.valueOf(j));
                        connection_status[j] = false;
                        sendfailmsg(j, msgs[1]);
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException");
                        connection_status[j] = false;
                        sendfailmsg(j, msgs[1]);
                    } catch (NullPointerException e) {
                        Log.e(TAG, "ClientTask socket NullException");
                        connection_status[j] = false;
                        sendfailmsg(j, msgs[1]);
                    }
                }
            }
            return null;
        }
    }
}
