package io.literaft.transport.nio;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class NioEventLoop implements Runnable {
    private static final Logger log = System.getLogger(NioEventLoop.class.getName());
    private static final int MAX_TASKS_PER_ITERATION = 16;
    private final ConnectionManager connectionManager;
    private final Selector selector;
    private final Queue<Task> taskQueue;
    private final Queue<Message> inboundMessageQueue;
    private final AtomicBoolean wokenUp;
    private volatile boolean running;
    private int timeout;

    public NioEventLoop(ConnectionManager connectionManager) throws IOException {
        this.connectionManager = connectionManager;
        selector = Selector.open();
        taskQueue = new ConcurrentLinkedQueue<>();
        inboundMessageQueue = new ArrayDeque<>();
        wokenUp = new AtomicBoolean();
        running = true;
    }

    @Override 
    public void run() {
        try {
            while (running) {
                runCycle();
            }
        } finally {
            try {
                selector.close();
            } catch (IOException e) {
                log.log(Level.WARNING, "Selector close failed during shutdown", e);
            }
        }
    }

    public void execute(Task task) {
        taskQueue.offer(task);
        wakeUp();
    }

    public void shutdown() {
        running = false;
        selector.wakeup();
    }

    private void runCycle() {
        wokenUp.set(false);
        try {
            if (!taskQueue.isEmpty()) {
                selector.selectNow();
            }  else if (timeout > 0) {
                selector.select(timeout);   
            } else {
                selector.select();
            }
        } catch (IOException e) {
            log.log(Level.ERROR, "Selector select failed; event loop stopping", e);
            running = false;
        }

        processReadyKeys();
        drainTasksBounded();
    }

    private void processReadyKeys() {
        Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();

        while (keyIterator.hasNext()) {
            SelectionKey selectionKey = keyIterator.next();
            
            if (!selectionKey.isValid()) {
                keyIterator.remove();
                continue;
            }

            if (selectionKey.isAcceptable()) {
                try {
                    connectionManager.onAccept();
                } catch (RuntimeException e) {
                    log.log(Level.ERROR, "Accept handler threw unexpected error; listener kept alive", e);
                }   
            }

            try {
                if (selectionKey.isConnectable()) {
                    ((NioSocket) selectionKey.attachment()).onConnect();
                }

                if (selectionKey.isWritable()) {
                    ((NioSocket) selectionKey.attachment()).onWrite();
                }

                if (selectionKey.isReadable()) {
                    List<Message> inboundMessages = ((NioSocket) selectionKey.attachment()).onRead();
                    inboundMessageQueue.addAll(inboundMessages);
                }
            } catch (IOException e) {
                NioSocket socket = (NioSocket) selectionKey.attachment();
                log.log(Level.WARNING, "Connection lost (peer=" + socket.getPeerId() + "); closing and notifying manager", e);
                teardown(selectionKey, socket);
            } catch (RuntimeException e) {
                NioSocket socket = (NioSocket) selectionKey.attachment();
                log.log(Level.ERROR, "Connection handler bug (peer=" + socket.getPeerId() + "); closing", e);
                teardown(selectionKey, socket);
            }

            keyIterator.remove();
        }
    }

    private void drainTasksBounded() {
        int processedTasks = 0;
        while (!taskQueue.isEmpty() && processedTasks < MAX_TASKS_PER_ITERATION) {
            Task task = taskQueue.poll();
            processTask(task);
            processedTasks++;
        }
    }

    private void processTask(Task task) {
        try {
            task.run();
        } catch (Throwable t) {
            log.log(Level.ERROR, "Task threw (task=" + task + "); continuing", t);
        }
    }

    private void wakeUp() {
        if (!wokenUp.getAndSet(true)) {
            selector.wakeup();
        }
    }

    private void teardown(SelectionKey key, NioSocket socket) {
        try {
            closeQuietly(key.channel());
            key.cancel();
            connectionManager.onConnectionDied(socket);
        } catch (Throwable t) {
            log.log(Level.ERROR, "Teardown failed (peer=" + socket.getPeerId() + ")", t);
        }
    }

    private void closeQuietly(Channel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            log.log(Level.WARNING, "Channel close failed during teardown", e);
        }
    }
}
