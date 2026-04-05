package ufc.victor.localenv.actors;


import ufc.victor.protocol.abstractions.IMessageHandler;
import ufc.victor.protocol.abstractions.IProtocol;
import ufc.victor.protocol.abstractions.events.ControlEvent;
import ufc.victor.protocol.abstractions.events.Event;
import ufc.victor.protocol.abstractions.events.MessageEvent;
import ufc.victor.protocol.abstractions.events.TimeoutEvent;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ActorNode implements IMessageHandler,Runnable {

    private final NodeId id;

    private final BlockingQueue<Event> mailbox = new LinkedBlockingQueue<>();

    private final Thread workerThread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private IProtocol protocolLogic;

    public ActorNode(NodeId id) {
        this.id = id;
        this.workerThread = new Thread(this, "Actor-" + id.value());
    }

    public Thread getWorkerThread() {
        return workerThread;
    }

    public void onMessage(Message msg) {
        mailbox.offer(new MessageEvent(msg));
    }

    // Called by Timer (converts to Event)
    public void onEvent(Event event) {
        mailbox.offer(event);
    }

    // Called by Simulation Main
    public void start() {
        if (running.compareAndSet(false, true)) {
            workerThread.start();
        }
    }

    public void stop() {
        mailbox.offer(new ControlEvent(ControlEvent.Type.SHUTDOWN));
    }

    public void crash() {
        running.set(false);
        workerThread.interrupt();
    }

    public void setProtocolLogic(IProtocol logic) {
        this.protocolLogic = logic;
    }

    // ----------------------------------------------------------
    // THE PROCESSING LOOP (Single Threaded)
    // ----------------------------------------------------------
    @Override
    public void run() {
        System.out.println("Node " + id + " online.");

        while (running.get()) {
            try {
                // 1. Wait for ANY event (Message or Timeout)
                Event event = mailbox.take();

                // 2. Process it sequentially
                process(event);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (running.get()) {
                    System.err.println("Node " + id + " crashed on event: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Node " + id + " offline.");
    }

    // The "Switch" logic from Valduriez Algorithm
    private void process(Event event) {
        if (event instanceof MessageEvent me) {
            if (protocolLogic != null) {
                protocolLogic.onMessage(me.message());
            }
        }
        else if (event instanceof TimeoutEvent te) {
            protocolLogic.onTimeout();
        }
        else if (event instanceof ControlEvent ce) {
            if (ce.type() == ControlEvent.Type.SHUTDOWN) {
                running.set(false);
            }
        }
    }

    public NodeId getId() { return id; }
}
