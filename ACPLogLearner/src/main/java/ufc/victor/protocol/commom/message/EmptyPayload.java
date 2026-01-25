package ufc.victor.protocol.commom.message;

 public final class EmptyPayload implements MessagePayload {

    public static final EmptyPayload INSTANCE = new EmptyPayload();

    private EmptyPayload() {}
}
