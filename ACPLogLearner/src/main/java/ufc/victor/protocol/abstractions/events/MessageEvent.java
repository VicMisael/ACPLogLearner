package ufc.victor.protocol.abstractions.events;

import ufc.victor.protocol.commom.message.Message;

// Event Type 1: A packet arrived from the network
public record MessageEvent(Message message) implements Event {}