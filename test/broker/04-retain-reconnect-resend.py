#!/usr/bin/env python3

# Test whether retained messages are resent when a durable client reconnects
# and reclaims its session without resubscribing. Reconnects ten times and
# checks that the same set of retained messages is received every time.

from mosq_test_helper import *


def read_packet(sock, name):
    # Read a single MQTT packet (assumes remaining length fits in one byte)
    header = mosq_test.read_bytes(sock, 1, name)
    rl = mosq_test.read_bytes(sock, 1, name)
    body = mosq_test.read_bytes(sock, rl[0], name)
    return header + rl + body


def do_test(proto_ver):
    if proto_ver == 5:
        props = mqtt5_props.gen_uint32_prop(mqtt5_props.SESSION_EXPIRY_INTERVAL, 60)
        connect_packet = mqtt_packets.gen_connect("retain-reconnect-test", proto_ver=proto_ver, clean_session=False, properties=props)
    else:
        connect_packet = mqtt_packets.gen_connect("retain-reconnect-test", proto_ver=proto_ver, clean_session=False)

    connack1_packet = mqtt_packets.gen_connack(rc=0, proto_ver=proto_ver)
    connack2_packet = mqtt_packets.gen_connack(rc=0, proto_ver=proto_ver, flags=1)

    mid = 1
    subscribe_packet = mqtt_packets.gen_subscribe(mid, "retain/reconnect/#", 0, proto_ver=proto_ver)
    suback_packet = mqtt_packets.gen_suback(mid, 0, proto_ver=proto_ver)

    retained = [
        mqtt_packets.gen_publish("retain/reconnect/one", qos=0, payload="state one", retain=True, proto_ver=proto_ver),
        mqtt_packets.gen_publish("retain/reconnect/two", qos=0, payload="state two", retain=True, proto_ver=proto_ver),
    ]
    expected = sorted(retained)

    port = mosq_test.get_port()
    broker = MosquittoBroker(port=port)

    with broker:
        # Durable client subscribes, then goes away
        sock = mosq_test.do_client_connect(connect_packet, connack1_packet, timeout=20, port=port)
        mosq_test.do_send_receive(sock, subscribe_packet, suback_packet, "suback")
        sock.close()

        # Retained state is published while the subscriber is offline
        pub_sock = mosq_test.do_client_connect(
            mqtt_packets.gen_connect("retain-reconnect-pub", proto_ver=proto_ver),
            mqtt_packets.gen_connack(rc=0, proto_ver=proto_ver), timeout=20, port=port)
        for packet in retained:
            pub_sock.send(packet)
        pub_sock.close()

        # Reconnect ten times without resubscribing, the same retained
        # messages must be resent every time.
        for i in range(10):
            sock = mosq_test.do_client_connect(connect_packet, connack2_packet, timeout=20, port=port, connack_error=f"connack {i}")
            received = sorted(read_packet(sock, f"retained {i}") for _ in retained)
            if received != expected:
                raise ValueError(f"reconnect {i}: received {received!r}, expected {expected!r}")
            sock.close()


if __name__ == '__main__':
    do_test(proto_ver=4)
    do_test(proto_ver=5)
