"""Utils file.

This file is to house code common between the Master and the Worker

"""

import json
import socket


def handle_msg(sock):
    """Handle messages received by socket."""
    try:
        clientsocket, _ = sock.accept()
    except socket.timeout:
        return None
    message_chunks = []
    while True:
        try:
            data = clientsocket.recv(4096)
        except socket.timeout:
            continue
        if not data:
            break
        message_chunks.append(data)
    clientsocket.close()
    # Decode list-of-byte-strings to UTF8 and parse JSON data
    message_bytes = b''.join(message_chunks)
    message_str = message_bytes.decode("utf-8")
    try:
        message_dict = json.loads(message_str)
    except json.JSONDecodeError:
        return None
    return message_dict
