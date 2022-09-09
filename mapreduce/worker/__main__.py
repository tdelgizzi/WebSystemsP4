"""Worker server class for mapreduce."""
import os
import logging
import json
import time
import pathlib
import socket
import subprocess
import threading
import click
import mapreduce.utils

# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    """Class definition for Worker."""

    master_port = -1
    pid = -1
    port = -1
    signals = {}

    def __init__(self, master_port, worker_port):
        """Initialize a Worker object."""
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())
        # Get process id of worker
        self.pid = os.getpid()
        self.master_port = master_port
        self.port = worker_port
        self.signals["shutdown"] = False
        # Create a new TCP socket
        tcp_socket = threading.Thread(target=self.tcp_server)
        tcp_socket.start()
        time.sleep(1)
        tcp_socket.join()

    def tcp_server(self):
        """Run TCP server for receiving messages from master."""
        is_registered = False
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.port))
        sock.listen()
        # Send the register message to the Master
        self.send_register()
        sock.settimeout(1)
        heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        while not self.signals["shutdown"]:
            time.sleep(0.1)
            message_dict = mapreduce.utils.handle_msg(sock)
            if not message_dict:
                continue
            message_type = message_dict['message_type']
            logging.debug(
                "Worker:%s received\n%s",
                self.port,
                json.dumps(message_dict, indent=2),
            )
            if message_type == 'register_ack' and not is_registered:
                # create new thread responsible for sending heartbeat messages
                is_registered = True
                heartbeat_thread.start()
            elif message_type == 'shutdown':
                self.signals['shutdown'] = True
                if is_registered:
                    heartbeat_thread.join()
            elif message_type == 'new_worker_task':
                self.complete_task(message_dict)
            elif message_type == 'new_sort_task':
                self.sort_files(message_dict)

    def complete_task(self, task_info):
        """Execute task for mapping and reducing stage."""
        input_files = task_info['input_files']
        executable = task_info['executable']
        output_directory = pathlib.Path(task_info['output_directory'])
        output_files = []
        for f_name in input_files:
            filename = f_name.split('/').pop()
            output = str(output_directory / filename)
            out_file = open(output, "w+")
            i = open(f_name, "r")
            subprocess.run([executable], stdin=i, stdout=out_file, check=True)
            output_files.append(output)
        message = {
            "message_type": "status",
            "output_files": output_files,
            "status": "finished",
            "worker_pid": self.pid
        }
        self.send_message(json.dumps(message))

    def sort_files(self, task_info):
        """Sort files for grouping stage."""
        input_files = task_info['input_files']
        output_file = task_info['output_file']
        values = []
        for input_val in input_files:
            f_val = open(input_val, "r")
            for line in f_val:
                values.append(line)
        values.sort()
        with open(output_file, "w+") as out:
            for row in values:
                out.write(row)
        message = {
            "message_type": "status",
            "output_file": output_file,
            "status": "finished",
            "worker_pid": self.pid
        }
        self.send_message(json.dumps(message))

    def send_message(self, message):
        """Send message to Master."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", self.master_port))
        sock.sendall(message.encode('utf-8'))
        sock.close()

    def send_register(self):
        """Create and send registration message for master."""
        message = json.dumps({"message_type": "register",
                              "worker_host": "localhost",
                              "worker_port": self.port,
                              "worker_pid": self.pid})
        self.send_message(message)

    def send_heartbeat(self):
        """Send heartbeat to master every 2 seconds."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("localhost", self.master_port - 1))
        while not self.signals["shutdown"]:
            time.sleep(2)
            message = json.dumps({"message_type": "heartbeat",
                                  "worker_pid": self.pid})
            sock.sendall(message.encode('utf-8'))
            # stall for two seconds before sending next message
            sock.settimeout(2)
        sock.close()


@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    """Call the worker class."""
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()
