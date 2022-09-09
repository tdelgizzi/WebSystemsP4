"""Master implementation of mapreduce."""

import os
import logging
import json
import time
import socket
import pathlib
import threading
import heapq
import shutil
import click
import mapreduce.utils


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Master:
    """Class definition for Master."""

    job_queue = []
    task_queue = []
    active_tasks = []
    live_workers = []
    worker_dict = {}
    current_job = None
    signals = {}
    status = {}

    def __init__(self, port):
        """Initialize Master server."""
        self.signals["shutdown"] = False
        logging.info("Starting master:%s", port)
        logging.info("Master:%s PWD %s", port, os.getcwd())
        self.status = {
            "state": "start",
            "job_count": 0,
            "port": port
        }
        self.current_job = None
        self.job_queue = []
        self.task_queue = []
        self.active_tasks = []
        self.live_workers = []
        self.worker_dict = {}
        # create tmp folder
        path_val = pathlib.Path("tmp/")
        path_val.mkdir(parents=True, exist_ok=True)
        # delete any old mapreduce job folders in tmp
        file_list = path_val.glob("job-*")
        for old_file in file_list:
            shutil.rmtree(old_file)
        udp_server = threading.Thread(target=self.heartbeat_listener)
        udp_server.start()
        heartbeat_thread = threading.Thread(target=self.heartbeat_monitor)
        heartbeat_thread.start()
        tcp_server = threading.Thread(target=self.server)
        tcp_server.start()
        # wait to return until all master threads have exited
        tcp_server.join()
        heartbeat_thread.join()
        udp_server.join()

    def heartbeat_monitor(self):
        """Host thread for monitoring the heartbeat of a worker."""
        while not self.signals['shutdown']:
            time.sleep(2)
            for worker in self.live_workers:
                worker['num_pings'] += 1
                if worker['num_pings'] >= 5:
                    worker['state'] = 'dead'
                    worker_id = worker['pid']
                    task = self.remove_active_task(worker_id)
                    self.__handle_idle_task(task)

    def heartbeat_listener(self):
        """Handle UDP thread for worker heartbeats."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Bind the UDP socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.status['port'] - 1))
        sock.settimeout(1)
        while not self.signals["shutdown"]:
            time.sleep(0.1)
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)
            # process heartbeat
            worker = None
            if self.worker_dict:
                worker = self.worker_dict[message_dict['worker_pid']]
            if worker:
                worker['num_pings'] = 0

    def server(self):
        """Create TCP socket at port and call listen() function."""
        # Create an INET, STREAMing socket, this is TCP
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.status['port']))
        sock.listen()
        sock.settimeout(1)
        # wait for incoming messages
        while not self.signals['shutdown']:
            time.sleep(0.1)
            message_dit = mapreduce.utils.handle_msg(sock)
            if not message_dit:
                continue
            logging.debug(
                "Master:%s received\n%s",
                self.status['port'],
                json.dumps(message_dit, indent=2),
            )
            message_type = message_dit['message_type']
            if message_type == "shutdown":
                self.signals['shutdown'] = True
                self.send_shutdown_message()
            elif message_type == "register":
                self.handle_registration(message_dit)
            elif message_type == "new_master_job":
                self.handle_new_job(message_dit)
            elif (message_type == "status" and
                  message_dit["status"] == "finished"):
                self.handle_status(message_dit)
        sock.close()

    def __handle_idle_task(self, task):
        """Handle a task that was being handled by a now-dead worker."""
        # remove task from active queue
        # find available worker
        found = False
        for worker in self.live_workers:
            if worker['state'] == 'ready':
                self.assign_worker_task(worker, task)
                found = True
        if task and not found:
            # put it back on task_queue
            self.task_queue.append(task)

    def handle_registration(self, message_dict):
        """Handle registration of a new worker."""
        new_worker = {
            "state": "ready",
            "host": message_dict['worker_host'],
            "port": message_dict['worker_port'],
            "pid": message_dict['worker_pid'],
            "num_pings": 0
        }
        self.live_workers.append(new_worker)
        self.worker_dict[message_dict['worker_pid']] = new_worker
        # send message back to worker
        reply = {
            "message_type": "register_ack",
            "worker_host": message_dict['worker_host'],
            "worker_port": message_dict['worker_port'],
            "worker_pid": message_dict['worker_pid']
        }
        send_message(new_worker, json.dumps(reply))
        # check task/job queues,
        # if task_queue is not empty, give worker next task
        # else, if job queue is not empty and not is_running,
        # start mapping stage of next job
        if len(self.task_queue) > 0:
            next_task = self.task_queue.pop(0)
            self.assign_worker_task(new_worker, next_task)
        elif not self.current_job and len(self.job_queue) > 0:
            self.current_job = self.job_queue.pop(0)
            self.begin_mapping_stage()

    def handle_new_job(self, message_dict):
        """Handle a message creating a new job."""
        self.create_new_job()
        message_dict['job_id'] = self.status['job_count']
        self.status['job_count'] += 1
        # if server is busy or no available workers
        # the job is added to queue
        if not self.ready_workers() or self.current_job:
            self.job_queue.append(message_dict)
        else:
            # if there are workers and the server is not busy
            # the Master can begin job execution
            # log messages
            self.current_job = message_dict
            self.begin_mapping_stage()

    def handle_status(self, message_dict):
        """Handle messages marked as 'status' from Worker."""
        # check if there are tasks left
        # if there are, assign next
        # task to worker with worker_pid
        # if not, end map stage
        if self.worker_dict:
            worker = self.worker_dict[message_dict['worker_pid']]
        worker['state'] = 'ready'
        self.remove_active_task(message_dict['worker_pid'])
        if len(self.task_queue) == 0 and len(self.active_tasks) == 0:
            if self.status['state'] == "mapping":
                logging.info("Master:%s end map stage", self.status['port'])
                # start grouping stage
                self.begin_grouping_stage()
            elif self.status['state'] == "grouping":
                outps = (pathlib.Path("tmp/job-" +
                         str(self.current_job['job_id'])) / "grouper-output")
                # handle sorted files
                self.handle_sorted_files(outps)
                logging.info("Master:%s end group stage", self.status['port'])
                # start reduce stage
                self.begin_reduce_stage()
            elif self.status['state'] == "reducing":
                logging.info("Master:%s end reduce stage", self.status['port'])
                # wrap up job
                self.wrap_up()
                logging.info("Master:%s finished job. Output directory: %s",
                             self.status['port'],
                             self.current_job['output_directory'])
                # check job queue for next available job
                self.find_next_job()

        elif len(self.task_queue) > 0:
            self.assign_worker_task(worker, self.task_queue.pop(0))

    def wrap_up(self):
        """Move and rename final outputs as instructed."""
        output_directory = pathlib.Path(self.current_job['output_directory'])
        path_val = pathlib.Path(output_directory)
        path_val.mkdir(parents=True, exist_ok=True)
        final_outputs = (pathlib.Path("tmp/job-" +
                         str(self.current_job['job_id'])) / "reducer-output")
        files = os.listdir(final_outputs)
        for file_val in files:
            number_string = str(file_val)[-2:]
            os.rename(final_outputs / file_val,
                      output_directory / ("outputfile" + number_string))

    def find_next_job(self):
        """Look at job queue for next job, start it if one exists."""
        if len(self.job_queue) == 0:
            self.current_job = None
        else:
            # pull next job from queue and start it
            self.current_job = self.job_queue.pop(0)
            self.begin_mapping_stage()

    def find_active_task(self, worker_id):
        """Locate active task in self.active_task, and return index."""
        index = 0
        found = False
        for task in self.active_tasks:
            if task['worker_pid'] == worker_id:
                found = True
                break
            index += 1
        if found:
            return index
        return -1

    def remove_active_task(self, worker_id):
        """Locate active task in self.active_task_queue and remove it."""
        index = self.find_active_task(worker_id)
        if index != -1:
            return self.active_tasks.pop(index)
        return None

    def send_shutdown_message(self):
        """Handle shutdown message to all workers."""
        for worker in self.live_workers:
            # send message to worker
            if worker['state'] != 'dead':
                message = json.dumps({"message_type": "shutdown"})
                send_message(worker, message)

    def create_new_job(self):
        """Create new directory for job."""
        path_val = pathlib.Path("tmp/job-" + str(self.status['job_count']))
        path_val.mkdir(parents=True, exist_ok=True)
        # create new subdirectories
        sub_directories = ["mapper-output", "grouper-output", "reducer-output"]
        for sub_folder in sub_directories:
            new_p = path_val / sub_folder
            new_p.mkdir(parents=True)

    def ready_workers(self):
        """Iterate live_workers and return number of ready workers."""
        count = 0
        for worker in self.live_workers:
            if worker['state'] == 'ready':
                count += 1
        return count

    def divvy_tasks(self):
        """Round robin assign tasks in task_queue."""
        # assign tasks to workers
        i = 0
        while i < len(self.live_workers) and len(self.task_queue) > 0:
            cur_worker = self.live_workers[i]
            if cur_worker['state'] == 'ready':
                # pop front of queue
                task = self.task_queue.pop(0)
                self.active_tasks.append(task)
                # add cur_worker pid to task
                task['worker_pid'] = cur_worker['pid']
                # send them the task
                send_message(cur_worker, json.dumps(task))
                # change worker status
                cur_worker['state'] = 'busy'
            # increment
            i += 1

    def begin_mapping_stage(self):
        """Start the mapping stage of the job."""
        logging.info("Master:%s begin map stage", self.status['port'])
        # partition input directories
        partitions = partition_input(self.current_job['input_directory'],
                                     self.current_job['num_mappers'])
        mapper_output = (pathlib.Path("tmp/job-" +
                         str(self.current_job['job_id'])) /
                         "mapper-output")
        output_directory = pathlib.Path(self.current_job['output_directory'])
        path_val = pathlib.Path(output_directory)
        path_val.mkdir(parents=True, exist_ok=True)
        self.status['state'] = "mapping"
        # add tasks to task_queue from partition
        for inputs in partitions:
            new_task = {
                "message_type": "new_worker_task",
                "input_files": inputs,
                "executable": self.current_job['mapper_executable'],
                "output_directory": str(mapper_output)
            }
            self.task_queue.append(new_task)
        # assign tasks to workers
        self.divvy_tasks()

    def begin_grouping_stage(self):
        """Start the grouping stage of the job."""
        logging.info("Master:%s begin group stage", self.status['port'])
        self.status['state'] = "grouping"
        # Assign workers to sort files
        # - will be round robin assignment,
        # there may be more workers than files
        # partition mapping outputs
        input_directory = (pathlib.Path("tmp/job-" +
                           str(self.current_job['job_id'])) / "mapper-output")
        num_reducers = self.ready_workers()
        partitions = partition_input(input_directory, num_reducers)
        grouper_output = (pathlib.Path("tmp/job-" +
                          str(self.current_job['job_id'])) /
                          "grouper-output" / "sorted")
        # add tasks to task_queue from partition
        index = 1
        for inputs in partitions:
            name_num = str(index)
            if index < 10:
                name_num = "0" + str(index)
            new_task = {
                "message_type": "new_sort_task",
                "input_files": inputs,
                "output_file": str(grouper_output) + name_num
            }
            self.task_queue.append(new_task)
            index += 1
        # assign tasks to workers
        self.divvy_tasks()

    def begin_reduce_stage(self):
        """Create reducer tasks and give them to ready workers."""
        logging.info("Master:%s begin reduce stage", self.status['port'])
        self.status['state'] = "reducing"
        num_reducers = self.current_job['num_reducers']
        # partition input directories
        reducer_input = (pathlib.Path("tmp/job-" +
                         str(self.current_job['job_id'])) / "grouper-output")
        reducer_output = (pathlib.Path("tmp/job-" +
                          str(self.current_job['job_id'])) / "reducer-output")
        index = 0
        # adapted partitioning, not every file in reducer_input is relevant
        partitions = []
        for f_val in reducer_input.glob("reduce*"):
            if index < num_reducers:
                partitions.append([str(f_val)])
            else:
                partitions[index % num_reducers].append(str(f_val))
            index += 1
        # add tasks to task_queue from partition
        for inputs in partitions:
            new_task = {
                "message_type": "new_worker_task",
                "executable": self.current_job['reducer_executable'],
                "input_files": inputs,
                "output_directory": str(reducer_output)
            }
            self.task_queue.append(new_task)
        # assign tasks to workers
        self.divvy_tasks()

    def assign_worker_task(self, worker, task):
        """Add to task dict and call send_message."""
        if worker and task:
            task['worker_pid'] = worker['pid']
            self.active_tasks.append(task)
            send_message(worker, json.dumps(task))
            worker['state'] = 'busy'

    def handle_sorted_files(self, sorted_files_dir):
        """Aggregate keys from sorted files into num_reducers files."""
        # access sorted_files_dir
        opened_files = []
        writer_files = []
        for fil in pathlib.Path(sorted_files_dir).glob('sorted*'):
            opened_files.append(open(str(fil), "r"))
        num_reducers = self.current_job['num_reducers']
        for num in range(1, num_reducers + 1):
            num_string = str(num)
            if num < 10:
                num_string = "0" + str(num)
            writer_files.append(open(pathlib.Path(sorted_files_dir) /
                                                 ("reduce" + num_string), 'a'))
        last_val = "-----NA-----"
        num_unique_keys = -1
        for row in heapq.merge(*opened_files):
            line = str(row)
            if line != last_val:
                num_unique_keys += 1
                last_val = line
            # add to file
            writer_files[num_unique_keys % num_reducers].write(line)
        for writer in writer_files:
            writer.close()


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    """Call the master class."""
    Master(port)


def send_message(worker, message):
    """Send a message to worker."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((worker['host'], worker['port']))
    sock.sendall(message.encode('utf-8'))
    sock.close()


def partition_input(input_directory, partition_num):
    """Return partitioned inputs according to round robin."""
    # look at list of files in input_directory
    directory = pathlib.Path(input_directory)
    directory_files = os.listdir(directory)
    directory_files.sort()
    index = 0
    partitions = []
    for f_val in directory_files:
        if index < partition_num:
            partitions.append([str(directory / f_val)])
        else:
            partitions[index % partition_num].append(str(directory / f_val))
        index += 1
    return partitions


if __name__ == '__main__':
    main()
