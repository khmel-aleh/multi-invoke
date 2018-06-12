# -*- coding: utf-8 -*-
"""
To be available to launch new task in parallel, add new dict's item to AVAILABLE_TASKS dict, where:
{'teamcity parameter':
    {'invoke_name': 'invoke task name(with collection)',
     'kwargs': 'invoke task kwargs (can be omitted)'}}
"""


import datetime
import logging
import os
import sys
import threading
import time
import traceback

from invoke import task
from invoke.executor import Executor
from logging.handlers import QueueHandler
from multiprocessing import Process, Queue

from helper import tc_log, create_output_dir

_TC_LOGGER_NAME = 'tc_logger'

AVAILABLE_TASKS = {
    'build_doc': {'invoke_name': 'build.doc'},
    'code-duplication': {'invoke_name': 'check.code-duplication'},
    'codestyle': {'invoke_name': 'check.codestyle'},
    'rf-linter': {'invoke_name': 'check.rf-linter-rules'},
    'selftest-rhel': {'invoke_name': 'check.selftest-rhel'},
    'dry_run': {'invoke_name': 'run.dry'},
}


class TaskInfo(object):
    """Class represents a launched invoke task"""

    def __init__(self, process_name, invoke_task_name, worker):
        """Initialization

        *Args:*\n
            process_name: name of run process
            invoke_task_name: invoke task to run (full path with collection name)
            worker: InvokeTaskProcess instance
        """
        self.process_name = process_name
        self.name = invoke_task_name
        # queue for delayed message records
        self.queue = []
        self.worker = worker
        self.is_run = True
        self.is_failed = False

    def dump_log(self):
        """Write redirected stdout and stderr streams of parallel invoke task to the teamcity"""
        logger = logging.getLogger(_TC_LOGGER_NAME)
        with tc_log('%s execution report' % self.name, logger):
            try:
                for _record in self.queue:
                    _record.name = logger.name
                    logger.handle(_record)
                with tc_log('STDOUT', logger):
                    for line in open(self.worker._std_streams._stdout_filename, 'r'):
                        logger.info(line.strip())
                with tc_log('STDERR', logger):
                    for line in open(self.worker._std_streams._stderr_filename, 'r'):
                        logger.info(line.strip())
            except IOError as e:
                logger.info('Failed to read file %s' % e.filename)


class TasksExecutor(list):
    """Class contains all invoke tasks and methods for working with them"""

    def __init__(self, tasks_to_run=None):
        """Initialization

        *Args:*\n
            tasks_to_run: colon separated string with tasks names
        """
        super(list, self).__init__()
        # queue where all running tasks send messages
        self._queue = Queue()
        self._msg_status = "{time} [PID:{pid}] {status}: [{name}]"
        # log listener thread
        self._log_listener = None
        self._tasks = self.parse_tasks_to_run(tasks_to_run)
        self._tc_logger = None

    def parse_tasks_to_run(self, str_to_parse=None):
        """ Parse tasks to execute

        Args:
            str_to_parse: colon separated string with tasks names.

        Returns:
            List of items(invoke_task_name, task_kwargs)
        """
        if str_to_parse is None:
            return AVAILABLE_TASKS.values()
        return [AVAILABLE_TASKS[tn] for tn in str_to_parse.split(':') if tn in AVAILABLE_TASKS]

    def prepare_tasks(self):
        """Form list of invoke tasks to execute.
        Each item is instance of InvokeTaskProcess.
        """
        for _taks in self._tasks:
            invoke_task = _taks['invoke_name']
            proc_name = invoke_task.replace('.', '_')
            t_kwargs = _taks.get('kwargs', None)
            _worker = InvokeTaskProcess(task_name=invoke_task, name=proc_name, queue=self._queue, task_kwargs=t_kwargs)
            self.append(TaskInfo(process_name=proc_name, invoke_task_name=invoke_task, worker=_worker))

    def start_tasks(self):
        """Start invoke tasks"""
        for _task_info in self:
            _task_info.worker.start()
            self.logger.info('Started task "%s"' % _task_info.name)

    def start_log_listener(self):
        """Start log listener in separated thread."""
        self._log_listener = threading.Thread(target=self.logger_listener_thread)
        self._log_listener.start()

    def stop_log_listener(self):
        """Send None value to the log listener to stop it"""
        self._queue.put(None)
        self._log_listener.join()

    def logger_listener_thread(self):
        """Polling message queue for records.
        Print messages from tc_logger immediately.
        Postpone messages from task process.
        """
        while True:
            record = self._queue.get()
            if record is None:
                break
            # Log with TeamCity logger
            if record.name == _TC_LOGGER_NAME:
                print(record.message)
                sys.stdout.flush()
            else:
                # Postpone message handle until process is over
                _queue = self.get_queue_by_proc_name(record.processName)
                _queue.append(record)

    @property
    def logger(self):
        """Configure teamcity logger.

        Returns: tc_logger
        """
        if self._tc_logger is None:
            self._tc_logger = logging.getLogger(_TC_LOGGER_NAME)
            qh = QueueHandler(queue=self._queue)
            qh.setLevel(logging.DEBUG)
            self._tc_logger.setLevel(logging.DEBUG)
            self._tc_logger.addHandler(qh)
        return self._tc_logger

    def get_queue_by_proc_name(self, name):
        """Get process message queue by it name.

        Args:
            name: process name.

        Returns:
            Process message queue
        """
        for _task in self:
            if name == _task.process_name:
                return _task.queue

    def log_process_state(self, task_info, status_msg, rc=None):
        """Log process state to the teamcity

        Args:
            task_info: name of task.
            status_msg: status message.
            rc: process exit code.
        """
        msg = self._msg_status.format(time=datetime.datetime.now(), pid=task_info.worker.pid,
                                      status=status_msg, name=task_info.process_name)
        if rc is not None:
            msg += '. Exit code: {}'.format(rc)
        self.logger.info(msg)

    @property
    def running_tasks(self):
        """Get running invoke tasks.

        Returns:
            List of running invoke tasks.
        """
        return [_task for _task in self if _task.is_run]

    @property
    def failed_tasks(self):
        """Get failed invoke tasks.

        Returns:
            List of failed invoke tasks.
        """
        return [_task.name for _task in self if _task.is_failed]

    def wait_complete(self):
        """Inquire running tasks and wait for them to complete"""
        while self.running_tasks:
            for task_info in self.running_tasks:
                rc = task_info.worker.exitcode
                if rc is None:
                    self.log_process_state(task_info, 'Still running', rc)
                    time.sleep(3)
                    continue
                elif rc == 0:
                    self.log_process_state(task_info, 'Finished successfully', rc)
                elif rc > 0:
                    self.log_process_state(task_info, 'Finished with fail result', rc)
                    task_info.is_failed = True
                elif rc < 0:
                    self.log_process_state(task_info, 'Interrupted', rc)
                    task_info.is_failed = True
                task_info.is_run = False
                task_info.dump_log()


class StdStreams(object):
    """Class with context manager interface. Redirect std streams of tasks (calls subprocess) to files.
    Create files stdout.txt and stderr.txt in task output directory.

    """

    def __init__(self, task_name):
        """Initialization

        *Args:*\n
            task_name: task name.
        """
        _artifact_path = create_output_dir(task_name)
        self._stdout_filename = os.path.join(_artifact_path, 'stdout.txt')
        self._stderr_filename = os.path.join(_artifact_path, 'stderr.txt')

    def __enter__(self):
        """Implement context manager interface.

        Returns:
            Tuple of standard output file handles and standard error file handles.

        """
        sys.stdout = self._stdout_open = open(self._stdout_filename, 'w')
        sys.stderr = self._stderr_open = open(self._stderr_filename, 'w')
        return self._stdout_open, self._stderr_open

    def __exit__(self, *args):
        """Implement context manager interface.

        Args:
            *args: context manager interface args.
        """
        self._stdout_open.close()
        self._stderr_open.close()
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__


class InvokeTaskProcess(Process):
    """Class to represent a launch of invoke task. Inherited from Process."""

    def __init__(self, task_name, queue, name, task_kwargs={}):
        """Initialization

        *Args:*\n
            task_name: task name.
            queue: message queue.
            name: process name.
            task_kwargs: parameters of invoke task.
        """
        Process.__init__(self, name=name)
        self._task_name = task_name
        self._queue = queue
        self._task_kwargs = task_kwargs if isinstance(task_kwargs, dict) else {}
        self.daemon = True
        self._root_logger = None
        self._std_streams = StdStreams(task_name=self._task_name)

    def run(self):
        """Override Process method"""
        self.configure_logging()
        from tasks import ns
        rc = 1
        try:
            with self._std_streams as (out, err):
                self._task_kwargs.update({'stdout': out, 'stderr': err})
                _result = Executor(ns).execute((self._task_name, self._task_kwargs))
            # result: {<Task 'task_to_build_doc'>: 0}
            rc = list(_result.values())[0]
        except Exception as e:
            self._root_logger.error('Task %s failed with traceback:\n' % self._task_name + traceback.format_exc())
        finally:
            exit(0) if rc is None else exit(rc)

    def configure_logging(self):
        """Configure process logger."""
        self._root_logger = logging.getLogger()
        self._root_logger.setLevel(logging.INFO)

        _qh = QueueHandler(self._queue)
        _qh.setLevel(logging.INFO)
        self._root_logger.addHandler(_qh)


@task
def quality_gate_task(ctx):
    """Invoke task for executing quality tasks in parallel.
    Get tasks to execute from "QG_TASKS" env variable.

    Args:
        ctx: invoke context;
    """
    tasks = os.getenv('QG_TASKS', None)
    _executor = TasksExecutor(tasks_to_run=tasks)

    _executor.start_log_listener()
    _executor.prepare_tasks()
    _executor.start_tasks()
    _executor.wait_complete()

    failed_tasks = _executor.failed_tasks
    _executor.stop_log_listener()

    if failed_tasks:
        raise AssertionError('Failed tasks:\n\t{}\nSee build log for details.'.format('\n\t'.join(failed_tasks)))

    exit(0)
