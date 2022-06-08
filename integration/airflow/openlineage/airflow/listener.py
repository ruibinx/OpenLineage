import logging
import threading
import time
import uuid
from queue import Queue, Empty

import attr

from typing import TYPE_CHECKING, Optional, Callable

from airflow.listeners import hookimpl

from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors import ExtractorManager
from openlineage.airflow.utils import DagUtils, get_task_location, get_job_name, get_custom_facets

if TYPE_CHECKING:
    from airflow.models import TaskInstance, BaseOperator
    from sqlalchemy.orm import Session


@attr.s(frozen=True)
class ActiveRun:
    run_id: str = attr.ib()
    task: "BaseOperator" = attr.ib()


class ActiveRunManager:
    """Class that stores run data - run_id and task in-memory. This is needed because Airflow
    does not always pass all runtime info to on_task_instance_success and
    on_task_instance_failed that is needed to emit events. This is not a big problem since
    we're only running on worker - in separate process that is always spawned (or forked) on
    execution, just like old PHP runtime model.
    """

    def __init__(self):
        self.run_data = {}

    def set_active_run(self, task_instance: "TaskInstance", run_id: str):
        self.run_data[self._pk(task_instance)] = ActiveRun(run_id, task_instance.task)

    def get_active_run(self, task_instance: "TaskInstance") -> Optional[ActiveRun]:
        return self.run_data.get(self._pk(task_instance))

    @staticmethod
    def _pk(ti: "TaskInstance"):
        return ti.dag_id + ti.task_id + ti.run_id


log = logging.getLogger('airflow')


class TaskRunner:
    """
    Run extractors in a separate thread. We do this because sometimes extractors execute
    long-running tasks, such as a network call to another service (e.g., BigQuery) or execute
    sometimes long-running database queries. The task runner starts a daemon thread and manages a
    queue for tasks to be executed. While the queue is non-empty, the thread will pull tasks and
    execute them. A `running` state flag tells the thread whether to keep reading from the queue
    even if it's empty.
    An `atexit` hook is registered to set the `running` flag to false, wait until the queue is
    empty, and then wait for the thread to complete any tasks
    """

    def __init__(self):
        self.queue = Queue(maxsize=0)
        self.thread = threading.Thread(
            target=self.run,
            daemon=False
        )
        self.running = True
        self.thread.start()
        log.info("Started OpenLineage event listener thread")

    def push(self, item: Callable):
        self.queue.put(item, False)

    def run(self):
        while self.running:
            log.info("running = true")
            try:
                item: Callable = self.queue.get(True, 5)
                item()
            except Empty:
                log.info("Nothing in the queue")

    def terminate(self):
        """
        Terminate the running thread
        :return:
        """
        log.info("Tearing down OpenLineage thread - waiting for active tasks")
        time.sleep(2)
        # self.running = False

        # don't wait for the queue to be empty - a task may be hanging, meaning tasks at the end
        # of the queue may never be handled, so we'd wait forever.
        # leaving this here in case someone else thinks to add this line in the future :)
        # self.queue.join()

        # self.thread.join(timeout=5)
        log.info("OpenLineage thread torn down")


run_data_holder = ActiveRunManager()
extractor_manager = ExtractorManager()
adapter = OpenLineageAdapter()
runner = None
monitor = None


def monitor_thread():
    log.error("MONITOR THREAD STARTED DAEMON FALSE NOKILL")
    main_thread = threading.main_thread()
    main_thread.join()
    log.error("MAIN THREAD JOINED")
    # runner.terminate()


def start_threads():
    global runner
    global monitor
    runner = TaskRunner()
    log.error("MONITOR THREAD GOING TO START")
    monitor = threading.Thread(target=monitor_thread)
    monitor.daemon = False
    monitor.start()


def execute_in_thread(target: Callable, kwargs=None):
    runner.push(target)


@hookimpl
def on_task_instance_running(previous_state, task_instance: "TaskInstance", session: "Session"):
    log.info("OpenLineage listener notified about task instance start")
    start_threads()
    if not hasattr(task_instance, 'task'):
        log.warning(
            f"No task set for TI object task_id: {task_instance.task_id} - dag_id: {task_instance.dag_id} - run_id {task_instance.run_id}")  # noqa
        return

    dagrun = task_instance.dag_run
    task = task_instance.task
    dag = task_instance.task.dag

    run_id = str(uuid.uuid4())
    run_data_holder.set_active_run(task_instance, run_id)
    parent_run_id = str(uuid.uuid3(uuid.NAMESPACE_URL, f'{dag.dag_id}.{dagrun.run_id}'))

    def on_running():
        task_metadata = extractor_manager.extract_metadata(dagrun, task)

        adapter.start_task(
            run_id=run_id,
            job_name=get_job_name(task),
            job_description=dag.description,
            event_time=DagUtils.get_start_time(task_instance.start_date),
            parent_job_name=dag.dag_id,
            parent_run_id=parent_run_id,
            code_location=get_task_location(task),
            nominal_start_time=DagUtils.get_start_time(dagrun.execution_date),
            nominal_end_time=DagUtils.to_iso_8601(task_instance.end_date),
            task=task_metadata,
            run_facets={
                **task_metadata.run_facets,
                **get_custom_facets(task_instance, dagrun.external_trigger)
            }
        )

    execute_in_thread(on_running)


@hookimpl
def on_task_instance_success(previous_state, task_instance: "TaskInstance", session):
    log.info("OpenLineage listener notified about task instance success")

    run_data = run_data_holder.get_active_run(task_instance)

    dagrun = task_instance.dag_run
    task = run_data.task if run_data else None

    def on_success():
        task_metadata = extractor_manager.extract_metadata(
            dagrun, task, complete=True, task_instance=task_instance
        )
        adapter.complete_task(
            run_id=run_data.run_id,
            job_name=get_job_name(task),
            end_time=DagUtils.to_iso_8601(task_instance.end_date),
            task=task_metadata
        )

    execute_in_thread(on_success)


@hookimpl
def on_task_instance_failed(previous_state, task_instance: "TaskInstance", session):
    log.info("OpenLineage listener notified about task instance failed")

    run_data = run_data_holder.get_active_run(task_instance)

    dagrun = task_instance.dag_run
    task = run_data.task if run_data else None

    def on_failure():
        task_metadata = extractor_manager.extract_metadata(
            dagrun, task, complete=True, task_instance=task_instance
        )

        adapter.fail_task(
            run_id=run_data.run_id,
            job_name=get_job_name(task),
            end_time=DagUtils.to_iso_8601(task_instance.end_date),
            task=task_metadata
        )

    execute_in_thread(on_failure)
