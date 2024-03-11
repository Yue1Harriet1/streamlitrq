import rq
from datetime import datetime, timedelta
import settings
from sqlalchemy import engine
import pandas as pd
from typing import(
	Callable,
	List,
	Optional,
	Tuple
)
from . import rqu
from . import settings
from . import task
from multiprocessing import Process
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine
import psutil
import os



def enqueue_process(func, func_input, task_name:str, log_filepath:str, queue, queue_type:str="rq"):
	"""
	enqueue the function process and save 'out' and 'error' logs to respective log file.

	Args:
		func: function to be executed
		func_input: parameters the above function needs
		log_file: path to the respective log file.
		queue_type: the type of task queue that the function is joining for execution

	Raises:
		OError if log file cannot be created

	Returns:
		a job object of the particular queue.
	"""

	metadata = {'progress': 0}
	try:
		if queue_type == "rq": 
			job = queue.enqueue(task.print_numbers, 5, meta=metadata)

	except OSError as exc:
		raise exc

	return(job)


def execute_job(func, func_input, log_filepath:str, job_name:str, task_name:str, now:datetime, queue, queue_type:str="rq"):
	"""
	Interface for running a job:
		-> launch a process
		-> write job execution log.

	Args:
		func: function to be executed
		func_input: parameters the above function needs
		log_file: path to the respective log file.
		job_name: name generated for the job.
		now: datetime.now()
		queue_type: the type of task queue that the function is joining for execution

	Returns:
		a job object of the particular queue.
	"""
	job = enqueue_process(func, func_input, task_name, log_filepath, queue, queue_type)
	return(job)


def schedule_process_job(func, func_input, job_name:str, task_name:str, start:datetime, interval_duration:timedelta, weekdays:Optional[List[str]], execution_frequency:str, execution_type:str, queue, queue_type:str="rq"):
	"""
	Launch a scheduler process that spawns job execution processes if launch conditions are met.
	Checks for current date. If date criterion is met -> start the process with command execution.

	Args:
		func: function to be executed
		func_input: parameters the above function needs
		job_name: name generated for the job.
		start: execution datetime.
		interval_duration: interval to wait before before scheduling the next job execution.
		weekdays: (optional) list with selected weekdays.
		execution_frequency: frequency of execution: "Interval" / "Daily"
		execution_type: type of execution schedule: is execution "Scheduled" or not.
		queue_type: the type of task queue that the function is joining for execution

	"""
	stdout_log_file = f"{settings.BASE_LOG_DIR}/{job_name}_stdout.txt"
	if execution_frequency == "Once":
		job = execute_job(func=func, func_input=func_input, log_filepath=stdout_log_file, job_name=job_name, task_name=task_name, now=datetime.now(), queue=queue, queue_type=queue_type)
		return(job)

	# If process must be executed now, decrease start date by interval timedelta:
	# this way 'match_duration' will return True in the 'process_should_execute' check.
	if execution_frequency == "Now":
		start -= interval_duration

	while True:
		now = datetime.now()
		if process_should_execute(now, start, interval_duration, weekdays):
			job = execute_job(func=func, func_input=func_input, log_filepath=stdout_log_file, job_name=job_name, task_name=task_name, now=datetime.now(), queue=queue, queue_type=queue_type)
			start += interval_duration
		else:
			time.sleep(1)	


def start_scheduler_process(func, func_input, job_name:str, task_name:str, start: datetime, interval_duration:timedelta, weekdays: Optional[List[str]], execution_frequency: str, execution_type:str, queue_type:str="rq") -> (int, int):
	"""
	Run a process with the selected parameters.

	Args:
		func: function to be executed
		func_input: parameters the above function needs
		job_name: name generated for the job.
		start: execution datetime.
		interval_duration: interval to wait before before scheduling the next job execution.
		weekdays: (optional) list with selected weekdays.
		execution_frequency: frequency of execution: "Interval" / "Daily"
		execution_type: type of execution schedule: is execution "Scheduled" or not.
		queue_type: the type of task queue that the function is joining for execution

	Returns:
		ID of the started process and the job		

	"""
	log_filepath = f"{settings.BASE_LOG_DIR}/{job_name}_redis_server_log.txt"
	conn, q = rqu.start_redis_queue(log_filepath)
	job = schedule_process_job(func, func_input, job_name, task_name, start, interval_duration, weekdays, execution_frequency, execution_type, q, queue_type)
	process = Process(target=rqu.run_worker, kwargs=dict(conn=conn, q=q))
	process.start()
	return(q, process, job)

def submit_job(func, func_input, job_name:str, task_name:str, start:datetime, interval_duration:timedelta, weekdays:Optional[List[str]], execution_type:str, task_id:int, db_engine:engine, queue_type:str="rq", execution_frequency:str="Once"):
	"""
	Run a process job and save related process information to an SQL alchemy file.

	Args:
		func: function to be executed
		func_input: parameters the above function needs
		job_name: name generated for the job.
		start: execution datetime.
		interval_duration: interval to wait before before scheduling the next job execution.
		weekdays: (optional) list with selected weekdays.
		execution_frequency: frequency of execution: "Interval" / "Daily"
		execution_type: type of execution schedule: is execution "Scheduled" or not.
		task_id: task ID
		sql_engine: sql engine to use for saving DF information to sql.
		queue_type: the type of task queue that the function is joining for execution

	"""
	q, process, job = start_scheduler_process(func, func_input, job_name, task_name, start, interval_duration, weekdays, execution_frequency, execution_type, queue_type)
	process_df = create_process_info_dataframe(func, func_input, job_name, process.pid, task_id)
	save_df_to_sql(process_df, db_engine)
	return(q, process, job)
	

def create_process_info_dataframe(func, func_input, job_name: str, pid: int, task_id: int) -> pd.DataFrame:
	"""
	Generate a dataframe with process information in the following format:

	{
		'task_id': [],
		'created': [],
		'process id': [],
		'job name': [],
		'task': [],
		'last update': [],
		'running': []
	}

	Args:
		func: function to be executed
		func_input: parameters the above function needs
		job_name: name generated for the job.
		pid: process ID.
		task_id: task ID.

	Returns:
		pandas DF with process related information.
	"""
	created = datetime.now()
	return pd.DataFrame(
		{
			'task_id': [task_id],
			'created': [created],
			'process id': [pid],
			'job name': [job_name],
			'task': [func.__str__()],
			'last update': [None],
			'running': [None]
		}
	)

def save_df_to_sql(df: pd.DataFrame, db_engine: engine) -> None:
	"""
	Save dataframe with process information to a local sql alchemy DB file.
	Args:
		df: process information df.
		sql_engine:  sql alchemy engine to use.

	Raises:
		OperationalError: if any sqlalchemy errors have been thrown.
	"""
	try:
		df.to_sql("processes", con=db_engine, if_exists="append", index=False)
	except OperationalError as exc:
		raise exc

def read_log(filename: str) -> List[str]:
	"""
	Utility function to read a logfile.

	Args:
		filename: name of the log file to be read.
	Raises:
		FileNotFoundError if respective log file is missing.

	Returns:
		list of strings, with each string representing a line.
	"""
	try:
		with open(filename, "r", encoding="utf-8") as reader:
			return reader.readlines()
	except FileNotFoundError as exc:
		raise exc

def write_job_execution_log(job_name: str, command:str, now: datetime, msg: str) -> None:
	"""
	Save job execution information to a log file.
	Args:
		func: function to be executed
		func_input: parameters the above function needs
		job_name: name of the job for which to write the log.
		now: datetime object with current timestamp.
		msg: message to be logged.

	Raises:
		OSError if log file creation fails.
	"""
	now_str = now.strftime(settings.DATE_FORMAT)
	for suffix in [".txt", "_stdout.txt"]:
		try:
			with open(f"{settings.BASE_LOG_DIR}/{job_name}{suffix}", "a") as file:
				if suffix == "_stdout.txt" and command != "":
					file.write(f"\n{'=' * 70} \n")
				file.write(f"{now_str} {msg} {command}\n")
		except OSError as exc:
			raise exc

def process_should_execute(now: datetime,
                           start: datetime,
                           duration: timedelta,
                           weekdays: Optional[List[str]]) -> bool:
    """
    Determine whether the process should execute or not:
        -> is it the correct day of the week?
        -> is it the correct scheduled interval?

    Args:
        now: datetime.now()
        start: datetime object with process start date.
        duration: interval timedelta to check whether schedule has been met.
        weekdays: optional list with selected weekdays.

    Returns:
        True/False based on the result of the check.
    """
    return match_weekday(now, weekdays) and match_duration(now, start, duration)




def match_weekday(now: datetime,
                  weekdays: Optional[List[str]]) -> bool:
    """
    Determine if 'today' is the day when a function must be executed.

    Args:
        now: datetime object representing current timestamp.
        weekdays: optional list of ints from 0 to 6 corresponding to different days of the week,
            e.g. 0 for Monday, etc.

    Returns:
        True/False based on the result of the check.
    """
    today = now.weekday()

    try:
        if not weekdays or settings.WEEK_DAYS[today] in weekdays:
            return True
    except KeyError as exc:
        raise exc

    return False



def match_duration(now: datetime, start: datetime, duration: timedelta) -> bool:
    """
    Check whether the sum of process start date and interval timedelta is less
    than current datetime. If yes -> process must be executed.

    Args:
        now: datetime.now().
        start: datetime object with process start date.
        duration: interval timedelta to check whether schedule has been met.

    Returns:
        True/False based on the result of the check.
    """
    return now > (start + duration)


def get_process_df(sql_engine: engine) -> pd.DataFrame:
    """
    Check for and initialize process info dataframe has already been created.
    If process dataframe already exists, filter out dead and 'zombie' processes to only
    show accurate process information.

    Returns:
        either an existing process dataframe or an empty one,
            following the settings format.
    """
    try:
        df = pd.read_sql_table("processes", con=sql_engine)
    except ValueError:
        df = pd.DataFrame(settings.FORMAT)
    except OperationalError as exc:
        raise exc

    return df


def update_process_status_info(df: pd.DataFrame) -> None:
    """
    If process dataframe already exists, filter out dead and 'zombie' processes to only
    show accurate process information.

    Args:
        df: df with process information.
    """
    df["running"] = df["process id"].apply(
        lambda x: psutil.pid_exists(x) and psutil.Process(x).status() == "running")


def update_df_process_last_update_info(df: pd.DataFrame) -> None:
    """
    Iterate over processes in process df and update respective process 'last update' values.

    Args:
        df: df with process information.
    """
    df["last update"] = df["job name"].apply(lambda x: check_last_process_info_update(x) if x else "")


def refresh_app(to_wait: int = 0) -> None:
    """
    (Optionally) wait for a given amount of time (in seconds)
    and trigger Streamlit app refresh.

    Args:
        to_wait: integer indicating amount of seconds to wait.

    Raises:
        RerunException that stops and re-runs the app script.
    """
    if to_wait:
        empty_slot = st.empty()

        for i in range(to_wait):
            empty_slot.write(f"Refreshing in {to_wait - i} seconds...")
            time.sleep(1)

    raise st.script_runner.RerunException(st.script_request_queue.RerunData())


def check_last_process_info_update(job_name: str) -> Optional[datetime]:
    """
    Use 'last modified' timestamp of the job log file to check
    when job and related process information has been updated last.

    Args:
        job_name: name of the job for which to perform the check.

    Returns:
        datetime: last modified timestamp.
    """
    filename = f"{settings.BASE_LOG_DIR}/{job_name}.txt"

    try:
        return datetime.fromtimestamp(os.path.getmtime(filename))
    except OSError:
        return None