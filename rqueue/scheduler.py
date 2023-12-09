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
			job = queue.enqueue(func, func_input, meta=metadata)

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
	job = enqueue_process(func, func_input, log_filepath, queue, queue_type)
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
	conn, q = rq.start_redis_queue(log_filepath)
	job = schedule_process_job(func, func_input, job_name, task_name, start, interval_duration, weekdays, execution_frequency, execution_type, q, queue_type)
	process = Process(target=run_worker, kwargs=dict(conn=conn, q=q))
	process.start()
	return(q, process, job)

def submit_job(func, func_input, job_name:str, task_name:str, start:datetime, interval_duration:timedelta, weekdays:Optional[List[str]], execution_type:str, task_id:int, sql_engine:engine, queue_type:str="rq", execution_frequency:str="Once"):
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
	return(start_scheduler_process(func, func_input, job_name, task_name, start, interval_duration, weekdays, execution_frequency, execution_type, queue_type))
	

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

def save_df_to_sql(df: pd.DataFrame, sql_engine: engine) -> None:
	"""
	Save dataframe with process information to a local sql alchemy DB file.
	Args:
		df: process information df.
		sql_engine:  sql alchemy engine to use.

	Raises:
		OperationalError: if any sqlalchemy errors have been thrown.
	"""
	try:
		df.to_sql("processes", con=sql_engine, if_exists="append", index=False)
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