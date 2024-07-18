import redis
from redis import Redis
from rq import Queue, Worker, Connection
from multiprocessing import Process
from subprocess import Popen
import pandas as pd
from sqlalchemy import engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine
from rq.exceptions import NoSuchJobError
from rq.job import Job
from datetime import datetime, timedelta


def launch_command_process(command: str, log_filepath:str) -> Popen:
	try:
		with open(log_filepath, "w") as out:
			return Popen(command.split(" "), stdout=out, stderr=out)
	except OSError as exc:
		raise exc

def start_redis_server(log_filepath: str):
	launch_command_process("src/redis-server", log_filepath)


def start_redis_queue(log_filepath: str):
	#start_redis_server(log_filepath)
	redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
	conn = redis.from_url(redis_url)
	q = Queue(connection=conn)
	return(conn, q)

def run_worker(conn: Connection, q: Queue):
	listen=['high', 'default', 'low']
	with Connection(conn):
		worker = Worker(q)
		worker.work()



def save_df_to_sql(df: pd.DataFrame, db_engine: engine) -> None:
	"""
	Save dataframe with process information to a local sql alchemy DB file. Replace if dataframe pre-exists
	Args:
		df: process information df.
		db_engine:  sql alchemy engine to use.

	Raises:
		OperationalError: if any sqlalchemy errors have been thrown.
	"""
	"""
	with db_engine.connect() as conn:
		conn.execute(text("DROP TABLE IF EXISTS 'processes' "))
		conn.commit()
	"""
	try:
		df.to_sql("processes", con=db_engine, if_exists="replace", index=False)
	except OperationalError as exc:
		raise exc



def create_process_info_dataframe(func, func_input, status, job_name: str, pid: int, job_id, index: int) -> pd.DataFrame:
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
			'id': [index],
			'created': [created],
			'process_id': [pid],
			'job_name': [job_name],
			'job_id': [job_id],
			'task': [func.__str__()],
			'last_updated_time': [None],
			'status': [status]
		}
	)

def insert_a_task_record(df: pd.DataFrame, db_engine: engine) -> None:
	try:
		df.to_sql("processes", con=db_engine, if_exists="append", index=False)
	except OperationalError as exc:
		raise exc



def update_task_status_info(df: pd.DataFrame, redis_conn, db_engine) -> None:
	"""
	If process dataframe already exists, filter out dead and 'zombie' processes to only
	show accurate process information.

	Args:
		df: df with process information.
	"""
	#df["running"] = df["job_id"].apply(
	#	lambda x: psutil.pid_exists(x) and psutil.Process(x).status() == "running")
	
	try:
		df["status"] = df["job_id"].apply(lambda x: (Job.fetch(x, connection=redis_conn)).get_status())
	except NoSuchJobError: ""
	save_df_to_sql(df, db_engine)
	return(df)	