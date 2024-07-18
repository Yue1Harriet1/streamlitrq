import redis
from redis import Redis
from streamlitrq import Queue, Worker, Connection
from streamlitrq.job import Job
from streamlitrq.registry import StartedJobRegistry, FinishedJobRegistry
from multiprocessing import Process
from subprocess import Popen
import os

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


def check_job_status(job_id, conn:Connection):
	job = Job.fetch(id=str(job_id), connection=conn)
	result = job.latest_results()
	return(result.Type.SUCCESSFUL, result.return_value)

def get_finished_jobs(conn:Connection):
	registry = FinishedJobRegistry('default', connection=conn)
	return(registry.get_job_ids())

def get_existing_jobs(conn:Connection):
	registry = StartedJobRegistry('default', connection=conn)
	return(registry.get_job_ids())