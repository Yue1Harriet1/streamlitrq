import redis
from redis import Redis
from streamlitrq import Queue, Worker, Connection
from multiprocessing import Process
from subprocess import Popen

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



