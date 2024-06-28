# streamlitrq

This package is inspired by tasklit and expands its capabilities in: 

- not just scheduling command line task but more general task defined from user interface, expecially about generative AI tasks.

- using task queue utilities such as rq for background process.


## Procedures:
1. submit_job: pass user input
	- input: function, function_input, start_task_id 
	- call `start_scheduler_process` function

2. start_scheduler_process: 
	- start redis server
	- start rq worker
	- call `schedule_process_job` function

3. schedule_process_job:
	- schedules config: Once or Now
	- call `execute_job` function

4. execute_job:
	- enqueue task function
