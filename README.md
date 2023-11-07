# rqueue

This package is inspired by tasklit and expands its capabilities in: 

- not just scheduling command line task but more general task defined from user interface, expecially about generative AI tasks.

- using task queue utilities such as rq for background process.


## Procedures:
1. submit_job: pass user input
	- input: function, function_input, start_task_id 
	- call `start_scheduler_process` function