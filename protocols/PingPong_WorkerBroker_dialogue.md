Ping Pong dialogue between a worker process and a broker process. 

## Sockets
* worker.frontend (DEALER) <-----> broker.backend (ROUTER)

## Constants

* BROKER_TIMEOUT: 
* N_TIMEOUTS:
* WOKER_EXPIRE_SEONDS:


## Message flows

Worker   READY--->       Broker

Worker    PING--->       Broker<br/>
Worker       <---PONG    Broker

Worker       <---job     Broker<br/>
Worker  result--->       Broker

<!-- Worker       <---EXIT    Broker -->

## Dialogues

### Worker is ready
1) Worker ready; worker send ["", READY]; The worker process sends ["", READY] when it is ready. 
2) Broker recv [worker_id, "", READY]; register worker; when broker receives READY on BACKEND it adds worker to queue and immediately sends PONG.
3) Worker recv ["", PONG]; worker updates broker aliveness; when the worker gets a pong from the broker it knows that the broker is alive and keep the connetion open.

### Worker sends PING
1) Worker heartbeat; send ["", PING]; Every second, the worker sends a ["", PING] to let the broker know that it's alive. 
2) Broker recv PING; send ["", PONG]; When the broker receives a PING on backend it immediately responds with a PONG. If worker_id is not in the ready queue, worker_id is added to the ready queue.

### Broker assign job to worker
1) Assign job; broker send [worker_id, "", job]; broker pops worker from queue and sends the job.
2) Worker recieve job; recv ["", job]; When the worker receives a message that is not ["", PONG] or ["", EXIT] it will assume that the message is a job task and process the job.   
3) Send result; worker send ["", result]; when the worker has finished the tasks it sends the result back to the broker.

## Aliveness events
### Broker timeout
If the worker has received no messages (PONG or jobs)for N_TIMEOUTS consecutive periods of BROKER_TIMEOUT the worker will close, re-initialize frontend and then send ["", READY].

### Worker timeout
* Timeout worker: When the broker has not received any messages from a worker for WORKER_EXPIRE_SECONDS it removes the worker from the ready queue.
* Update worker expiration: When the broker receives any message from a worker it updates the timeout the worker to current time plus WORKER_EXPIRE_SECONDS

<!-- * Worker exit; recv ["", EXIT]; If the worker receives exit message it will exit the process.  -->





<!-- ### Broker -->


<!-- * Kill worker; send [worker_id, "", EXIT]; broker removes worker from queue and sends exit message.  -->

## Notes
* The dialog between worker and broker is **asynchronous**.
* When the broker send EXIT to a worker, the worker does not send an acknowledgement. Hence the broker does not contain state infomation about the worker.
* While the worker is working, it does not send out PONG messages. 
