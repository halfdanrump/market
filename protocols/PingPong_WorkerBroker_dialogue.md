#PING/PONG dialogue between a worker and a broker

This document specifies the interaction between a PingPongWorker and a PingPongBroker. The role of the broker is
1. Keep a list of idle workers
2. Accept jobs from clients and forward them to the workers. 
The PING/PONG dialogue is a way for the broker to know which workers are responsive, and for the workers to know if the broker is responsive. If a worker sends a PING and doesn't get a PONG, it will assume that the broker process has died and reconnect to the broker after a while. Similarly, if a worker doesn't send any PINGs for a while, the broker will no longer send jobs to that worker. 

## Sockets
Worker has a DEALER socket called frontend. Broker has a ROUTER socket called backend. Broker also has a ROUTER socket called frontend for accepting requests from clients. The interaction between client and broker is not described here. 

Worker.frontend (DEALER) <-----> Broker.backend (ROUTER)

## Constants

* BROKER_TIMEOUT: The timeout for expected PONG messages
* N_TIMEOUTS: The worker will reconnect the socket after N_TIMEOUTS timeouts.
* WORKER_EXPIRE_SEONDS: The time that the broker waits for a PING from the worker before it considers the worker dead.


## Dialogues

Worker  (1) READY--->      (2)    Broker

Worker  (1) PING--->       (2)    Broker<br/>
Worker  (4)     <---PONG   (3)    Broker

Worker  (1)     <---job    (2)    Broker<br/>
Worker  (4) result--->     (3)    Broker
<!-- Worker       <---EXIT    Broker -->

### Worker is ready
1. Worker ready; worker send ["", READY]; The worker process sends ["", READY] when it is ready. 
2. Broker recv [worker_id, "", READY]; register worker; when broker receives READY on BACKEND it adds worker to queue

### Worker sends PING
1. Worker heartbeat; send ["", PING]; Every second, the worker sends a ["", PING] to let the broker know that it's alive. 
2. Broker recv PING; send ["", PONG]; When the broker receives a PING on backend it immediately responds with a PONG. If worker_id is not in the ready queue, worker_id is added to the ready queue.
3. Broker sends PONG; send [worker_id, "", PONG]; immediately after receiving PING broker sends PONG.
4. Worker recv ["", PONG]; worker updates broker aliveness; when the worker gets a pong from the broker it knows that the broker is alive and keep the connetion open.

### Broker assign job to worker
1. Assign job; broker send [worker_id, "", job]; broker pops worker from queue and sends the job.
2. Worker recieve job; recv ["", job]; When the worker receives a message that is not ["", PONG] it will assume that the message is a job task and process the job.   
3. Send result; worker send ["", result]; when the worker has finished the tasks it sends the result back to the broker.
4. Broker gets result; recv [worker_id, "", result]; Broker forwards result to client and adds worker to ready queue 

## Aliveness events
### Broker timeout
If the worker has received no messages (PONG or jobs)for N_TIMEOUTS consecutive periods of BROKER_TIMEOUT the worker will close, re-initialize frontend and then send ["", READY].

### Worker timeout
* Timeout worker: When the broker has not received any messages from a worker in the ready queue for WORKER_EXPIRE_SECONDS it removes the worker from the ready queue.
* Update worker expiration: When the broker receives any message from a worker it updates the timeout the worker to current time plus WORKER_EXPIRE_SECONDS

<!-- * Worker exit; recv ["", EXIT]; If the worker receives exit message it will exit the process.  -->





<!-- ### Broker -->


<!-- * Kill worker; send [worker_id, "", EXIT]; broker removes worker from queue and sends exit message.  -->

## Notes
* The dialog between worker and broker is **asynchronous**.
<!-- When the broker send EXIT to a worker, the worker does not send an acknowledgement. Hence the broker does not contain state infomation about the worker. -->
* While the worker is working, it does not send out PONG messages. 
* In this specification there is no mechanism for dealing with workers that crash while working and thus never returns a result (resending jobs after a timeout period is not very difficult though).
